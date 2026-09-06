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

**Amended 2026-09-05 — the count under `no` and the pair named under `wait` are
M10's, and are not maintained.** The division stands, and so does every reason
given for it; the set has simply grown since. `timeout` joined `wait` when
[ADR-099](#adr-099--authenticated-routes-carry-a-request-timeout-an-explicit-body-ceiling-and-a-per-principal-rate-limit)
gave the server a request deadline, and `stale` joined `no` with conditional
writes; `elsewhere` is still exactly the three codes named. No corrected count
is written in their place, because a number in prose has nothing holding it —
which is how both of these came to be wrong with nothing failing. The live
division is `ErrorCode::retry()`, published per code in the `ErrorCode` table
of `docs/openapi.yaml` and held to the enum by
`every_error_code_is_specified_with_the_retry_class_the_server_uses` in
`crates/kimmy-api/tests/openapi.rs`.

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

**Amended 2026-08-30 — the image no longer compiles.** The Dockerfile's
release-mode build, which is what made QEMU untenable above, is no longer on
the release path: the image ships the `kimmyd` from the release archive for
its architecture, and the two native runners and the manifest merge remain
for the reasons ADR-107 gives.

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

**Amended 2026-08-28 — the origin-node rule is retired.** Keeping "whoever
wrote a document embeds it immediately" on the streaming path left the two
rules able to disagree: under a bulk load the writer works through its backlog
sequentially, the owner's deferral grace expires on everything the writer has
not reached, its re-check finds the vectors still stale, and from then on two
nodes embed the same backlog side by side. Measured on a three-member cluster:
570 provider calls for 361 documents inserted in one batch, exactly the cost
class this record was written to close. Ownership now decides on every path.
The owner embeds each write the moment its stream delivers it — its own
writes and replicated ones alike, from the entry's image — and a non-owner
embeds nothing on the stream, including what it wrote itself. A non-owner's
deferral is a re-check of one question, *am I the owner now?*: ownership is a
function of the live member set, so it moves only when the owner has left,
and then the survivor that inherited the collection embeds what the old owner
left undone. While the owner lives the deferral is re-armed, not dropped, so
an owner's death mid-backlog is covered; after ten minutes it is let go, so a
healthy cluster's history is not pinned in every non-owner's queue. The trade
is latency: a write made on a non-owner gets its vectors after one round of
replication rather than immediately, which is the same trade this record
already made for backfill.

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

**Observed** on a three-member production cluster, 2026-08-26, at 0.7.0 with
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
observed case in production (2026-08-26) was not a partitioned member but every
member at once, each behind the others' horizon after ten hours dark — a
refusal would have left the cluster refusing itself. The warning and the
topology field give the operator the fact and the recommended action (stop
the peer, reset its data directory, let anti-entropy refill it) at the
moment they matter; had they existed, the outage would have been named
in the first sync round after the horizon passed rather than found by
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

**Amended 2026-08-30 — the route re-evaluates keys, and the over-statement
is gone.** The cost argument above overstated what re-evaluation would take.
The pass already reads every named document to know it still exists; the
one function the write path uses to compute a document's keys under an
index runs over that same document for a few microseconds, and needs no
transaction, no index scan and no maintenance. So the route now does it: a
member whose current keys meet none of the others' has left its group, a
group with fewer than two members left is not reported, and an index that
is gone or no longer unique has no constraint to break. Deletion and rewrite
become one case, which is the definition of "standing" the decision should
have had — *at least two of the named documents still exist and still share
a key*. Nothing else changed: still derived on request, nothing stored,
nothing written, still bounded by retention, still `read`. The one visible
wrinkle is that `merged` names the recorded arrival, so after that document's
own value is rewritten it can name an `_id` that is no longer among the
group's `ids`; a client that wants only the survivors has them in `ids`.

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

**Amended 2026-08-30 — `describe` carries it too.** The same value, from the
same source, is now on `GET …/describe` as `nodeDurability`, and through it
on the MCP `describe_collection` tool. `/v1/version` remains the operator's
answer; `describe` is the one call a client makes before it writes, and it
was the only place a client learned everything about a collection except
what an acknowledged write to it means. The name says *node* because the
class is per node, not per collection, and a field named `durability` on a
collection description reads as something to set there. Still a field, not
a capability, for the reason above.

## ADR-089 — The CLI is for people: `client_credentials` leaves `kimmy`

**Decision.** `kimmy login --client-credentials` and `kimmy token
--client-credentials` are removed, with the `KIMMY_OIDC_CLIENT_SECRET`
variable and the `client_secret` settings key they existed to consume. The
CLI runs exactly two flows: a password login for a named local account, and
the RFC 8628 device flow for everyone else. A script, a cron job or a
healthcheck sets `KIMMY_TOKEN` (or the settings file's `token`) to a bearer
token minted elsewhere — a personal access token from the identity provider's
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
chat service's agents) has its own implementation and never shelled out to
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

## ADR-090 — `ddl` is its own action, and it federates

**Decision.** A new action, `ddl`, covers creating and dropping collections,
creating and dropping indexes, and configuring or disabling embeddings. Those
operations required `admin` before; they require `ddl` now, and `admin` still
implies it. `ddl` implies no data access and never reaches the system database.
It maps through an identity provider like `read` and `write` do; the
federation refusal stays exactly where it was, on `admin`.

**What prompted it.** An agent connecting over MCP through an OIDC provider — a
federated principal — was told to `create_collection` before inserting, by the
server's own instructions, and was refused: the tool needed `admin`, and
ADR-067 forbids federating that. The instructions said one thing and the
authorization model another, and the only ways to reconcile them without this
change were to hide the tool (ADR-025 says why not), to hand the agent
`allow_federated_admin` (a superuser with user management and backup, to
create a collection), or to have a person create every collection an agent
might want. None of those is the model; the model was wrong about what
`admin` bundled.

**Why a split rather than a flag.** `admin` conflated two things: shaping the
*data* and administering the *server*. The break-glass argument in ADR-067 is
about the second — a compromised provider that can mint a user manager, take a
backup, or open `__users` is unrecoverable from inside the database. A
provider that can mint a principal that creates collections is in the same
position as one that mints a writer: bounded by grants, revocable by editing a
mapping, and nothing the root account cannot undo. So the boundary moves to
where the argument actually applies.

**Alternatives rejected.** *`write` implies `ddl`* — a writer that can drop
the collection it writes to has a much larger blast radius than one that can
only change rows, and "can insert" should not silently mean "can drop".
*`ddl` implies `read`* — shaping a collection is not reading it; a role that
needs both names both, as `watch` and `read` already work. *Making
`configure_vectors` stay `admin`* — enabling embeddings creates a shadow
collection and disabling them can drop it, which is collection DDL by any
reading; leaving it behind `admin` would have reproduced the original problem
one step later, when the agent tried to make its new collection searchable.

**The system database.** `Principal::system_access` opens `__kimmy` to any
holder of `admin`, on any grant. `ddl` gets no such door: a `ddl` grant over
`*` creates collections everywhere except there. The unit test that says so
is the one that must never be deleted.

**Cost.** One more action to explain, and every grant that meant "this role
may create collections" now has to say `ddl` if it did not already say
`admin` — no existing grant loses anything, because `admin` implies `ddl`.
The OpenAPI enum, the CLI docs, the actions table and the startup-refusal
message all grow a word.

**Consequence.** The recommended agent role is `read`, `write`, `search`,
`ddl` over the databases it owns — everything it needs to build and use its
own collections, and nothing about the server.

---

## ADR-091 — Search verifies the source document, not only the chunk

**Decision.** `vector_search` and `hybrid_search` check every hit against the
source collection after ranking, and drop the ones whose document no longer
exists. Separately, the embedding worker removes a deleted document's chunks
regardless of provider (`byo` included), and its streaming path re-reads the
document's current stamp after the provider returns and before it writes, so
an embed that outlasts a delete or an update stores nothing.

**What was wrong.** ADR-022 promises that "a deleted document cannot surface".
It was kept for a missing *chunk record* — the graph skips a candidate whose
record is gone — and not for a missing *document*. Deleting a document never
touched the shadow collection; the worker removed the chunks when it reached
the `Delete` entry in the stream. Until then the chunks were scored and
returned like any other, with an `_id` that resolved to nothing. That window
was a second or two on a healthy owner and unbounded when the worker was
behind, disabled, or not this node's. Two further defects hid in the same
code: a `byo` collection's chunks were never cleaned up at all, because the
"nothing to embed" bail preceded the delete branch; and the streaming path
embedded from the entry's own image without checking that the document still
existed when the provider came back, so a slow embed could land chunks *after*
the delete that should have removed them, permanently.

**Why at read time, not in the delete transaction.** Removing the chunks
inside `delete_where` would make the promise structural, and it is the
obvious fix. It is not the chosen one, for three reasons. The write path is
bounded deliberately (ADR-083, ADR-086) and the shadow's chunk keys are not
reachable from the source collection's metadata without a scan or a new
index. TTL expiry, drop-collection and a replica applying a remote delete are
all further delete paths, and each would need the cascade too — the read-time
check covers every one of them at once. And the check is one point read per
hit, after ranking, on a result that is at most `MAX_K` long: cheap in the
place where a mistake is visible, rather than a cost on every write to keep a
structure the search path does not trust anyway (ADR-022's own principle).

**What it does not do.** A hit dropped here is not replaced, so a result may
be shorter than `k` by the number of deletions the worker has not yet caught
up with. That is reported honestly rather than papered over by ranking again.
Updated documents are unchanged by this decision: their old chunks are
replaced when the worker re-embeds, and until then the old text is what
matches — bounded staleness on live data, which ADR-022 already accepts.

**Consequence.** The `vectors.md` statement of the invariant now distinguishes
the chunk from the document. The one place source documents were already
consulted — the `filter` path, which scans them — is unchanged.

---

## Next

- [Roadmap](roadmap.md) — decisions still to be made
- [Testing](testing.md) — how these choices are defended

## ADR-092 — MCP listing tools omit KimmyDB's own internals

**Decision.** The `list_databases` and `list_collections` tools omit what
`resources/list` has omitted since ADR-027: the `__kimmy` system database, and
any `__`-prefixed or `.__vectors` collection. `find`, `count`,
`describe_collection` and every other tool reach an internal by name exactly
as before, under the ordinary access check.

**Why.** ADR-027 drew the line between a resource — material an agent attaches
to its context — and a tool — a specific question a caller asks. A listing sits
on the resource side of that line even though it is a tool: its whole purpose
is to tell an agent what to open next. Driving the server after 0.14.0 through
an MCP client, `list_collections` on a database with embeddings answered
`["notes", "notes.__vectors"]`, and the honest expectation of an agent reading
that is that both are things to look at. The second is a collection of
1024-float arrays that describes nothing `notes` does not, and
`describe_collection` on `notes` already says the vectors exist. Naming the
shadow in the list invites the wrong call at a great cost in context.

**Why not hide it everywhere.** The REST API's listing stays complete. The CLI
targets `orders.__vectors` by name on purpose (`kimmy` splits
`shop.orders.__vectors` into a database and a collection because a collection
name may contain a dot), operators back up and inspect shadows, and a human
reading a REST response is not an agent deciding what to open. The
implementation detail belongs to the MCP layer, in the same function resources
use, so the two cannot disagree about what "internal" means.

**Not a security control.** It is a default. The access decision remains
`Principal::can`; a superuser can still `count` `__kimmy.__users` through a
tool call, and a federated principal still cannot. Anyone who needs the shadow
knows its name.

**Cost.** An agent that genuinely wants to inspect a shadow collection has to
know the naming rule, which `list_collections`'s description now states.

---

## ADR-093 — Placeholder secrets are refused off loopback, and the HS256 floor is 32 bytes

**Decision.** Two rules, both enforced in `Config::validate` so that
`check-config` refuses exactly what the server refuses. First, a node whose
HTTP listener binds anything other than a loopback address — or, with
clustering on, whose cluster listener does — refuses to start when
`auth.root_password`, `auth.jwt_secret` or `cluster.cluster_secret` is one of
the values this repository's own files put where a secret goes. The list is
`PLACEHOLDER_SECRETS` in `kimmyd`'s `config.rs`: the compose file's former
defaults, the commented-out lines in `kimmy.example.toml`, the quick starts'
former values, the example programs' passwords, and the words anyone types
when they mean to come back later (`password`, `secret`, `changeme`, `root`,
…). Matching is exact apart from case and surrounding whitespace. The error
names the setting and the environment variable and never the value. Second,
`kimmy_auth::MIN_SECRET_LEN` is 32 bytes, not 16. Only the local HS256 path
has a shared secret, so only it has a floor; the OIDC verifier's parallel
rule (`OidcSettings::validate`) polices the audience and is unaffected.

**Why.** A value that appears in a public repository is held by everyone who
has read it, so it is not a secret in any sense that matters; and a copied
quick start is the single most likely way a database ends up on a routable
address with one. With the signing key anyone can mint a root token; with
the cluster secret anyone who can reach the gossip port can inject writes;
the bootstrap password is the first thing tried against a fresh node. The
existing rule for `--insecure-no-auth` already draws the line at loopback,
and this is the same line for the same reason: on the host's own interfaces
nothing off the host can reach the node, so a convenience value costs
nothing, and off them it costs everything.

The floor moves because RFC 7518 §3.2 says a key for HS256 should be no
shorter than the hash's output, 256 bits. A 16-byte key is not broken, but it
is half the entropy of the MAC it feeds, one captured token is all the
material an offline search needs, and the value is shared by every node of
the cluster. Sixteen was chosen when the number had to be *something*; it
should be the number the specification gives.

**Why not an absolute refusal.** ADR-067 refused federated `admin` outright,
for good reasons, and ADR-074 had to open it behind a flag because the
absolute form made a legitimate deployment impossible rather than merely
awkward. A flag whose purpose is to remove a security rule is the outcome to
avoid, and the way to avoid it is to scope the rule to the condition that
makes the value dangerous instead of to the value. An absolute refusal of
placeholders would break exactly the case it is not aimed at — a developer
on a laptop running an example as written — and the examples are how the
project is evaluated; an example that has to be edited before it runs is one
that gets edited into something worse, or into a flag. So loopback keeps
working with every placeholder, anything reachable refuses them all, and
there is no switch. A denylist is admittedly the weak form of a rule — a
value absent from it is not thereby good — which is why the length floor
still applies on top, and why the list is short and made of things that have
actually shipped rather than an attempt at a dictionary.

**Why not warn.** A warning at startup is read once, by the person who
already knows, and never by the person who inherits the deployment. The
`--insecure-no-auth` precedent is a refusal for the same reason.

**Cost.** Two configurations that ran under 0.16.x do not run under this
version, which is why the release carrying it is a minor rather than a patch
(`docs/compatibility.md`). A `jwt_secret` of 16–31 bytes must be rotated
before upgrading, and rotating it ends every session once, on every node at
the same time. A node that was running on a placeholder off loopback must be
given real values, which is the point. The compose file no longer supplies
defaults, so `docker compose up` needs three variables in a `.env` or the
environment; it says so, and names them. The list has to be maintained: a
new example value added anywhere in the repository belongs on it, and a test
pins the ones that have shipped so far.

---

## ADR-094 — Hybrid fusion is tunable per request; defaults stay equal-weight RRF

**Decision.** `hybrid_search` takes two optional request fields. `weights`
(`{ "dense": w_d, "lexical": w_l }`, each `>= 0`, not both zero) scales the two
halves before they are summed, so the fused score is
`w_d / (60 + rank_dense) + w_l / (60 + rank_lexical)`. `min_overlap` (`>= 1`)
is the number of distinct query terms a chunk must contain to enter the
lexical ranking at all; a query with fewer distinct terms than that uses its
own count. The gate removes lexical *evidence*, not documents: a document it
drops from the lexical half keeps its full dense contribution. Both fields are
validated at the API and refused with the ordinary `400`. Their defaults —
`{1, 1}` and `1` — reproduce the previous ranking to the bit, and the MCP tool
and the CLI expose the same controls with the same defaults.

**Why.** Measured on a corpus of short conversational documents, forty graded
queries and eight embedding models, hybrid search recalled roughly a third
less than plain vector search on the same queries — for every model, the gap
narrowing only as the dense model got stronger (Recall@10 of 0.677 dense
against 0.429 hybrid on the weakest). That is the wrong way round for a feature
whose reason to exist is to improve on the dense half. The mechanism is in
the lexical half. It ranks by term overlap (recorded in `deviations.md`), and
each half is retrieved four times wider than `k` before fusion. On documents
of a sentence or two, nearly every candidate in that window shares one or two
common words with the query, so the lexical ordering among them is close to
random; and reciprocal rank fusion, weighting both halves equally, gives that
near-random rank the same authority as the dense one. Half the evidence being
noise costs a third of the recall.

The two knobs are the two places the mechanism can be interrupted. `weights`
lowers the authority of the lexical rank; `min_overlap` removes the candidates
whose lexical rank is noise, since a chunk that agrees with the query on two
or more distinct terms is what an exact-term match — the case hybrid search is
for — actually looks like. Per request rather than per collection because the
right setting depends on the query as much as the corpus: a query naming a
product code wants the lexical half; a paraphrase does not.

**Why the defaults do not move.** The measurement was made on one kind of
corpus. On longer documents term overlap is a weaker but real signal, and a
default that helped short conversational text could cost a corpus of manuals
the exact-term matches it relies on. Nothing about an existing deployment's
ranking changes until the knobs have been measured there; the change ships
the instrument, not a conclusion. When the measurements are in, a better
default is a one-line change with evidence behind it.

**Alternatives.** *A BM25 lexical half*, with per-collection term statistics,
is the principled fix and would make the lexical rank informative on short
documents too. It is deferred rather than rejected: it needs document
frequencies maintained under replicated writes and a rebuild path, which is
real machinery to add ahead of knowing how much of the gap the two knobs
already close. *A confidence gate relative to the best lexical score* —
admit only candidates within some fraction of the top lexical score — was
considered and set aside: term-overlap scores are normalized by chunk length,
so "within a fraction of the best" measures how short a chunk is as much as
how well it matches, and the threshold would have no meaning a caller could
reason about. A count of distinct matched terms is something a caller can
predict from the query in hand. *Changing the default weighting* was rejected
for the reason above.

**Cost.** Two more fields on a request that is shared with `vector_search`,
which ignores them; the specification says so. Callers who want the better
ranking on a short-document corpus have to ask for it, per request, until a
measured default replaces the equal weights. A weight of zero on the lexical
half is a slower way of running `vector_search`, and the documentation says
that too.

---

## ADR-095 — The embedding worker batches provider calls across documents

**Decision.** The worker fills one provider call from the chunks of
consecutive documents of the same collection, on the streaming path and in a
backfill alike, bounded by three process settings under `[vector.batch]`:
`max_chunks` (32), `max_tokens` (32 768, by the estimate `chunk.max_tokens`
already cuts on) and `max_wait_ms` (100, waited only when the stream is
idle). The storage write stays one per document. A batch that fails
permanently is taken apart and each document sent alone, so the one at fault
is skipped and named and the rest land; a retryable failure retries the whole
batch. The document and chunk counters count what they always did.

**Why.** `EmbeddingProvider::embed` took a batch from the day it was written,
and the worker handed it one document at a time. A document short enough to
be one chunk — most documents, in most collections — was a batch of one, and
paid a whole round trip, the provider's tokenisation and its scheduling by
itself. Measured against a llama.cpp CPU server with ~43-character inputs: 32
calls of one input, 394 ms; one call of 32 inputs, 18 ms — a factor of
twenty-two. Per-document calls put a floor of one round trip under every
document, and a write rate a little above what the floor allows grows the
backlog without bound; that is Little's law, and it is what a live ingest
showed, with the worker healthy, the provider idle most of each round trip,
and the vectors falling further behind by the minute.

The bounds are three because a provider's limits come in three shapes. A
count, because hosted providers cap inputs per request (Cohere at 96, Gemini
at 100) and 32 sits under all of them — it is also the size the measurement
was taken at. A token total, because request bodies have limits too, and the
token estimate the chunker already uses is the honest unit: 32 768 estimated
tokens is exactly 32 chunks at the default chunk ceiling, about 64 KiB of
text, inside every hosted provider's per-request budget. A wait, because a
quiet collection's one document must not sit until the next write; it is
waited only when the stream is idle — on a backlog the next entry is already
there and the batch fills without waiting — and 100 ms is less than the
remote round trip it saves.

**Why the storage write is not batched too.** `put_vectors` is replace-all
for one document's chunks, staleness is one document's HLC, and both are what
make re-embedding idempotent and a crash replayable (the staleness section
of `vectors.md`). Batching the write would make a crash between two
documents' writes a question — which of the batch landed? — that today has
no answer because it never needs one: each document either has its vectors at
its HLC or does not. The oplog position is recorded once the batch has
landed, never before, so the guarantee is unchanged; it merely covers a few
entries at once. The cost of keeping the writes separate is one commit per
document, which is what it always was, and which was never the bottleneck.

**Why process settings, not collection settings.** The bounds describe the
round trip this node makes: the same request-size limits apply whichever
collection's documents fill the call, and a node against a metered API and a
node against a local server want different waits regardless of collection.
A batch only ever holds one collection's documents, so the per-collection
provider, model and prefix are respected without being repeated. Putting the
bounds in the collection's vector configuration would also make them
replicated metadata that changes the fingerprint and triggers a reindex,
which a change to a *scheduling* parameter must not do.

**Alternatives.** Batching the storage write — rejected above. Per-collection
bounds — rejected above. Concurrent provider calls (`max_in_flight`) — left
out: the streaming path records one position for everything before it, and
several batches in flight would either serialise their completions in
arrival order (buying little) or record positions out of order (unsafe); the
measured gain from batching alone is the twenty-two-fold one, and the case
for concurrency should be made against a provider that batching has left
idle, which none has yet. A batch-size histogram on `/metrics` — skipped:
`kimmy_embed_chunks_total` over `kimmy_embed_provider_requests_total` is the
average the operator wants, and a histogram would be the first bucketed
series in a set that is otherwise plain counters. Opportunistic batching with
no timer at all — available as `max_wait_ms = 0`, and not the default,
because a remote provider gains more from a slightly larger call than a
quiet collection loses to a tenth of a second.

**Cost.** A quiet collection's document is embedded up to `max_wait_ms`
later than before. A permanent failure in a batch of *n* costs *n* extra
calls, once, to find the document at fault. The `ollama` provider still sends
one request per input, because its embeddings endpoint takes one, so batching
saves it nothing on the wire. A batch spanning several entries holds their
positions until it lands, so a crash mid-batch replays up to a batch's worth
of entries rather than one; every replay is a no-op on the staleness check.
And the structural fact batching does not change is worth stating where
operators size deployments: a collection is embedded by exactly one owner
node, so adding members does not raise one collection's throughput — it
raises how many collections embed at once.

---

## ADR-096 — Federated tokens are refused above a maximum lifetime

> **Superseded by [ADR-112](#adr-112--the-provider-decides-how-long-its-tokens-live).**
> The refusal, its setting and its errors were removed in the release after the
> one that shipped them. The reasoning below is kept because ADR-112 argues
> against it and the argument is worth reading in both directions.

**Decision.** `OidcVerifier::verify` refuses a token whose own `exp − iat`
exceeds `auth.oidc.max_token_lifetime_secs`
(`KIMMY_OIDC_MAX_TOKEN_LIFETIME_SECS`), 900 seconds by default, and refuses a
token that carries no `iat` at all. The refusal is a 401 whose
`WWW-Authenticate` challenge carries `error="invalid_token"` and an
`error_description` naming the limit in seconds and nothing about the token.
The setting is refused at startup outside 1–86400, by the same function
`check-config` runs. The check is on the token's two claims and nothing else:
no clock, and so no leeway.

**Why.** ADR-073 states the window plainly: a federated principal's role
*membership* is frozen in its access token, because this database makes no
introspection call, so a revocation at the provider is honoured only when the
token expires. It named short token lifetimes as the mitigation and left the
lifetime to the provider. That is a mitigation this node could not see being
applied — a provider minting day-long tokens was indistinguishable, from here,
from one minting five-minute tokens, and the security guide's "keep lifetimes
short" was advice with nothing enforcing it. The lifetime is the one dimension
of the window that is readable from the token itself, so it is the one this
node can bound.

Fifteen minutes because of where the providers sit. Their access-token defaults
fall between five minutes and an hour — Keycloak's is five, Okta's, Google's
and Entra ID's about an hour — and one of them, Auth0, defaults an API's tokens
to a day. 900 seconds admits the short defaults outright and refuses the
day-long tokens that turn the window into a policy. A provider that defaults to
an hour is not excluded, it is asked a question: shorten the lifetime for this
resource, which every one of them supports per resource or per client, or raise
this node's limit — and the error says which number to raise it to, because the
description names the limit. An operator who raises it does so knowing what it
costs, which is the property the default is for. A missing `iat` is refused
rather than waved through because the limit would otherwise be one omitted
claim away from not applying; RFC 9068 §2.2 makes the claim REQUIRED in a JWT
access token, so a conforming provider never produces that token.

The limit is checked after the signature and after expiry. After the signature,
so that only a token the provider really minted is ever answered with anything
more specific than "invalid"; after expiry, so that a token which is both stale
and too long-lived is reported as the former, which is the one a client can fix
by itself. The 60 seconds of leeway (ADR-064) play no part: they exist because
the provider's clock is somebody else's, and that argument says nothing about
how long the provider chose to make a token valid for. The description reaches
the challenge through a response extension the challenge layer reads, rather
than by the error setting the header itself, so that a more specific
description never costs the `resource_metadata` pointer (ADR-071).

**Alternatives.** *Token introspection on every request* (RFC 7662) would close
the window entirely. Rejected: it adds a round trip to the provider on every
call, makes the provider's availability the database's, and not every provider
offers the endpoint — Entra ID does not. *A revocation feed* from the provider —
back-channel logout, a shared-signals stream — rejected because none is
standard across the providers this federation exists to serve, and a node that
honoured one provider's feed would be silently unprotected against another's.
*A warning rather than a refusal above the ceiling*, or no ceiling: rejected
because every other setting in the section is policed by refusal, and a
warning printed at startup is a warning nobody reads. *Measuring the remaining
time rather than the lifetime*: rejected because a long-lived token would then
be admitted once it had aged enough, which is exactly the token the window is
about.

**Cost.** An operator whose provider mints access tokens longer than fifteen
minutes has a change to make on upgrade: shorten the provider's lifetime for
this resource, or raise `max_token_lifetime_secs`. The refusal names the limit
so the change is discoverable from the first failed request, and a raised limit
is printed in the startup summary so it stays visible. A provider that omits
`iat` cannot be federated with until it stops, and there is no setting for
that. And the limit is a bound, not a revocation: a membership revoked at the
provider is still honoured until the token expires, for up to the configured
number of seconds.

---

## ADR-097 — The retention horizon is judged per origin

**Decision.** The retention pass records, beside the single
`oplog_collected_through` stamp, the highest stamp it removed **per origin**
(`oplog_collected`, one entry per node id, written in the same transaction as
the removal). Two decisions read it. A peer sends its witnessed vector with
`AskEntries` (`held`, optional on the wire), and the sender answers
`BeyondHorizon` only when, at an origin the peer trails, the peer's coverage
sits below what was collected of that origin — otherwise it is served from
the threshold it asked from, however far below the coarse horizon that is. And
the stale-rejoiner verdict of ADR-085 requires, besides trailing this node by
more than tombstone retention at an origin, that the peer also lack something
collected at that origin; the reported span is unchanged. A database an
earlier build collected from has no per-origin record, so `Engine::open` seeds
every origin it holds with the coarse horizon: coarse below that point, exact
above it.

**What was observed.** A member-at-a-time roll of a three-member cluster that
had been converged for 36 hours. Half a second after A came back, its first
round named *both* peers stale — `behind_secs=129091`, within minutes of the
time since the previous roll — with the message that tells an operator to
reset them; five seconds later both were back within retention. And each time
a member came back, the first peer to pull from it was told it was beyond the
horizon and pulled a full snapshot of a store it held in full but for one
entry. Data converged; the signals were false and the snapshots were cost.

**Why.** Both come from one write and one measure. A member re-registers
itself in the client topology when its build or endpoint changed, which a roll
that upgrades does on every member. A member that clients do not write
through has, until then, written nothing since the previous roll — so its own
origin's previous stamp is 36 hours old, collected on every peer along with
everything around it. The version vector summarises an origin by its newest
stamp, so at the next round every peer trails that origin by the whole
silence: `lag_behind_ms` with its roles swapped reads 36 hours, above
retention, and names the peer. In the other direction, `VersionVector::behind`
reduces "what am I missing" to one threshold — the peer's own coverage of the
origin it trails most — and that threshold is the 36-hour-old stamp, below
the single horizon stamp; the sender cannot tell that everything under the
horizon is the peer's own coverage, and the snapshot is the only safe answer
it has. Both are the same blindness: one stamp across all origins cannot say
*which* origin's history is gone. Recorded per origin, the question each side
needs is exact. Does the peer lack anything collected of the origin it trails?
In the roll, no: the gap held one entry, seconds old, servable, and pulled on
the next round. A peer that can still be served everything it lacks is not
beyond the horizon and has nothing to resurrect.

**Why not gate the verdict on the first round, or on two consecutive rounds.**
The restart is where it was seen, not what causes it: any origin that writes
after a silence longer than retention does the same to every peer, restart or
not, and a peer that is a round or two late to pull — a larger cluster's
fanout makes that routine — would be named on the third. A count is a guess at
the cause; the record is the cause.

**Why not treat the horizon as unknown just after start.** The horizon was
not wrong; it is persisted and correct. The threshold was below it for a
reason that the horizon alone cannot see.

**Why the peer sends its vector rather than the sender sending its record.**
The sender has both halves once the vector arrives — its own coverage, its
own record — and decides without another round trip. Sending the record the
other way would need a second request to say "serve me anyway", and the
requester already had to derive the threshold from the vector it now sends.
BSON is self-describing and `Message` derives serde without
`deny_unknown_fields`, so a field with a default crosses a version boundary
in both directions: an older sender ignores it and judges by the threshold, a
newer sender given none does the same. Nothing either build did before is
lost, and the round-trip test in `protocol.rs` pins both halves.

**Why the seeding.** The per-origin record and the coarse horizon are written
together, so on a database only this build has collected from, the highest
entry in the record *is* the horizon. A horizon above the record means an
earlier build removed entries the record never saw, from origins it cannot
name. Every held origin is raised to the horizon then — which is exactly the
coarse answer the horizon alone gave, so nothing is claimed that was not
claimed before. The record is exact from that point on, and the cost ends once
each origin has written again and been collected once more: for a cluster
rolled onto this build, the roll *after* this one is the first quiet one. An
origin the node learns of later needs no seeding, because nothing of it was
held here to collect before it was known.

**Alternatives.** Scan the oplog for the oldest entry of the origin above the
peer's coverage: it has been collected, which is the point. Persist per-peer
"last caught up" times: lost on restart, which is when this matters. Exclude a
node's own origin from the verdict: a cluster with one writing member would
then never name a stale peer. Suppress the topology re-registration: the write
is legitimate and the same shape arrives from any idle origin's next write.

**Cost.** One redb table with one 26-byte row per origin, raised in the same
transaction as the retention pass already commits; one map-sized field on
`AskEntries`; one read of the record per sync round. `behind_ms` and
`behindSecs` mean what they meant, at fewer peers. What the record does not
make exact: with `tombstone_retention_secs` longer than
`oplog_retention_secs`, "collected" is judged at the shorter window, and a
peer trailing by more than the longer one whose gap holds only entries aged
between the two is still named — the conservative side, and the same side
ADR-085 was on.

---

## ADR-098 — Query paths are bounded by what they return, not by what they scan

**Decision.** A read may hold memory proportional to its result — the page,
the sort window, the count — and never to the collection or the index range it
walks. Three paths were changed to make that true. `count` counts through a
visitor and keeps nothing. Index candidates stream out of one read transaction
and are rechecked as they arrive: a plan that pins a complete key is one run of
entries already in `_id` order, read in place and stopped where the caller
stops; a `$in` is those runs merged, one head per probe; a range that must be
delivered in `_id` order keeps only the `skip + limit` smallest document keys
of one pass over it, and goes back for more only when the recheck rejected
enough that the caller is still asking. A sorted `find` keeps a bounded heap of
the `skip + limit` least under the sort, with `_id` ascending appended as the
final key, and that window has a ceiling: `skip + limit` may not exceed 10,000
on a sorted `find`, refused with `400`.

**Why.** Reading the executor for how memory grows with collection size found
three places where one request's footprint was the collection's. `count` was
`collect_matching(..).len()`: every matching document decoded into a vector
and then counted, which on a `__vectors` shadow is every vector and its text.
The index path materialised every candidate key in the range, sorted and
deduplicated, before rechecking the first — on the order of hundreds of
megabytes for an unselective equality over ten million documents at
`limit: 1` — and a `$in` union built a set on top. A sorted `find` collected
every match as a `(stamp, document)` pair to sort it, and `skip` had no bound
at all. On a small host each of these was the likeliest way for one ordinary
request to take the node, and every other client's requests, down with it.
None of the fixes changes an answer: the count is the same count, the
candidates are the same candidates in the same order, and the bounded sort
produces the page the stable sort did — `_id` ascending was the order the scan
fed it, and is now stated as a key instead of relied on as a property of the
input.

**Why the index path has three shapes rather than one.** Index entries sort by
`(key, document key)`. Under one complete key the document keys are already in
order and each document appears once, so an exact probe needs no sorting and
no set, and a merge of exact probes needs one head per probe. Under a range of
keys the document keys interleave, and nothing short of seeing the whole range
says which `_id` comes first — so the range is still read in full, as it was,
but a bounded set keeps the `skip + limit` smallest instead of the whole range
sorted. The planner now says which case a plan is (`IndexPlan::exact`),
because the executor cannot tell from the byte ranges alone: an equality on a
prefix of a compound index is a range in disguise. A multikey range read in
index order recognises a repeat by recomputing the document's keys and taking
it at the first the range covers, rather than by remembering every document
seen.

**Alternatives.** *Index-backed counts* — answering `count` from the entries
without touching documents — were deferred: the recheck is what makes an
index-backed answer correct, and a count that trusted the index alone would be
wrong wherever the index is a superset (a multikey range, a partial filter the
query did not prove). It remains the obvious next step for the common case of
an exact probe with no residual filter. *Keeping an unbounded `skip` with a
warning* was rejected: a warning is read after the node is gone. *Clamping the
sort window as `limit` is clamped* was rejected because a clamped `skip`
returns a different page from the one asked for and says nothing — the one
kind of wrong a client cannot detect. *A cursor over arbitrary sort keys*,
which would make deep sorted paging cheap, is the longer road the deviations
register already names and is not taken here.

**Cost.** The sort window is a behaviour change: a sorted `find` with
`skip + limit` over 10,000 worked before and is refused now. Under the letter
of [Compatibility](compatibility.md) a tightened refusal is breaking; this one
ships in `/v1` as a capacity ceiling — the same class as the body limit and
`MAX_LIMIT` — with a `0.MINOR` bump and a release note, and the compatibility
document now records that exception in its table. A range plan in `_id` order
whose recheck rejects most candidates makes more than one pass over the range,
logarithmically many in what was rejected, where it made one; the common case
is one pass, and it no longer holds the range. `explain` gains
`indexEntriesRead`, so the difference between an exact probe stopped early and
a range read in full is visible to a client tuning a query.

---

## ADR-099 — Authenticated routes carry a request timeout, an explicit body ceiling and a per-principal rate limit

**Decision.** Three settings, each with a default chosen so that a node which
never sets them behaves as it did before:

- `server.request_timeout_secs` (default `30`) — a deadline on every REST
  route that answers with a document, applied per route group. A request
  still pending at the deadline is abandoned and answered `503` with a new
  error code, `timeout`, `retry: wait`. The change-stream upgrade
  (`/v1/db/{db}/coll/{coll}/watch`) and `/mcp` — the two surfaces whose
  response is a connection rather than a document — carry no deadline.
- `server.max_body_bytes` (default `2097152`) — the request body ceiling
  axum applied on its own, now a setting. Over it, `413 payload_too_large`
  as before.
- `server.rate_limit.per_principal` and `per_principal_window_secs`
  (defaults `0`, meaning off, and `60`) — a second token bucket keyed on the
  authenticated principal, checked in the `Auth` extractor after the token is
  verified and the session confirmed. Over it, `429` with `Retry-After`, the
  login limiter's response. Refusals are counted in a sibling series,
  `kimmy_rate_limited_principal_total`, and the key map is capped by
  `max_tracked_keys` exactly as the login limiters' are.

**Why.** ADR-007 put a limiter on the one unauthenticated route where a limit
is a security control, and left the authenticated routes unbounded on the
argument that a capacity number without a measurement behind it is a guess.
That argument still holds for the *number*. It never held for the
*mechanism*. A principal that is compromised, or a client with a bug in its
retry loop, can hold connections open by sending a body slowly, send bodies as
large as the framework allows, and make requests as fast as the network
carries them — and until now the only answers were a proxy in front of the
node or revoking the user. These are the three of those an operator can now
bound on the node itself, with the number left to them.

*What the deadline bounds, and where it lives.* The timeout wraps the whole
service call for a route and fires when that call is still *pending* at the
deadline. A request to this server is pending in exactly two places: while
its body is still arriving, and while an embedding provider is being waited
on. Storage work is synchronous and never yields, so the deadline cannot fire
inside a scan, a bulk insert, an index backfill or a database drop — a request
that finishes late is answered with its result. That is the right outcome for
those operations (turning a completed index build into a refusal would be
strictly worse than answering late), and it is why the default stays at 30 s
without exempting any of them individually. It also means the setting is not
a query timeout, and every place that documents it says so. The layer is
applied per route group rather than to the whole table so that the exemption
is a place a route is registered — visible in a diff — and not an attribute on
one line of forty.

*Why 503, and why `wait`.* RFC 9110 §15.5.9 defines 408 as the server not
having received a complete request within the time it was prepared to wait,
and permits the client to repeat the request; browsers and several HTTP
libraries do so silently. That is the wrong instruction for a request this
node may have partly acted on — a write whose embedding call stalled — and
says nothing true about the provider case at all. 504 is a gateway's
statement about its upstream, and this node is the origin. 503 says what
happened: this server did not handle this request. The envelope's `retry`
says what to do about it, and it is `wait` rather than `elsewhere` (ADR-057)
because the deadline is only ever reached while waiting for the client's own
body or for the provider every node shares, and neither improves by moving
nodes — `elsewhere` would send the same slow upload round the whole cluster.
No `Retry-After`, because the server has no idea when a slower client or a
slower provider will be faster.

*Why the per-principal limit is in the extractor and not a layer.* The
principal is not known until the token is verified, so a layer keyed on it
would have to authenticate too — either verifying twice or caching the result
in request extensions for the handler to find. The `Auth` extractor is the
one place every authenticated surface already passes through: REST handlers
take it, `/mcp`'s middleware calls it, the change-stream upgrade takes it.
Checking there covers all three by construction, and ordering the check after
the session check means a refused token is a `401` and never spends a real
user's budget: the limiter counts principals, not guesses. The key is
`local:<name>` for a local user and `oidc:<issuer>:<sub>` for a federated one,
so a provider's `root` and this cluster's `root` cannot share a budget, and
the two classes can never collide. `Limiter::acquire` checks and spends under
one lock, because here every request counts rather than only the failures
and `check` followed by `record` would admit a whole round of concurrency
past the burst.

*Why the limit is off by default.* The number is the operator's; the
mechanism is the server's. The documentation offers a starting point — 3000
over 60 seconds, fifty a second sustained per principal — and says what it is
relative to, and names the series that tells an operator whether it is right.

**Alternatives.**

- *A global timeout, WebSocket upgrades included.* Rejected. axum hands the
  upgraded socket to a task of its own once the `101` is written, so a global
  layer would today happen not to break change streams; but `/mcp`'s
  streaming responses would be cut, and the exemption is the contract rather
  than a property of the current upgrade path. The test holds the contract.
- *408 or 504.* Rejected, above.
- *Per-route body limits* — a larger ceiling for `/bulk`, a smaller one for
  login. Deferred. One ceiling matches what was already enforced; splitting
  it is a decision that wants bulk-import workloads measured first, and the
  mechanism (a `DefaultBodyLimit` on a route group) is a one-line change when
  they have been.
- *Cooperative cancellation of storage work at the deadline.* Deferred. It
  needs a deadline threaded through the engine's scans and commits, and what
  a commit past its deadline should do — finish, or roll back — is a storage
  decision rather than an HTTP one.
- *Wiring `max_body_bytes` into rmcp's own body limit.* Deferred. rmcp reads
  `/mcp` bodies under its own 4 MiB ceiling and answers its own `413`; making
  the two one setting means either accepting rmcp's envelope for that route
  or reading the body twice.
- *A label on `kimmy_rate_limited_total` instead of a sibling series.*
  Rejected. A label changes the shape of a series production dashboards
  already name; a sibling leaves the existing one byte-for-byte what it was.

**Cost.** A new error code, `timeout`, in a set clients branch on — additive
under ADR-057, because the envelope carries `retry`. A client uploading a
2 MiB body slower than about 70 KB/s now sees a `503` where it saw success;
the setting exists to raise. Every authenticated request costs one integer
comparison when the per-principal limit is off and one lock acquisition when
it is on. The 429 counter is no longer a single-source number:
`kimmy_rate_limited_total` counts both limiters, and
`kimmy_rate_limited_principal_total` is how they are told apart.

---

## ADR-100 — Local login is a mode, and a federated subject has a display name that is never an identity

**Decision.** Two settings, one about each way in.

`auth.local.login` says where `POST /v1/auth/login` answers: `always` (the
default, and exactly what shipped), `loopback_only` (only to a connection whose
TCP peer is a loopback address; anyone else gets a 403 with the `forbidden`
code), or `disabled` (the route answers 404). `/v1/auth/refresh` follows the
same rule, because it mints a local token too. The mode governs **minting**
and nothing else: a local token already issued keeps verifying under every
mode, on every node, until it expires. Startup refuses `disabled` unless
`auth.oidc` is configured, since a node with neither could authenticate
nobody. The peer is the socket's, never a forwarded header — the setting is
about who can reach the process, and a header is something a client writes.

`auth.oidc.subject_claim` names a claim — `preferred_username`, `email`,
`upn` — whose string value rides on a federated principal as its **display**
name. It appears in the audit record (as `display`, only when it differs from
the subject) and in `/v1/auth/whoami` (always, falling back to the subject).
It appears nowhere else: `sub` remains the principal's name for authorization,
role resolution, rate limiting, the `federated` flag and every comparison the
server makes. A claim that is missing or not a string falls back to `sub`
without refusing the token.

**Why.** Federation made two things true at once that pulled in opposite
directions. A node behind an identity provider still needs its break-glass
root: ADR-067 reserved `admin` to local accounts precisely so that a
misconfigured or compromised provider cannot mint a superuser, and that
account is worthless if it cannot log in. But the password route is also the
one unauthenticated endpoint that costs Argon2 work per attempt and accepts a
guess from anywhere, and an operator whose people all arrive through the IdP
has no reason to leave it open to the network. `loopback_only` is the middle
position that keeps both properties: root is reachable from the host — over
SSH, from a sidecar, through a tunnel — and unreachable from the network the
IdP was meant to front. `disabled` exists for the deployment that has decided
to run its emergency account elsewhere (a second node bound to loopback, say)
and wants this one to hold no password door at all; the startup refusal keeps
it from being chosen by accident on a node with no other way in.

The subject claim answers a different complaint. A subject from a real
provider is an opaque identifier — a GUID from Entra ID, a `00u…` string from
Okta — and an audit line that says `user=3f2a…` tells the person reading it
nothing until they open the provider's console. Every deployment that has
tried to read its own audit trail has asked for the email. The tempting fix is
to *use* the email as the principal, and it is wrong: an email is mutable (a
rename at the provider would silently make one person two principals, or two
people one), it is not unique across providers, and some providers let a user
set it. `sub` is the one claim OpenID Connect makes stable and provider-scoped,
which is why ADR-064 built on it. So the readable name is carried **beside**
the identity, labelled as display, and consulted by nothing that decides
anything. A principal whose email changes keeps its roles, because its roles
never depended on the email; a test holds that.

**Alternatives.**

- *Remove local login entirely once `auth.oidc` is configured.* Rejected: it
  deletes the break-glass account ADR-067 and ADR-074 both lean on, and the
  canonical pattern elsewhere (Vault, MinIO, Grafana) is an emergency local
  account kept alongside SSO, not removed by it.
- *A 404 for `loopback_only` as well as `disabled`, to hide the route.* A 404
  from a route that answers 200 to the neighbour is not a secret, it is a
  puzzle; the route is documented and its existence is not the thing being
  protected. 403 with the `forbidden` code says what happened, and the CLI can
  turn it into advice. `disabled` answers 404 because there the route really
  is absent from what this node offers.
- *Honour `X-Forwarded-For` for the loopback test.* Rejected: the header is
  client-supplied, and the login rate limiter already documents why trusting
  one without a proxy that rewrites it is worse than nothing. A reverse proxy
  on the same host will look like loopback, and the documentation says so
  rather than the code pretending otherwise.
- *Use the email as the principal, or key roles on it.* Rejected above; it is
  the whole point of the decision.
- *Put the display name in the local token.* There is no local token for a
  federated principal, and minting one is the identity laundering ADR-065
  refuses.

**Cost.** A deployment that sets `loopback_only` behind a same-host reverse
proxy gets no restriction from it and has to know that; the docs say it in
three places. `refresh` under `loopback_only` refuses a client that logged in
from the host and later refreshes from elsewhere, which is the rule applied
consistently rather than a gap. The audit record grows a field that appears
only on federated lines whose provider set the claim, and any collector
keyed on the exact field set has one more optional field to know about.
`whoami` grows a required `display` field, which is additive.

---

## ADR-101 — Local signing secrets rotate through a two-key window

**Decision.** `auth.jwt_secret` gains an optional companion,
`auth.jwt_previous_secret` (`KIMMY_JWT_PREVIOUS_SECRET`). Every local token is
signed with the current secret; verification tries the current secret first and
the previous one only if the current one finds the signature wrong. A token
neither verifies is refused with the unchanged invalid-token error. The previous
secret is held to `MIN_SECRET_LEN` and must differ from the current one, and
`check-config` refuses both faults exactly as the node does. Rotation is the
documented procedure: previous = old, current = new, roll every node, wait one
`token_ttl_secs`, remove the previous secret, roll again. The node logs an
`info` at startup naming that deadline and one `warn` when it passes, counted
from process start and not persisted.

**Why.** The secret was not being rotated, and the reason was mechanical:
changing it invalidated every outstanding token on every node at once, so a
rotation was an outage for every client holding a session. A
control that costs an outage is a control that is not exercised, and a secret
that is never rotated is one whose exposure is never recovered from. The
standard remedy for a symmetric key is a window in which two keys verify and
one signs; because tokens carry their own expiry and the issuer only ever signs
with the new key, the window closes by itself one lifetime later. The check
order matters in one respect only — an expired token is reported as expired by
whichever key verified its signature, and that answer is final, because a token
the current key signed was never signed by the previous one. The token version
check (ADR-052) runs after the signature check in `Auth`, whichever key passed
it, so rotation and revocation stay separate: rotating does not revoke, and
revoking does not need a rotation.

**Alternatives.** *A `kid` header and a key ring* — the general form, where
each token names its key and the verifier holds any number. Deferred: HS256 with
two keys covers what an operator needs (one rotation at a time, closing on its
own), and a new header claim would be a wire change that every existing token
lacks, so the verifier would need the two-key fallback anyway for the first
rotation. *JWKS-style key ids for local tokens* — publishing local keys the way
a provider publishes its own. Rejected for now: local tokens are symmetric and
verified only by the nodes that hold the secret, so there is nobody to publish
to, and a key set implies an asymmetric scheme this database has not chosen.
*Persisting when the previous secret was first seen*, so the reminder survives
a restart. Not done: a node's data file is the wrong place for a fact about its
environment, the reminder exists to be noticed rather than relied on, and
counting from process start is exact in the common case (the rotation *is* the
restart) and only ever conservative otherwise.

**Cost.** A stolen previous secret stays valid until it is removed — the window
is a deliberate extension of exposure, bounded by the operator following the
procedure, which is why the docs say to remove it after one lifetime and the
node says so twice. The reminder is per process and forgets on restart. The
cluster secret is not covered; it is a different key for a different channel,
and its rotation remains what it was.

---

## ADR-102 — Vector-search filters use the planner, and the exact and lexical paths hold only the top k

**Decision.** The `filter` of `vector_search` and `hybrid_search` is evaluated
by the executor's planner-backed read — the primary key when it pins `_id`, a
secondary index when one applies, a collection scan otherwise, every candidate
rechecked against the full filter — and only the matching ids are kept, a
page at a time. The join with the shadow collection then runs in whichever
direction is cheaper: when the filter admits at most 1,000 documents, their
chunks are read by key and scored exactly; above that, the search runs as it
would unfiltered and hits outside the set are discarded. Separately, the exact
vector path and the lexical half of hybrid search rank through a bounded set
that holds the best `k` chunks — the per-document cap applied as chunks
arrive, ties broken by chunk key — rather than collecting a hit per chunk and
sorting.

**Why.** Both paths held memory in proportion to the collection for a request
whose answer is `k` hits. The filter was a `for_each_doc` over the source
collection with no planner at all, building a set of every matching id, so an
index on the filtered field bought nothing and a filter that admitted three
documents still decoded a million. The exact path (every collection under 500
chunks, every `dot`-metric collection, and the fallback for a failed graph
build) and the lexical path (every hybrid search, at four times `k`) pushed a
hit — text included — for every chunk they scored and sorted the vector.
ADR-098 states the rule these break: a read may hold what it returns, not what
it walks. Routing the filter through `exec` is also what makes the answer
consistent with `find`: the same plan, the same recheck, the same primary-key
short cut, and `explain` on a `find` with the same filter tells an operator
what the search will get.

**Why the join has two directions and a fixed boundary.** With the allowed set
in hand, reading the admitted documents' chunks by key costs the size of the
set and is exact; scanning or walking the graph and discarding costs the size
of the collection, and for the graph is approximate twice over — the walk is
widened eightfold when a filter is present and still returns fewer than `k`
when the set is small. So the keyed join wins whenever the set is small, and
the discard join wins when the set is most of the collection, because then the
graph's candidates are mostly admitted anyway. The boundary is a count rather
than a fraction because the keyed join's cost does not depend on the
collection: a thousand documents' chunks read by key is the same work over a
million documents as over two thousand, and a thousand is comfortably past the
widest window any request can ask for (`MAX_K` is 1,000, and hybrid's halves
run at `4k`). A fraction would need the collection's size, which is a scan to
learn. A document's chunks are one contiguous run under its id because chunk
keys are `{source}#{chunk}` and string keys encode in `_id` order, so the keyed
read is a bounded range, not a probe per chunk number; the same run now serves
the single-document reads (`get_vectors`, the worker's staleness check), which
were each a scan of the shadow.

**Why a bounded set with the cap inside it.** The per-document cap is what
stops a long document filling every slot, and it cannot be applied after a
heap of size `k` without making the heap unbounded — a document with ten
thousand chunks better than everything else would need all ten thousand held
to find the other nine documents. Applied on insertion it is exact: a chunk
of a document already holding its allowance has to displace that document's
own worst or it is out regardless of where it stands globally, and a chunk
that cannot beat the set's worst is out regardless of its document. Ties are
broken by the chunk's key so that equal scores rank the same way on every
run and on both join directions; the old stable sort ordered them by scan
position, which the keyed join does not have.

**Alternatives.** *Filtered traversal inside the graph* — passing the allowed
set to the walk so it never visits an excluded node — is the right long-term
answer for the middle ground, a filter that admits ten thousand of a million,
where the keyed join reads too much and the discard join finds too little. It
needs a graph that exposes its traversal, which the current one does not, and
is deferred. *A per-collection inverted index for the lexical half* would make
keyword search a posting-list merge rather than a scan and tokenisation of
every chunk; it needs term statistics maintained under replicated writes, and
the hybrid fusion-controls change (#181) already defers a BM25 lexical half
on the same ground. The bounded set makes the scan's memory acceptable
meanwhile; its time is still linear. *A proportion of the collection as the
boundary* was rejected above. *Reading the matched documents in one call*
rather than in pages was rejected because an unselective filter would then
hold every matching document at once — the failure ADR-098 names — where the
paged read holds a page and the ids.

**Cost.** A filter admitting between a few hundred and a thousand documents
on a small collection reads by key what a scan would have read in sequence:
the same records, a seek apiece. The tie order among equal scores changed
from scan position to chunk key; it was never specified. The paged read of the
filter re-plans once per page, which on an index plan today gathers the range's
candidate keys per page; the executor's streaming visitor makes that a seek,
and the filter's read is written to become one call to it. Hybrid search's
lexical half still ignores `filter` — a pre-existing gap this change neither
widens nor closes.

---

## ADR-103 — HNSW graphs are built off the lock and live under a budget

**Decision.** `IndexCache` takes its cache-wide lock only to look an entry up
and to install one. The build itself — the O(n log n) graph construction and
its reachability probe — runs between the two under a per-collection lock, on
a thread the async runtime has been told about (`kimmy_storage::blocking`, the
mechanism a storage commit uses for its fsync). A second search for a
collection being built takes the graph that already exists, under the
staleness rule ADR-022 set, or, when none exists, waits for that one build
rather than starting another. The build reads only each chunk's key and
vector, releases each vector as the graph copies it in, and keeps a sample of
128 for the probe. Resident graphs are budgeted by
`vector.index_cache.max_bytes` (default 512 MiB; `0` unbounded): each graph is
charged an estimate, `chunks × (dim × 4 + 5,000) + Σ (key length + 24)`, and
when installing one would exceed the budget the least recently searched graphs
are evicted first. A graph larger than the whole budget is installed anyway,
with a warning once. `/metrics` reports the resident total as
`kimmy_vector_index_cache_bytes`.

**Why.** Three findings from reading the build path, each a way for one
vector collection to take a node down without any request being unreasonable.
The build ran under the one lock every vector search on every collection goes
through, so a 4 s rebuild at 4,000 vectors — minutes at tens of thousands —
was 4 s in which no vector or hybrid search on the node returned; and it ran
on an async worker, which is the defect the 0.16.2 commit fix removed from
writes. The build materialised every `VectorRecord`, text included, and held
them until the probe had finished, so its peak was the graph plus the whole
shadow collection, paid every staleness window under writes and up to three
times when a build was discarded. And graphs were never released: at 6.5 KB
per 384-dimensional chunk — measured, and about twice what the vector alone
suggests, because `hnsw_rs` spends about 5 KB per node on neighbour lists and
per-layer tables whatever the width — a node's resident memory was the sum of
every collection ever searched, with nothing to say where it would stop. The
default is twice `storage.cache_bytes` rather than equal to it because the two
evictions are not alike: a page-cache miss is microseconds, a graph eviction
is a rebuild, so the graph budget is the one that should rarely be reached.
It is a ceiling, not an allocation; a node whose searched collections fit in
less uses less, as before.

**Alternatives.**

- *Parallel insertion.* `hnsw_rs::parallel_insert` was measured
  ([Benchmarks](benchmarks.md)): 3–4× faster on a ten-core host, saturating
  at four threads, with recall and reachability indistinguishable from the
  sequential graph. Not adopted, for now: a four-thread pool is every core of
  the hosts this project runs on, which returns the stall to the request path
  in a different suit, and a pool bounded to half the cores is one thread on
  those hosts anyway; it needs rayon as a direct dependency; and the
  reachability thresholds (ADR-061) were sized over hundreds of sequential
  builds, not three parallel ones. Recorded with its numbers so the decision
  can be reopened with a rebuild backlog in hand.
- *Memory-mapped graphs.* `hnsw_rs` can hold vectors as slices of a mapped
  file, which would take the `dim × 4` term out of resident memory and leave
  the 5 KB of bookkeeping — the larger term at common widths. Deferred: it
  changes the snapshot layout and the failure modes of a torn file, for a
  saving the budget already bounds.
- *Serving graphs from disk.* The snapshots already persist a built graph
  across restarts through `hnsw_rs`'s dump and reload; serving *from* the
  file rather than reloading it whole is the path to a graph that needs no
  budget at all. Deferred for the same reasons as mapping, of which it is the
  larger half — and an evicted collection already comes back through its
  snapshot, which is the cheap half.
- *A per-collection opt-out of the graph* (`index: false`, so a collection
  always scans). Left out: `VectorConfig` is a `deny_unknown_fields` struct
  built literally in nine places across the crates, and a field with a
  non-`false` default does not fit that shape cleanly; the size threshold and
  the budget cover the case it was for.
- *Refusing a search whose graph does not fit.* Rejected outright: the exact
  path exists, and a memory policy must never change what a search returns.

**Cost.** The size is an estimate, not an accounting — the allocator's own
overhead sits on top, and an in-flight search holds its `Arc` past an
eviction, so resident memory can exceed the budget briefly. A collection whose
graph is evicted pays a snapshot reload, or a rebuild, on its next search, so
a budget sized below the routinely searched set becomes churn;
`kimmy_vector_index_cache_bytes` pinned at the bound is the sign. Concurrent
searches for a collection with no graph yet all wait for the one build, which
is the trade against duplicating it. One more per-collection lock, one more
setting, one more series.

---

## ADR-104 — Array elements are addressed by filtered identifiers, not by query position

**Decision.** An update path may contain `$[]` and `$[<identifier>]`
segments, with the identifiers defined by an `arrayFilters` field on the
`update` and `find_and_modify` requests — camel-cased like `returnDocument`,
because it is MongoDB's name for MongoDB's feature. A filter document names
one identifier and is evaluated against each element with that prefix
removed; every identifier a path uses needs exactly one filter, and every
filter must be used. Inside the write transaction the positional segments are
expanded against the document into concrete index paths, and the existing
operators apply to those, so every operator that takes a path gains the
feature without being taught about elements. `$rename` is the exception,
refused as MongoDB refuses it. MongoDB's `$` — "the element the query
matched" — is refused with a message that names the replacement.

**Why.** Before this, one line item in an order could only be changed by
numeric index or by replacing the whole document, and the replacement loses
every concurrent update to the order's other fields. That was the largest
functional gap in the update language, and the one that pushed callers back
to read-modify-write over a database whose write path exists to make that
unnecessary (ADR-083). Of MongoDB's three forms, `$[<identifier>]` is the
general one: it does not depend on the query, it reaches every matching
element rather than the first, and it nests. `$` depends on the matcher
reporting which element satisfied the filter, which `filter::matches` does
not track — it answers *any* over a path's values — and adding that means a
position threaded through every comparison, a rule for which array wins when
several clauses touch arrays, and a value carried from the match into the
update. All of that to express what `$[<identifier>]` already expresses with
the condition written next to the path it governs. Expanding to index paths
rather than teaching each operator about elements keeps every operator
single-destination, which is the invariant `path::set` was built on, and it
makes `$unset` of an element leave a null hole for the reason `a.1` does:
later indices must keep meaning what they meant.

**Alternatives.** Implementing `$` first, because it is the older form:
rejected for the reasons above, and because it is the form MongoDB's own
documentation steers callers away from for anything beyond the simplest case.
A syntax of this project's own (`items[sku=gasket].shipped`): rejected because
an update written for MongoDB should run unchanged, and a filter document is
already the language for "which elements". Accepting an unused filter
silently: rejected because it is nearly always a misspelt identifier, and the
update would then change nothing while reporting `modified`. Carrying the
filters inside the update document (`{"$set": ..., "$arrayFilters": ...}`):
rejected because the update document is a set of operators and nothing else,
and every client that models the request would have to unpack it.

**Cost.** One more request field on two routes, modelled in every client and
covered by one conformance scenario. The expansion walks the array once per
positional operation, on the write path, for the documents that use the
feature only. A ported update that uses `$` is a `400` rather than a write,
which the register records.

---

## ADR-105 — Expressions evaluate in a lexical scope

**Decision.** `Expr::eval` runs in a `Scope`: the root document plus a chain of
frames, one per enclosing construct that binds a name. `$let`, `$map`,
`$filter` and `$reduce` push a frame holding the names they bind and evaluate
their body in it; a `$$name` reference searches the innermost frame first and
walks outward; `$$ROOT` and `$$CURRENT` are the root document and are never
rebound. A `$lookup` `let` is one more frame, laid under every stage of the
sub-pipeline. Names are also tracked while **parsing**, so a `$$name` nothing
binds is refused where it is written rather than evaluating to null per
document. `Expr::eval(doc)` remains and is the empty-scope case, so every
existing caller — `$project`, `$addFields`, `$replaceRoot`, `$group` — is
unchanged.

**Why.** One design cost unlocks four things that were each separately out of
reach. The array family — `$filter`, `$map`, `$reduce` and the eleven
positional operators beside them — was excluded from the first expression pass
with the recorded reason that `$$this` needs a scope, not another operator.
`$$ROOT`, which is how a `$project` embeds its source or a `$group` pushes
whole documents, is the same mechanism with a fixed binding. `$let` is the
mechanism exposed directly. And the `$lookup` `let`/`pipeline` form, the only
way to join on anything but one key's equality, is a scope whose frame is
evaluated once per input document and whose body is a pipeline rather than an
expression. Building the scope once and expressing all four through it is less
code and one rule — *a variable is a name in a frame; frames nest* — where four
special cases would each have carried their own.

The parse-time check follows from the same rule. Because every binder is known
while the tree is built, the parser can carry the lexical environment for free,
and a refusal at parse is the difference between "this pipeline is wrong" and
a null in every row that nobody notices — the failure the expression layer's
null-versus-error rule exists to avoid.

**Alternatives.** *Special-case `$$this` per operator* — have `$map` evaluate
its body against a synthetic document with `this` in it, or thread a single
optional "current element" through `eval`. Rejected: it handles one level of
nesting and not two, cannot express `$reduce`'s two names or `$let`'s
arbitrary ones, gives `$$ROOT` nothing to stand on, and leaves the `$lookup`
form with no way in. Each further operator would have re-derived a scope
badly. *Substitute variables at parse time* — rewrite `$$this` into a field
path before evaluation. Rejected because the element is not a field of any
document the path could name. *Evaluate against a merged document* — clone the
root and insert the bindings as fields. Rejected as a clone per element per
document, and because it lets a binding shadow a real field by accident.

**Cost.** A scope chain per evaluation: a frame is a borrowed slice and a
parent pointer, so pushing one per array element is a stack slot and no
allocation, and `eval` on a scope with no frames is what it was before. The
parser carries a `Vec<String>` of declared names that grows and shrinks with
nesting. The `$lookup` pipeline form is a nested loop — O(local × foreign),
inherent to a form whose body may do anything with the variables — and is
documented as such, with the equality form recommended wherever the join is
one key and a leading `$match` hoisted out of the loop because the filter
language cannot read the variables. The `$$ROOT` value is a clone of the
document, paid only when it is read whole. And the parse-time check means an
expression is bound to the names it was parsed with: `parse_with_vars`
followed by a plain `eval` is an error rather than a null, which is the point.

---

## ADR-106 — `$expr` joins the filter language by delegating to the expression evaluator

**Decision.** `{$expr: <expression>}` is a filter clause. It parses through
`Expr::parse`, evaluates through `Expr::eval` against the whole document, and
matches when the result is truthy under the expression language's rule. It is
accepted at the top level and inside `$and` / `$or` / `$nor` like any clause,
which puts it in every place a filter is taken — `find`, `count`, `update`,
`delete`, `find_and_modify`, `$match`, the vector pre-filter and the MCP tools
— by construction, because they share one parser. The planner never reads it.

**Why.** Every other filter operator compares a field with a constant. "Which
accounts have spent more than their budget" has no spelling in that language:
the value on the right-hand side is a field, and the only way to ask it was an
aggregation — `$addFields` a difference, `$match` on its sign — for a question
that is plainly a filter. That is a scan-side gap with no substitute short of
the pipeline, and the pipeline's expression evaluator already knows how to
read a field, compare two values and do arithmetic. `$expr` is what MongoDB
calls the same bridge, and clients written against MongoDB write it.

Delegating rather than reimplementing means the operator set inside `$expr` is
the expression set — all of it, arithmetic and `$cond` included — and stays so
as that set grows. It also means `$expr` inherits the evaluator's judgements
without a second copy of them: a missing field is null, null propagates, a
type violation refuses rather than yielding null.

**Alternatives.** *A field-reference syntax inside the existing operators* —
`{spent: {$gt: "$budget"}}`, say — was rejected. It changes the meaning of a
string that starts with `$` on the right-hand side of a filter comparison,
which is a legal constant today and stored in real documents; making it a
reference is a silent reinterpretation of existing queries, which the filter
parser has refused to do everywhere else (mixing operators and plain fields,
`$options` without `$regex`). It would also cover only the two-field
comparison and not the arithmetic beside it, so the pipeline would still be
needed for `qty × price > 100`. *A `Result`-returning `matches`* so an
evaluation error could fail the request, as MongoDB does, was deferred: it
touches every caller for a data-dependent case the regex arm already resolves
as "no match", and the register records it as the way to close the gap.

**Cost.** Two, both made visible in `docs/query-language.md` rather than
smoothed over.

*Never indexable.* An expression names no field the planner can bound, so a
filter that is only `$expr` is a full scan, and `explain` says so. An indexable
clause beside it still plans, with the expression applied to each candidate;
the planner treats `Filter::Expr` exactly as it treats a disjunction —
contributes nothing, never narrows. A partial index likewise cannot be proven
usable by an `$expr`.

*Two comparison semantics in one filter document.* `$gt` inside `$expr` is the
expression `$gt`: the canonical cross-type order, whole-array comparison, no
type bracketing. `$gt` outside it is the filter `$gt`: within a type group,
element-wise over arrays. The pairs disagree on precisely the inputs
`docs/query-language.md` already calls out as surprising — a missing field is
*less than* zero inside `$expr` and incomparable outside it — and the page
puts the two readings side by side with the rule of thumb that resolves them:
a constant on the right means the ordinary operator; a field or a computation
on the right means `$expr`.

---

## ADR-107 — The container image ships the release archive's binary

**Decision.** `publish-ghcr.yml` compiles nothing. Each architecture's image
is built with the Dockerfile's `prebuilt` stage from the `kimmyd` inside the
`kimmyd-<target>.tar.xz` that `build-local-artifacts` produced for that
target — taken from the release run's own workflow artifacts, or from the
GitHub Release on a manual dispatch — after verifying dist's checksum and
running the binary once on the runner that will build its image. The two
native runners, the digest-then-merge manifest and the tags of ADR-063 stay.

**Why.** ADR-063 rejected QEMU because the Dockerfile compiled the workspace,
and it compiled it once more per architecture *after* dist had built the
same target: nine minutes on amd64 in a nineteen-minute release, for a binary
that already existed. Worse than the time was the provenance. The file in the
image and the file on the Release page were two builds of one commit — and
not even the same kind of build: the Dockerfile linked against the Debian
image's glibc while the archive is the static musl binary ADR-063 chose so
that one file runs on any distribution and in a `scratch` container. Nothing
checked that the two agreed. Now they are one file: the archive's checksum is
verified before the image is built, and the binary in the image is the one
anyone can download and hash. `--version` on the runner catches an archive of
the wrong architecture before anything is pushed, and it prints the commit
the binary carries, which dist's checkout bakes in.

**What changes for the container.** Its `kimmyd` is now the musl binary,
which is what every other channel already ships. The one difference an
operator could observe is the allocator: the workspace sets no global
allocator, so the container moves from glibc's malloc to musl's, which is
slower under heavy multithreaded allocation. If that shows up in a
measurement, the answer is a global allocator in `kimmyd` — one change that
then applies to every channel alike — not a second build of the server for
the container.

**Alternatives.** One buildx invocation for both platforms under QEMU, with
the binary chosen by `TARGETARCH`, would remove the merge job; not taken,
because the runtime stage's apt layer would then run emulated for arm64 while
native arm64 runners are free, and because the merge flow is the code that
has produced every tag so far. Downloading from the Release in the release
flow too, for one code path instead of two: rejected because the workflow
artifact is the same file one hop closer, and the Release download exists for
the dispatch, which has no run to draw on. `cache-builds` in dist, a
rust-cache in `build-local-artifacts`: evaluated and left off — a cache is
readable only from the ref that wrote it or from the default branch, no job
on main builds the musl targets, and each tag would write a set of caches a
gigabyte or more that no later tag could read, against the 10 GiB budget CI
already had to be pruned back under.

**Cost.** The image on GHCR depends on dist having built the archive first,
which the job order in `release.yml` already guaranteed (`custom-publish-ghcr`
runs after `host`); a dispatched republish depends on the Release's assets
still being there. The `GIT_COMMIT` build argument is passed by nothing on the
release path any more; it stays in the Dockerfile for the default stage and a
laptop build. And a `docker build` from a checkout still compiles, so the
laptop image and the published one are built differently — the published one
is now the one whose binary can be checked against a published hash.

---

## ADR-108 — The project ships an OSS security baseline: dependency policy, automated updates, signed provenance

**Decision.** Three things, none of which touches the server. *One*: the
dependency graph has a written policy, `deny.toml`, enforced by `cargo deny`
in its own workflow (`.github/workflows/deny.yml`). A known vulnerability
fails; licenses are an allowlist of exactly what the graph carries, the
workspace's AGPL permitted by crate name for the server crates and no
GPL-family license permitted at all; OpenSSL, `native-tls` and `aws-lc-rs`
are banned; crates.io is the only source. The check runs when a manifest,
the lockfile or the policy changes, and weekly. Its scope is the default
feature set — the build that ships, as for `check-native-deps.sh`. Beside it,
`scripts/check-license-boundary.sh` asserts the one rule a single-lockfile
allowlist cannot state: the Apache-2.0 `kimmy-client` depends on no AGPL
crate in its shipped graph. *Two*: Dependabot proposes updates weekly for
Cargo, GitHub Actions, the Go module and the Python project, minor and patch
grouped into one pull request per ecosystem, majors alone, each held for
seven days after publication. *Three*: releases are attested. Build
provenance — SLSA, signed keylessly through Sigstore under the workflow run's
OIDC identity, stored by GitHub — for the container image's manifest digest
now, conditional on the repository being public, and for every release
archive through dist's `github-attestations` once it is. Verification is
`gh attestation verify`, documented in the operations guide. `SECURITY.md`
says what is supported, where to report, what to expect, and what is in and
out of scope.

**Why.** Being open source means being depended on by people who cannot
audit the build. Three questions they are entitled to have answered without
asking: what is in the dependency graph and who decided it could be there;
how quickly a known problem in it is noticed and fixed; and whether the file
they downloaded is the one the release workflow built. Each was answered by
prose or by habit before this — the `Cargo.toml` comments explain why rustls,
and ADR-016's correction records how long a prose claim went unchecked — and
the lesson of ADR-016 is exactly that a claim nothing checks stops being true
without anyone noticing. The policy is the checkable form of what the
comments already say. Advisories in particular are published against crates
that are already in the lockfile, which is why the weekly run exists: a
path-filtered check on pull requests alone would first notice a new advisory
on the next unrelated change to `Cargo.lock`, whenever that happened to be.

Provenance is the piece an operator can act on alone. A checksum beside the
archive proves the download matched the upload; an attestation proves the
upload was produced by this repository's workflow from this commit, with an
identity that cannot be copied off a laptop because it never existed on one.

**Alternatives.** *cosign with a maintainer-held key* — rejected. A
long-lived private key is the thing that gets leaked, and a key rotation is
a thing nobody rehearses; keyless signing under GitHub's OIDC identity has no
key, and the verification question becomes "was this built by that
workflow", which is the question an operator actually has. It also keeps
`cosign` off the verifying side: `gh` is enough. *Renovate* — rejected for
now. More configurable than Dependabot and better at grouping, but a
third-party application with write access to the repository, for a workload
of four ecosystems that Dependabot's grouping and cooldown already contain.
Revisit if the pull-request noise outgrows them. *`cargo audit` in CI* —
subsumed; `cargo deny` reads the same advisory database and adds the three
checks `cargo audit` does not have. *The deny job inside `ci.yml`* —
rejected because a path filter is a property of a workflow, not of a job,
and this check has nothing to say about a change that touches no manifest.
*Enabling dist's `github-attestations` now* — deferred on a fact rather than
a preference: GitHub generates attestations for a private repository only on
an Enterprise Cloud plan, dist emits the attest step with no condition, and
a release that fails at its attest step is a worse outcome than a release
without attestations. The image step carries its own visibility condition
and needs no such wait.

**Cost.** CI minutes: about a minute per run of `deny`, only on changes to a
manifest, the lockfile or the policy, plus one run a week; nothing is added
to the pull-request path for an ordinary change. Dependabot: up to fourteen
open pull requests across the four ecosystems by the configured limits, in
practice one or two a week, each running the full CI; the grouping and the
cooldown are what hold that number down. Two advisories are ignored in
`deny.toml` today, each with its reason beside it — one unfixable upstream
(`rsa`, RUSTSEC-2023-0071: a private-key timing channel the server never
exercises, since it verifies RSA signatures and signs nothing with RSA) and
one fixed by a lockfile bump that is a separate change (`h2`,
RUSTSEC-2026-0258); the second line comes out with that bump, and the
unused-ignore warning is what says so. An ignored advisory is a debt the
file makes visible rather than one it hides. Two follow-ups at go-public,
neither in this repository's code: enable private vulnerability reporting in
the repository settings, which GitHub offers only for public repositories,
and uncomment `github-attestations` in `dist-workspace.toml`, run
`dist generate`, and commit the regenerated `release.yml`.

---

## ADR-109 — A pipeline's leading `$match` is planned like `find`; nothing else is reordered

**Decision.** `aggregate` reads its source through `collect_matching`, the
same planner-backed path `find`, `count`, `update` and `delete` use. When the
pipeline begins with `$match` — one stage, or several consecutive ones merged
into a conjunction — that filter is the scan's filter, so an indexed equality,
range or `$in` fetches its candidates and a primary-key equality fetches one
document. The 100,000-document ceiling is measured against what that filter
admits. Every stage after the leading run executes exactly as before, on
exactly the input it had. A `$match` anywhere else in the pipeline is not
moved.

**Why.** Before this, `aggregate` loaded the whole collection and then ran
the stages, so a pipeline over a collection past the ceiling was refused no
matter how selective its `$match` was — the one case where "narrow the
pipeline with an earlier `$match`", which the refusal recommended, could not
help. Reading through the planner fixes that without a second planner: the
filter language is shared, `Stage::Match` already holds a `Filter`, and
`collect_matching` already re-checks every candidate against the full filter,
so an index can only narrow the candidate set and never change the answer.
The scan is asked to stop one past the ceiling, so a `$match` that admits too
much is refused without materialising everything it admits.

Only the *leading* run is safe to push down, and the reason is what the
documents look like when the stage runs. A leading `$match` sees documents as
stored, so its filter is a filter over the collection. A `$match` after
`$project` reads the projected shape; after `$unwind`, one element per
document; after `$group`, the buckets. Moving any of those to the source would
change what they match. The conservative rule — plan the prefix, run the rest
— is the one MongoDB's optimizer follows too, and it keeps a pipeline's
meaning independent of which indexes happen to exist.

**Alternatives.** *A general stage reorderer* that hoists a `$match` past
stages it provably does not depend on (`$sort`, `$skip`/`$limit` under some
conditions, `$addFields` on other fields). It is real value and a real
analysis — field dependence through computed expressions, `$unwind` changing
cardinality — and nothing here needs it yet; when it comes it composes with
this rather than replacing it. *Planning every `$match` independently* is not
possible: only the source is indexed. *Applying the ceiling to the collection
as before and only using the index for speed* would have kept the refusal
that motivated the change.

**Cost.** A pipeline with no leading `$match` behaves as it did, through the
same scan. `aggregate` has no `explain`, so the access path a leading `$match`
gets is visible only by sending the same filter to `find` with `explain` — the
same planner, so the same answer; the docs say so. The refusal for an
oversized source now names the leading `$match` when there is one, and says
"more than" the ceiling rather than an exact count, because the scan stopped
counting there.

---

## ADR-110 — A written threat model and a per-release SBOM

**Decision.** Two documents: one written once and kept, one generated per
release.

[`docs/threat-model.md`](threat-model.md) states the assets, the actors and
trust boundaries, the threats considered at each boundary with the control in
place and the file it lives in, what is out of scope, and the operational
assumptions the controls rest on. Every claim in it was checked against the
code before it was written down; the two that could not be settled from the
code alone are marked *verify* rather than asserted. Controls that are in
review at the time of writing are marked *next release* and named by their
setting, so the document is correct for the release it ships with.

Every release ships a CycloneDX 1.5 JSON software bill of materials **per
shipped binary per target** — `kimmyd-<target>.cdx.json` and
`kimmy-cli-<target>.cdx.json`, with a `.sha256` beside each — generated by
`scripts/sbom.sh`: `cargo cyclonedx` at a version pinned in the script,
fetched on the release runner as a prebuilt binary and checked against a hash
recorded in the script rather than the one published beside the download. The
files are attached through dist's `[[dist.extra-artifacts]]`, which runs the
script in the global-artifacts job after every platform build and uploads
each named file beside the archives; the generated `release.yml` did not
change.

**Why.** [Security](security.md) grew as the mechanisms did, a section per
mechanism, so the question "what happens if *this* is compromised" had to be
answered by reading all of it and holding it at once. Several things were true
and written nowhere together: that a member is inside the trust boundary
rather than at it; that membership gossip is authenticated but readable and
replayable; that a `ddl` holder chooses where a collection's text is sent and
which environment variable authenticates the call; that nothing is encrypted
at rest. A threat model is the document arranged by adversary rather than by
feature, and writing it against the code rather than from memory is what
found the provider-endpoint edge and a not-defended-table row that had been
stale since ADR-040 (corrected alongside) — which is the argument for writing
one at all.

The bill: a release is a few hundred crates, and which ones at which versions
is knowable from `Cargo.lock` only by someone holding the source at the right
commit and a toolchain. The person asked "are we exposed to advisory X" is
holding an archive or an image, and needs the answer from what they hold, in
the format their scanner already reads.

**Alternatives.**

- *SPDX rather than CycloneDX.* Both are standards and every scanner reads
  both; `cargo sbom` emits either. CycloneDX, because the Rust generator is
  maintained by the CycloneDX project itself, because the dependency graph
  and per-component hashes are first-class in 1.5, and because it is what
  Dependency-Track and grype consume natively. A consumer that requires SPDX
  is one conversion away (`cyclonedx-cli convert`); publishing both was
  rejected as two files that can disagree.
- *One bill for the workspace.* Rejected. The graph differs by target —
  sixty-nine crates, Windows, wasm and Android among them, appear only when
  every target is included — and a union would have a scanner flagging code
  that is not in the binary. One per binary per target mirrors the archives
  exactly, name for name.
- *Generating the bill on every pull request.* Rejected. A bill describes a
  released artifact; one per PR is a file nobody consumes and a tool download
  per run, and the question it would answer on a PR — did the lock file gain
  something — is what the lockfile diff and `scripts/check-native-deps.sh`
  already answer. Generation is cheap to run by hand
  (`scripts/sbom.sh <target>`) when someone wants to look before a tag.
- *A custom publish job uploading with `gh release upload`.* Would work, and
  would allow a version in the filename. `extra-artifacts` is the mechanism
  dist provides: the files are listed in `dist-manifest.json`, the workflow
  stays generated rather than hand-edited, and the archives are not versioned
  in their names either.
- *An SBOM attestation on the container image (`buildx --sbom`).* Deferred.
  It would describe the Debian layer and see nothing of the crates inside a
  static binary, and it touches the per-architecture build step of the image
  workflow, which is being reworked at the same time (ADR-107). The musl
  `kimmyd` bill describes what the image ships.

**Cost.** The threat model is prose, and prose goes stale; the mitigation is
the one the rest of the documentation uses — file references so a claim can
be checked, and *next release* markers that have to be removed once those
settings ship. The bill adds one pinned tool download to the global-artifacts
job and a hash to bump when the tool is upgraded. It is not signed: its
checksum proves the download is the file the workflow uploaded, not how the
workflow built it, and a build attestation is a separate control. The CLI's
bill follows the archive's name (`kimmy-cli-…`) rather than the binary's
(`kimmy`), for the same reason the archive does.

---

## ADR-111 — Parsers and token verifiers are fuzzed on a schedule

**Decision.** Nine libFuzzer targets cover the surfaces that take
attacker-controlled bytes: the filter, update, projection, sort, pipeline and
expression parsers (each evaluated after parsing, not just parsed), the HTTP
edge's JSON ⇄ BSON conversion, the order-preserving key encoder, and both
token verifiers — HS256 with a fixture secret, OIDC with a fixture JWKS held
in memory. They run weekly and on demand in their own workflow, five minutes
per target, never per pull request. The harness bodies are plain functions in
a workspace crate, `kimmy-fuzz-harness`, so the stable PR gate compiles them;
only the one-line `fuzz_target!` wrappers live outside the workspace, in
`fuzz/`, where nightly is needed. The seed corpora are committed and run as
ordinary tests, which is also where a minimised crash goes once it is fixed.

**Why.** [Testing](testing.md) allocates effort by how quietly a bug would
fail, and every one of these surfaces is reached by bytes from a caller who
has not yet proven anything: a request body, a bearer header, a document
that will become an index key. A panic in a parser is a worker gone for one
request; a wrong answer from the key encoder is an index that silently omits
documents. Property tests already state the load-bearing invariants over
generators their authors designed. A coverage-guided fuzzer states the same
invariants over a generator that learns from the code's branches, and so
reaches the inputs the author did not think to generate. That the harnesses
assert invariants and not merely "no panic" is what makes them worth the
runner time: the first run over the seed corpus, before any mutation, found
that `{$set: {"_id.x": 1}}` changed a document's identity past a check that
knew only the literal name, and a few seconds of mutation found the JSON
boundary writing a binary's subtype and never reading it back. Neither
panicked. Both are in the changelog.

The split between harness and wrapper is the part that keeps this alive. A
crate built only by a weekly job breaks the week a function it calls is
renamed and is noticed the week after; a crate the workspace clippy pass
compiles breaks in the pull request that renames it. The libFuzzer wrappers
cannot join the workspace — `libfuzzer-sys` carries a C++ runtime and the
sanitizer build wants nightly — so the wrappers are made trivial and the
harnesses are made stable.

**Alternatives.** *Property tests alone* — kept; they are complementary, and
the fuzz generator for BSON values deliberately mirrors the key-encoding
property test's edges so the two disagree only where one has found something.
*Fuzzing per pull request* — rejected for cost: nine targets at even a minute
each on every push is more runner time than the whole test suite, for a search
whose yield is proportional to time spent, not to how recently the code
changed. *OSS-Fuzz* — deferred until the repository is public; it would supply
the continuous campaign this schedule only approximates, and the harness crate
is laid out so that adopting it is a build script, not a rewrite. *`cargo fuzz`
on stable via `--sanitizer none`* — considered for the PR gate and rejected;
it would still pull the libFuzzer runtime into the workspace, and the crate
split gives the same compile guarantee with none of it.

**Cost.** One more workspace crate, compiled but not shipped, adding
`arbitrary` to the dependency graph. A second lockfile under `fuzz/`. Around
an hour of runner time a week: one sanitizer build shared by the nine run jobs
through a one-day artifact, plus five minutes each. A fuzz cache on the order
of a gibibyte, saved only from `main`. And a standing obligation: a red weekly
run is a bug report against this repository, to be minimised, fixed and turned
into a seed rather than silenced.

---

## ADR-112 — The provider decides how long its tokens live

**Decision.** `OidcVerifier::verify` does not examine a federated token's
lifetime. Signature, issuer, audience, `exp` and `nbf` decide, and how long the
provider chose to make the token valid for is not among them.
`auth.oidc.max_token_lifetime_secs`, its `--oidc-max-token-lifetime-secs` flag,
its `KIMMY_OIDC_MAX_TOKEN_LIFETIME_SECS` variable, `TokenLifetimeExceeded`,
`TokenLifetimeUnbounded` and `InvalidTokenLifetimeLimit` are all removed. No
warning replaces the refusal. This supersedes ADR-096, which shipped in the
previous release.

**Why.** ADR-096 was right about the problem and wrong about whose problem it
is. The problem is real and unchanged: a federated principal's role membership
is frozen in its access token because this database makes no introspection
call, so a revocation at the provider is honoured only when the token expires
(ADR-073), and the width of that window is the token's lifetime. ADR-096
concluded that because the lifetime is the one part of the window readable from
here, it is the part to bound from here. That does not follow.

**The failure mode is catastrophic and cannot be seen in advance.** The lifetime
belongs to the provider and arrives only with a real token. `kimmyd
check-config` passes, the node starts, the startup summary says nothing, and
then every federated request is refused from the first one. An operator who read
the upgrade notes, ran the pre-flight check the notes named and rolled the
cluster a member at a time still ends up with a total authentication outage —
and the 401 they get back is the same 401 an audience mismatch produces, so the
cause is not obvious from the failure either. A default whose blast radius is
"all federated access" and whose detectability before the fact is zero is the
wrong default whatever it protects.

**It fires on ordinary configurations, not exotic ones.** Okta, Google and Entra
ID default access tokens to about an hour; Auth0 defaults an API's to a day.
Every one of those is over a 900-second bound. The refusal's common case is a
correctly configured, widely deployed identity provider, which makes it a
compatibility break disguised as a security control.

**The escape hatch ADR-096 assumed does not always exist.** Its cost section
says an operator can "shorten the provider's lifetime for this resource, which
every one of them supports per resource or per client". The commercial providers
it names do. A small or in-house authorization server may mint one lifetime for
everything it issues, in which case shortening it for this resource shortens it
for every other relying party too. For that operator the only available lever
was raising the limit, which makes the setting a step to discover and perform
before the database works, in exchange for the behaviour they would have had
without it.

**And it is the operator's decision.** What was being refused is their own
identity provider's configuration, which they control and which serves their
other systems too. A database that rejects a token its operator's IdP just
minted — for a property of that IdP, not of the caller or the request — is
making a policy decision that is not its to make. No comparable product makes
it: Elasticsearch's JWT realm, Kafka's SASL/OAUTHBEARER, Trino and MongoDB's
OIDC support all validate the token and accept the lifetime it carries. The
enforcement points that exist in this ecosystem are at the authorization server,
where the operator can act on them.

**Why no warning replaces it.** A rate-limited advisory WARN was implemented and
then removed in the same round. It keeps ADR-096's premise alive — that this
node has an opinion about a setting belonging elsewhere — which is the premise
being rejected. It is unactionable for exactly the operator who most reliably
triggers it, the one whose provider has a single global lifetime. It fires
forever on configurations that are deliberate and legitimate. And a permanent
warning nobody can act on is how a log stops being read, which costs more than
it buys on a node that otherwise warns only about real faults. The window is now
stated in `docs/security.md`, which is where a fact an operator should know but
cannot change from here belongs.

**What this is not.** It is not a claim that the window is harmless. It is the
cost of verifying without an introspection call, it is why access-token
lifetimes should be short, and the security guide says so. It is a claim that
saying so is this project's job and enforcing it is not. An operator who wants a
fifteen-minute window sets one at their provider, where it also protects
everything else they run.

**Cost.** The window is now bounded only by the provider, and a node cannot tell
an operator that theirs is wide. Nothing detects a provider that starts minting
day-long tokens. Both were true of every release before ADR-096 and are true of
every comparable product. The tests that pinned the refusal are kept in inverted
form — an hour-long token, a day-long token and a token with no `iat` must all
verify, at the unit level and end to end — so that reintroducing the refusal
fails the suite rather than passing it quietly.

**Upgrade.** A `kimmy.toml` that still sets `max_token_lifetime_secs` will not
start, because the configuration denies unknown fields; the key has to come out.
The same is true of the command-line flag. `KIMMY_OIDC_MAX_TOKEN_LIFETIME_SECS`
is different and may be left set: clap reads an environment variable only for an
argument that declares it, and nothing declares that one now, so a stale
variable is ignored rather than fatal. The distinction is worth stating in the
release notes rather than giving one instruction for all three, because an
operator told to clear the variable might clear it *before* upgrading, which on
the previous release reverts them to the 900-second default and causes the very
outage this removes.

---

## ADR-113 — Jenkins runs the merge gate; GitHub Actions runs the release

> **Superseded by [ADR-114](#adr-114--github-actions-runs-the-whole-pipeline-again).**
> The Jenkins pipeline was removed when the repository went public and its
> minutes became free. The reasoning below is kept because it is a worked
> example of a measurement overturning the argument that motivated it, and
> because the boundary it drew — what can and cannot leave GitHub — still
> holds.

**Decision.** The day-to-day gate — formatting, clippy, the workspace test
suite, the native-dependency check and the cluster harness — runs on a Jenkins
multibranch pipeline against a self-hosted Linux agent, described by the
`Jenkinsfile` at the repository root. The **tag-driven release stays on GitHub
Actions**: `release.yml` (generated by `dist`, ADR-063) and `publish-ghcr.yml`
are unchanged and are not reimplemented in Jenkins. `ci.yml` is retired only
after both have run in parallel long enough to compare, and is **disabled
rather than deleted** — its triggers reduced to `workflow_dispatch` — so that
returning to Actions is a one-line revert of a current file.

**Why the gate moves.** Two problems, and only one of them is what it looked
like. Measured over the week to 2026-09-01: a pull-request run cost about seven
billable minutes, a push to `main` about twenty-three, and a tag about
eighty-one — twenty tags in seven days. So the *cost* was dominated by release
frequency and by one job inside it, the macOS build at GitHub's 10× multiplier,
neither of which Jenkins touches. Releasing deliberately is the fix for that and
is recorded in `docs/compatibility.md`.

What Jenkins fixes is *speed*, and the mechanism is narrow enough to name: a
persistent agent keeps `target/` and the cargo registry warm between builds,
where a hosted runner unpacks a cache into a fresh machine every time. For a
twelve-crate Rust workspace that is the whole difference. It is not the CPU —
the agent is not obviously faster than a hosted runner, and the first build on a
cold branch will be slower.

**Why the release does not move.** Three things are bound to GitHub and would be
lost, not relocated:

- **macOS binaries.** There is no macOS agent, and Homebrew ships the `kimmy`
  CLI from them (ADR-063). Building them elsewhere means signing and notarising
  them elsewhere, which is a story this project does not have.
- **arm64.** The agent is x86_64 and there is no arm64 agent. ADR-063 already
  weighed the alternative and rejected it: QEMU emulation is the better part of
  an hour per architecture against minutes on a native runner.
- **Build provenance (ADR-108).** `actions/attest-build-provenance` signs
  against GitHub's OIDC workload identity, which no other builder can obtain.
  **It is dormant today and arms itself at go-public** — `publish-ghcr.yml`
  guards it with `if: ${{ !github.event.repository.private }}`, and
  `github-attestations` in `dist-workspace.toml` is commented out for the same
  reason, because GitHub attests private repositories only on Enterprise Cloud.
  A control that currently appears to do nothing is precisely the one a
  migration discards without noticing, which is why it is written down here
  rather than left to be rediscovered.

**The boundary, stated once so it is not re-litigated:** everything before a tag
is Jenkins, everything from the tag onward is Actions. That is also where the
workflows already divided, which is what makes this cheap.

**Reversibility is a requirement, not a nicety.** This repository is private and
will be public again; Jenkins is a private-repo measure. Three rules keep the
exit about an hour's work, and they constrain how the pipeline may be written:
`ci.yml` is disabled rather than deleted; every Jenkins stage is a thin wrapper
around a command the repository already documents as a gate, so neither system
becomes the only place a check exists; and `main` keeps exactly one required
status check, so swapping back is a single edit.

**The trigger to leave is not cost — it is contributors.** Public repositories
get standard runners free, so the money argument disappears on its own. The
argument that replaces it is sharper: a pull request from a fork cannot build on
this Jenkins. It sits in a LAN-private zone, is reached by a periodic scan
rather than a webhook, and authenticates as one person. Actions handles fork
pull requests natively. The day this repository takes outside contributions is
the day the gate comes back.

**Alternatives.** *Move everything to Jenkins*, as a sibling repository
does: rejected for the three reasons above — that repository ships no macOS
binaries, no multi-architecture artifacts and no attestations, so it pays none
of the costs. *Leave everything on Actions and only reduce tag frequency*:
tempting, and it captures most of the money, but it leaves the pull-request loop
on a cold cache, which is the complaint that started this. *Self-hosted GitHub
Actions runners* instead of Jenkins: keeps one system and one configuration
language, and would have been the better answer if a runner could be trusted on
the same host as the development cluster — but a self-hosted runner executing
workflow files from a fork is a well-known hazard, and the moment this
repository is public that is exactly what it would be asked to do.

**Cost.** A second CI system to maintain, and a build agent that shares a host
with the development cluster this database is tested against — so every stage
declares a memory limit, and the pipeline reports disk on every run, because a
full agent is a host problem rather than a CI one. A persistent workspace can
also pass on stale state where a fresh runner could not; a periodic clean build
on `main` is the answer, and it is the one piece of hygiene this arrangement
needs that the hosted one did not.

---

## ADR-114 — GitHub Actions runs the whole pipeline again

**Decision.** The Jenkins pipeline is removed: the `Jenkinsfile`, the
multibranch job and its credential. `ci.yml` runs the merge gate on pull
requests and pushes to `main`, `release.yml` and `publish-ghcr.yml` run on a
tag, and `main` requires the `fmt, clippy, test` context as it always did.
This supersedes ADR-113, one release after it. *(The aggregate context was
retired the next day, as ADR-107 had planned: `main` requires `fmt, clippy`
and `test` directly, and the job that produced the old context is gone.)*

**Why.** ADR-113 existed for one reason: a private repository bills every
Actions minute, and the routine gate was the routine spend — about seven
billable minutes per pull-request push and twenty-three per merge, against an
allowance a single active day could consume. **Making the repository public
removes that reason entirely.** Public repositories get standard GitHub-hosted
runners free with no minute cap, on every plan, and every runner this project
uses is standard: `ubuntu-latest`, `ubuntu-22.04`, `ubuntu-24.04`, the
`ubuntu-24.04-arm` native arm64 runner, and a hosted macOS agent. Nothing here
is a larger runner, which is the one category that is still billed in a public
repository.

The 10× macOS multiplier that was roughly half of all spend — sixty of the
eighty-one billable minutes a release cost — stops existing rather than being
optimised.

**And Jenkins was never the faster option, only the cheaper one.** ADR-113
argued a persistent agent with a warm `target/` would speed up the gate. Four
measurements said otherwise: the test stage took 762s warm against 777s cold,
nextest brought it to 710s, and optimising the test profile made it worse.
`nextest` reports 613s executing 1725 tests against roughly 110s compiling, with
a floor at the slowest single test — four run 248–270s, inserting thousands of
documents where every insert is a real redb write transaction. A warm cache can
only touch the compile half, and optimisation ruled out CPU, so what remains is
`fsync`. Against GitHub's ~420s for the same suite, the self-hosted agent was
roughly 70% slower. Merges took sixteen minutes instead of ten.

So the trade was *cheaper and slower*, and once the cheaper half is worth
nothing there is no trade left.

**The other reason, which would apply even if minutes still cost something.** A
pull request from a fork cannot build on that Jenkins. It sits in a LAN-private
zone, is discovered by a periodic scan rather than a webhook, and authenticates
as one person. A public repository that accepts contributions needs a gate that
runs on a contributor's branch, and Actions does that natively. ADR-113 named
this as the trigger to reverse it; this is that reversal, arriving with the
going-public decision rather than after it.

**What ADR-113 got right, and is retained.** The boundary it drew still holds
and is worth not re-deriving: the macOS binaries, the arm64 artifacts and the
ADR-108 build provenance cannot be built anywhere but GitHub. Provenance in
particular is signed against GitHub's OIDC workload identity, and it stops being
dormant the moment this repository is public — `publish-ghcr.yml` guards it with
`if: ${{ !github.event.repository.private }}`, and `github-attestations` in
`dist-workspace.toml` can be uncommented at the same time so release archives
are attested too.

The release-cadence guidance in `docs/compatibility.md` is also retained,
re-justified. It was written as a cost measure and it is not one any more, but a
tag per round of merges was never mainly a billing problem: every tag is a
published release, a Homebrew formula update, an image and a set of SBOMs, and
twenty in a week tells a reader nothing about which one to run.

**Cost.** One CI system, on somebody else's infrastructure, with the outage
exposure that implies — the same position as before ADR-113. The self-hosted
option is also worse than it was: since 1 March 2026 GitHub charges $0.002 per
minute for self-hosted *runner* usage, which does not apply to an independent
Jenkins but does apply to the obvious middle path of putting an Actions runner
on one's own hardware. And this repository now has a documented episode of
adopting a second CI system and removing it within a day, which is a cost worth
naming: the measurements were cheap, but they were made after the decision
rather than before it.

---
## ADR-115 — An embedding provider is held to a key-variable and address policy

**Decision.** A collection's vector configuration may name only what the node's
operator allows. Three rules, enforced when the configuration is accepted and
again when the provider is built:

- **Key variables.** A hard denylist first, configurable by nobody: every
  `KIMMY_*` variable that is not `KIMMY_PROVIDER_*` is the node's own — the
  signing secret, the cluster secret, the bootstrap password, the previous
  secret during a rotation — and is refused as `api_key_env` by name. An
  `allowed_key_env` entry that would reach one (`KIMMY_JWT_SECRET`, `KIMMY_*`,
  `K*`) is a startup error. Then `vector.provider.allowed_key_env`: exact names
  or a prefix with one trailing `*`, defaulting to `OPENAI_API_KEY`,
  `COHERE_API_KEY`, `GEMINI_API_KEY` and `KIMMY_PROVIDER_*`. *(Amended the
  same day, one release later: `DEEPINFRA_API_KEY` joined the default list —
  DeepInfra is a documented host of the `open_ai` dialect, and a default that
  admits the dialects' own variables but not the documented host's is a
  default that does not match the documentation.)*
- **Endpoints.** The address policy webhooks have had since their SSRF fix,
  moved whole into a new `kimmy-egress` crate so both use one denylist: public
  addresses only unless the host is in `vector.provider.allowed_hosts`; the
  endpoint — or the dialect's default — resolved and every address checked;
  the provider's `reqwest` client resolving through `CheckedResolver` and
  following no redirect.
- **Profiles and a lock.** `[vector.providers.<name>]` defines a provider in
  the node's configuration; a collection names it with a new `ProviderConfig`
  variant, `{"kind":"profile","name":…}`. `vector.provider.endpoints_locked`
  makes `profile`, `byo` and `local` the only kinds configure time accepts. A
  profile's definition is held to the two rules above at startup and by
  `check-config`.

A refusal at configure time is a `400` that names the variable or the host and
the setting that governs it, never a value. Configuring or disabling
embeddings writes an audit record beside the authorization one — provider
kind, endpoint host or profile name, key variable name — as registering a
webhook does.

**Alternatives.**

- *Checking at configure time only.* Rejected. A configuration also arrives by
  replication from another member, having passed that member's API and never
  this one's; a policy the worker does not enforce is a courtesy. So the
  worker and the search path build providers through the same policy, and a
  refused stored configuration is a permanent failure for that collection,
  logged once by name.
- *A denylist alone, with every other variable allowed.* Rejected. The node
  cannot know which of the process's other variables are secrets — a cloud
  credential injected by the platform is not `KIMMY_*` — so the default has to
  be a list of what a provider may read, with the node's own names refused on
  top of it whatever the list says.
- *Applying the webhook policy by copying `egress.rs` into `kimmy-vector`.*
  Rejected. Two copies of an address denylist are two places for the next
  reserved range to be missing from; `kimmy-api` depends on `kimmy-vector`, so
  the module had to move below both. The only thing that could not move
  verbatim was the wording, which now arrives as a `Purpose` the caller states.
- *Making the lock the default.* Rejected for now. A node that ships locked
  refuses every collection's provider until an operator writes a profile, and
  the hosted defaults under their documented variables are exactly what the
  allowlist admits with nothing configured. The lock is for operators who want
  the set of destinations to be a file they own.
- *A warn-only period for configurations that predate the rule.* Rejected.
  The product is pre-launch and every deployment is the operator's own; a
  refusal that says what to change is worth more than a release of warnings
  nobody reads.

**Why.** `ddl` was designed to be granted freely — it is what lets an agent
create its own collections — and a grant that is meant to be given to tenants
cannot also be a grant of the node's environment and network position. Before
this, it was: the endpoint check was a scheme check, and `api_key_env` was any
name at all. The threat model recorded that as a trust statement and told
operators to treat `ddl` as outbound access. A trust statement is not a
control, and this is the control.

**Cost.** One behaviour change for an existing deployment: an Ollama or
llama.cpp on `localhost` or a LAN address needs its host listed in
`vector.provider.allowed_hosts`, where before nothing was checked. Provider
clients no longer follow redirects, so a provider that answers `302` fails
where it used to work. One more workspace crate. A configure-time and a
build-time DNS resolution per provider, the same cost webhooks already pay.
What remains out of scope is narrower than before and stated in the threat
model: a `ddl` holder on an unlocked node choosing a *public* endpoint under
a listed key can send that collection's text — and that key — there. Binding
a listed variable to the hosts it may be sent to would close that without the
lock, and is the next step if it is ever needed.

---

## ADR-116 — An expression field path through an array fans out, as MongoDB's does

**Decision.** The expression layer has its own path resolver. A field path
read as an expression — `"$items.sku"`, `$$ROOT.items.sku`, `$$var.items.sku`
— walks its segments left to right; where a segment lands on an array before
the path ends, the rest of the path is applied to every element that is a
document, elements that are not documents or lack the field are skipped, and
the results form one array per array crossed with no further flattening.
A trailing array is returned as it is, a path that crosses no array is a
single value, a numeric segment is a field name and never an index, and a
missing path is still null. A variable bound to an array of documents fans
out the same way. Stage options that *name* a field rather than compute one
— `$unwind`'s path, `$sort`'s keys, `$lookup`'s `localField` and
`foreignField` — keep reading the single value at the path. The filter
language is untouched.

**Alternatives.** *Keep the first value and tell users to write `$map`.*
That was the state of things and was recorded as a known deviation; it broke
exactly the pipelines people port, and `$size`, `$in` and `$concatArrays`
over a dotted path were errors that MongoDB answers. *Reuse
`path::resolve` and wrap its result in an array when it returned more than
one value.* Rejected: that resolver serves the filter language, so it reads
`items.0` as an index as well as a field name, flattens every array it
crosses into one list, and cannot say whether an array was crossed at all —
`$a.b` over `a: [{b: [1, 2]}, {b: 3}]` would come back `[1, 2, 3]` where
MongoDB gives `[[1, 2], 3]`, and one element found could not be told from a
scalar. *Fan out everywhere a path appears, including `$unwind` and
`$lookup`.* Rejected because those are not expressions in MongoDB either;
`$unwind` needs a single place to write each element back to, and a join key
read one way on the local side and another on the foreign side would join
nothing.

**Why.** An expression asks for a value and a filter asks whether any value
matches, and the two questions have different answers over an array. The
filter language's resolver already models MongoDB's match semantics
faithfully; the expression layer had borrowed it and thrown away all but
the first value, which was wrong in a way that only became visible once
there were operators that consume arrays. A resolver that follows MongoDB's
aggregation rules exactly — including the one-level-per-array rule and the
numeric-segment rule, both easy to get wrong from memory — means a pipeline
that runs there runs here with the same result, which is the whole promise
of the compatibility surface. Pre-launch, the old behaviour has no
compatibility claim, so it is changed outright rather than gated.

**Cost.** A behaviour change in a `0.MINOR`: any pipeline that grouped,
projected or pushed a path through an array and depended on the first value
gets an array now. `$group: {_id: "$items.sku"}` buckets by the whole array,
which is the MongoDB result but not the old one. Two path rules now exist in
the codebase — the filter's, which reads a numeric segment both ways and
collects every value, and the expression's — and the doc comment on each
names the other and the one place they disagree. The expression resolver
clones the values it collects, as the previous one did for the single value
it returned; nothing on the hot filter path changed.

---

## ADR-117 — `kimmyd` sets mimalloc as its global allocator

**Decision.** `crates/kimmyd/src/main.rs` declares `mimalloc::MiMalloc` as the
`#[global_allocator]`, with the crate's default features, on every target the
binary is built for. It is set in the binary crate and nowhere else: a library
that sets a global allocator sets it for every program that links it, and no
library in this workspace may. `libmimalloc-sys` joins `cc` on
`scripts/allowed-native-deps.txt`. Measured 2026-09-01; the full table and its
conditions are in [Benchmarks](benchmarks.md#the-allocator-musl-glibc-and-mimalloc).

**Why.** Every release binary has been a static musl build since ADR-063, and
ADR-107 made the container ship that same file instead of a glibc build of its
own. ADR-107 named the one thing an operator could observe in the change —
musl's malloc where glibc's had been, "slower under heavy multithreaded
allocation" — and said that if a measurement showed it, the answer would be a
global allocator in `kimmyd`. Nobody had measured it. This is the measurement,
taken over a socket with the HTTP benchmark against three servers built from
one commit inside one Linux arm64 container pinned to four cores and 4 GiB:
the glibc binary the container used to ship, the musl binary it ships now, and
the musl binary with mimalloc. One load generator, itself a glibc build, drove
all three, so the client's allocator never moved. Plaintext, the better of two
interleaved runs, requests per second with the p99 in milliseconds:

| Scenario | Clients | glibc | musl | musl + mimalloc |
|---|---:|---:|---:|---:|
| point read by `_id` | 1 | 10,718 · 0.15 | 9,714 · 0.18 | 10,779 · 0.16 |
| point read by `_id` | 8 | 45,116 · 0.28 | **18,345 · 0.78** | 48,217 · 0.28 |
| point read by `_id` | 64 | 51,956 · 45 | **19,682 · 6.2** | 54,686 · 39 |
| `find`, page of 100 | 1 | 1,576 · 0.77 | 1,111 · 1.19 | 2,012 · 0.70 |
| `find`, page of 100 | 8 | 6,381 · 6.1 | **477 · 31** | 8,289 · 2.1 |
| `find`, page of 100 | 64 | 6,257 · 23 | **537 · 239** | 8,304 · 21 |
| `count`, whole collection | 8 | 171 · 70 | **19 · 547** | 201 · 59 |
| `count`, whole collection | 64 | 174 · 728 | **20 · 5,223** | 211 · 540 |
| bulk insert of 100 | 64 | 992 · 325 | **462 · 344** | 1,058 · 208 |
| peak resident set, MB | | 273–300 | 125–147 | 546–576 |

At one client the musl binary is within 10–30% of the glibc one. At eight it
serves point reads at 0.4× the rate, a page of 100 documents at 0.07× and a
`count` at 0.11×, and the ratio tracks how many allocations a request makes,
not how many requests there are: a point read allocates a handful of times,
decoding and re-encoding a page of a hundred documents allocates hundreds. musl's
allocator serialises every allocation on one lock, so four tokio workers stand
in one queue. The two runs of each configuration differed by 1–12% for glibc and
mimalloc and by up to 23% for musl, against a gap of 2.5× to 13×. mimalloc recovers all of it
and then passes glibc by 8–30% on reads, at one client too: the allocator is on
the single-request path as well as the contended one. Writes barely move in
any row, as they should — an insert is mostly fsync (the write gap, in
Benchmarks) — except the bulk path, whose batch decode allocates.

Every number on the benchmarks page before this one was taken from a glibc or
macOS build, which is why none of them showed it. The shipped binary has been
the slow one since 0.1.0 for anyone who downloaded a tarball and since 0.17.0
for the container, and the page said 70,000 point reads a second the whole
time.

**What this does to ADR-016's rule.** The correction on ADR-016 says: the
build pays for `ring`, so do not add a second native stack. This is a second
native library in the default build, and it is accepted with that sentence in
view rather than around it. It is different in kind from what the rule was
written against. The rule's case is `aws-lc-rs`: a second implementation of
primitives the tree already had, bringing CMake and a second build system for
nothing the first stack did not provide. mimalloc is one vendored C library of
a dozen files with no system dependency, no CMake, no `bindgen`, compiled by
the `cc` crate the build already needs for `ring`, so no build or
cross-compile needs a tool it did not need yesterday — the musl arm64 build
in the container above used the toolchain `musl-tools` installs and nothing
else. And it is not a second of anything: the tree had no allocator and took
the platform's, and the platform's turned out to be the regression. The line it
adds to the allowlist is the kind the allowlist's header says is right:
deliberate, with a reason, in a diff. What the rule reads as from here is
narrower and clearer for it — no second crypto stack, and no native dependency
without a measurement that justifies it.

**Alternatives.** *Leave the allocator alone.* The shipped binary would stay
2.5–13× slower under any concurrency above one than the binary that shipped
before ADR-107, on every channel; rejected because the one measurement ADR-107
asked for is in and it is not close. *Ship a glibc build in the container.*
ADR-107's reason for not doing that stands — two builds of one commit, one of
them unverifiable against the published hash — and the tarballs would still be
musl and still slow; a global allocator is one change that reaches every
channel alike, which is what ADR-107 said the answer would be. *jemalloc, via
`tikv-jemallocator`.* Not measured: it builds through autotools and a
configure script rather than `cc` alone, it is a larger library, and mimalloc
answered the question. The rule of thumb applied was one native addition, the
smallest that closes the gap. *mimalloc's `v2` tree, offered as a crate
feature.* Measured once: 3–15% above `v3` on throughput, inside the spread of
the `v3` runs on most cells, with an equal or larger resident set (546–649 MB).
One run inside the noise does not justify leaving the tree the crate's authors
ship by default, so `v3` stays; the feature is there if a later measurement
says otherwise.

**Cost.** A second native build dependency, recorded above. Resident memory:
under the benchmark's write burst — sixty-four clients each sending
hundred-document batches — the node's high-water mark was 546–576 MB with
mimalloc against 273–300 MB with glibc and 125–147 MB with musl. That is
retained heap rather than live data, since musl's own figure bounds what was in
use, and it is not the purge timer: `MIMALLOC_PURGE_DELAY=0` gave 608 MB. Where
it goes was not chased further — mimalloc keeps a heap per thread and hands
memory back in whole segments, and the burst has sixty-four requests in flight
across every worker — but `docs/operations.md`'s resident-memory row now
carries the figure, because a container sized tightly to the old peak wants
headroom. The SBOM gains two crates, both MIT, a license already on
`deny.toml`'s allowlist. And a benchmark page that quoted glibc numbers for a
musl binary from 0.1.0 to 0.19.0 is a cost already paid; the section this ADR
adds records the conditions under which every future number is taken.

---

## ADR-118 — The Linux release builds start from the cache `main` writes

**Decision.** The two Linux entries of dist's `build-local-artifacts` restore,
just before `dist build`, the rust-cache that CI saves from `main` for the same
target — and never save one of their own. Three pieces make that true. The
`build kimmyd` job in `ci.yml`, which every downstream job already took its
binary from, now compiles `x86_64-unknown-linux-musl` the way dist does — the
`dist` profile, `--workspace`, dist's musl RUSTFLAGS, dist's runner, the
current stable — and saves under `shared-key:
release-x86_64-unknown-linux-musl`. A new `build kimmyd (arm64)` job on
`ubuntu-24.04-arm` does the same for `aarch64-unknown-linux-musl`, produces no
artifact, and builds only when the cache has no entry under its exact key. And
`github-build-setup` in `dist-workspace.toml` points dist at
`.github/release-build-setup.yml`, two steps it inlines into the generated
job: the same toolchain action CI uses, then a `Swatinem/rust-cache@v2` with
`shared-key: release-${{ join(matrix.targets, '-') }}` and `save-if: false`.
`release.yml` stays generated; dist's `cache-builds` stays off; targets,
installers, publish jobs, the prebuilt-image flow and the SBOMs are untouched.
This amends ADR-107, which evaluated `cache-builds` and left it off because no
job on `main` built the release targets — a job does now, for the two Linux
ones — and ADR-114, which recorded what a release costs on Actions.

**Why.** Measured on the v0.17.0 release: `build-local-artifacts` took 9 m 24 s
for `x86_64-unknown-linux-musl`, about 6.5 minutes for the arm64 target and
about 5.5 for macOS, in a release of about eleven minutes in which everything
else finished inside two. Every tag started from a cold cargo cache, and
ADR-107 said why: a GitHub Actions cache is readable only from the ref that
wrote it or from the default branch, and nothing on `main` compiled a musl
target, so there was nothing a tag could read and no point writing what no
later tag could read either. That analysis stands. What it left open was the
other side of it — make `main` build what the tag builds — and the x86_64 half
of that was almost free, because a job on `main` already compiled `kimmyd` in
release for the Python, Go, conformance and docker jobs; it only compiled the
wrong target. Pointing that job at the release target costs nothing it was not
already paying, and its output is a better hand-off than before: the binary
those jobs test and put in the image is now the static musl file a release
ships, so the docker smoke test runs against the released kind of binary.

**What has to stay aligned.** rust-cache's key is
`v0-rust-<shared-key>-<os>-<arch>-<envhash>-<lockhash>`, and cargo has its own
fingerprints beneath it, so a warm start needs both to match. Each input, and
where it is held equal:

- *`shared-key`.* Set by hand in `ci.yml` and derived from `matrix.targets` in
  the setup file, so the release side cannot spell a target differently from
  the target list.
- *OS and architecture.* `Linux-x64` from `ubuntu-24.04` on both sides — the CI
  job names dist's runner rather than `ubuntu-latest` so the two cannot drift
  when the alias moves — and `Linux-arm64` from `ubuntu-24.04-arm` on both.
- *The environment hash.* It covers the rustc release, host and commit hash,
  and every `CARGO*`, `CC*`, `CFLAGS*`, `CXX*`, `CMAKE*` and `RUST*` variable
  the rust-cache step can see. Both sides install Rust with the same
  `dtolnay/rust-toolchain@stable` step, which gives the same stable at the
  same moment (a bare dist job would use whatever rustc the runner image
  shipped, which lags) and exports the same `CARGO_HOME`, `CARGO_INCREMENTAL=0`
  and `CARGO_TERM_COLOR=always`. `ci.yml`'s `RUSTFLAGS: -D warnings` and
  `CARGO_PROFILE_TEST_DEBUG: 0` moved from the workflow level to the four
  gate jobs, because the release job has neither; and the musl RUSTFLAGS are
  set on the cargo step alone, which is where dist sets them — on cargo's
  process, not the job — so neither rust-cache step sees a `RUSTFLAGS` at all.
- *The lockfile hash.* Over `Cargo.toml`, `Cargo.lock` and the toolchain and
  cargo config files. A tag is pushed at the commit `main` last built (the
  release pull request merges, `main` builds, the tag follows), so the two are
  equal. When they are not, rust-cache falls back to the newest entry of the
  family — the previous lockfile's set — and cargo reuses every dependency the
  change did not touch.
- *Cargo's fingerprints.* Same profile (`--profile dist`; `[profile.dist]` in
  `Cargo.toml` is what makes dist use it), same target directory
  (`target/<triple>/dist` under the checkout — rust-cache recognises the
  per-triple directory as a nested target and keeps its `deps`), same package
  selection (`--workspace`, dist's default, so every dependency is compiled
  once with the workspace's unioned features), same RUSTFLAGS — for a musl
  target dist 0.32.0 appends `-Ctarget-feature=+crt-static
  -Clink-self-contained=yes` to whatever the environment holds, which here is
  nothing — and the same `CARGO_INCREMENTAL`.

With those equal the tag's key is `main`'s key. When something is not equal
the cost is what every tag paid before, a cold build, and never a failure:
rust-cache restores nothing or restores a set cargo mostly rebuilds, and the
release proceeds. The case that will happen: a stable Rust released between
`main`'s last build and the tag changes the environment hash, and the fallback
cannot bridge that, so that one release builds cold; the next push to `main`
writes the new key — the arm64 job builds because its lookup misses — and the
tag after it is warm again. The macOS build has no cache at all, since no job
on `main` builds that target, and runs cold as it always has; with the Linux
builds warm it becomes the phase's long pole, at about five and a half minutes.

**The budget.** Measured locally on a `dist`-profile build of this workspace:
`deps` is 725 MiB on disk, of which about 100 MiB is the workspace's own
crates, which rust-cache drops before saving; the remainder with `.fingerprint`
and `build` compresses to about 207 MiB under zstd, which is what the cache
uses, and the registry index and `.crate` files add at most another 100 MiB.
Call it 300 MiB per family, nearer 350 for x86_64 where the conformance
driver's dev-dependencies add a few feature variants — against the roughly
1 GiB each of the existing debug-profile families. Two families, so 0.6 to
0.7 GiB of a 10 GiB limit. One of them replaces rather than adds: the `build`
job's old native family is written by nothing now and ages out under the
seven-day rule, or can be deleted by hand. The keys,
`v0-rust-release-<target>-Linux-<arch>-<envhash>-<lockhash>`, end in the lock
hash like every other rust-cache key, so `cache-cleanup.yml`'s family rule —
strip the trailing segment — groups them as it groups the rest and keeps one
entry per family. A new rustc opens a new family under a new environment hash;
the old one is read by nothing and ages out.

**Alternatives.** `cache-builds = true` in dist: it is a rust-cache in the same
place, keyed on the generated job's name, so it would look for
`v0-rust-build-local-artifacts-…`, find nothing, and save a set per tag that no
later tag could read, exactly as ADR-107 said. sccache with the GitHub cache
backend: it lives under the same visibility rule, so it too can only be fed
from `main`, and it adds a compiler wrapper and a second cache format for the
same effect the artifact cache gives directly. Building the release targets on
every pull request, so a tag could read a fresher cache: every PR pays two
musl builds so that a tag, which comes after a merge to `main` anyway, saves
nothing more than it already does — the caches are readable from `main`, and
`main` is where they are written. Gating the arm64 job on a diff of
`Cargo.lock` between the push's `before` and `after`: it says nothing about a
toolchain change or an evicted entry, both of which leave the tag cold until
the next dependency change, whereas the exact-key lookup is the condition
rust-cache itself uses to decide whether saving would write anything. Keeping
`-D warnings` and giving it to the release build too, to avoid moving the
variable: it changes no byte of the binary and gives a release one more way to
fail — a warning a newer stable introduces — which is the wrong trade.

**Cost.** The `build kimmyd` job installs `musl-tools` and a target before it
starts, a few seconds, and what it compiles is what it compiled before under
another name (`[profile.dist]` inherits `release` unchanged). The arm64 job is
new: a checkout and a restore per push to `main`, about a minute when the key
exists; a full build, six to seven minutes, when it does not — and the release
pull request's version bump changes `Cargo.lock`, so that is at least once per
release, which is the point. Four jobs now carry two environment lines that
the workflow carried once. The setup file is a second place where release
steps are written; dist copies it into `release.yml`, so the `plan` job's drift
check covers it, and the comment at the top of the file says so. And the
alignment above is silent when it breaks — a cold release, no red job — so the
place to look after a tag is the `build-local-artifacts` log for the Linux
targets, where rust-cache prints the key it restored, and the phase's time.

---

## ADR-119 — A replica applies a peer batch in one transaction

**Decision.** `Engine::apply_batch` applies a batch of replicated entries in
**runs**: every maximal sequence of consecutive document entries goes into one
write transaction, opened through `Engine::begin_write` on the first document
and committed once. A DDL entry ends the run — the run commits, the schema
change goes through `apply_ddl` as before, and the next document entry opens
the next run. The batch's witnessed vector, and for `apply_peer_batch` the
coverage vector `coverage_after_batch` computes, are raised inside the last
run's transaction rather than in transactions of their own. So a batch with no
schema change in it is exactly one commit and, under `durable`, one fsync.
`apply_remote` is split into `apply_remote_in_txn`, which writes into a caller's
transaction and writes nothing when the entry loses, and `report_remote_write`,
which does what has to follow the commit: counting and recording unique
violations, and returning the entries to publish. The public `apply_remote` is
the two around a transaction of its own, for snapshot restore and for tests.
Defended by `a_replicated_batch_is_one_commit_however_many_entries_it_holds`.

**Why.** Measured on a three-member cluster running 0.19.1. A client bulk
insert of 1,000 documents is one commit on the node that accepts it —
`insert_in_txn`, pinned by
`a_batch_is_one_commit_however_many_documents_it_holds` — and was about 1,000
commits on every node that replicated it:
`apply_batch` called `apply_one` per entry, `apply_one` called `apply_remote`,
and `apply_remote` opened and committed its own transaction. `apply_ddl`
committed once more for the originating entry it appends, `absorb_witnessed`
once for the batch's witnessed vector, and `apply_peer_batch` once again for
the coverage vector: a DDL-free batch of N entries cost about N + 2 commits.
Under `durable` every commit is an fsync, so `kimmy_commits` and
`kimmy_fsyncs` rose one for one per replicated document, a 1,024-entry sync
batch took about 145 s to apply, and replication ran at 8–13 documents a
second — 1,000 documents converged in 78.7 s, 4,000 in 492.6 s, 500 in 61 s.
The writer's side had been fixed for exactly this reason; the replica's side
had never been measured, and nothing counted it, because `kimmy_commits` is
read against `kimmy_requests_total` and a replica's commits answer no request.

**What a run holds fixed.** Every per-entry check is unchanged and still runs
per entry, in stamp order: a peer's `UniqueViolation` is refused, the drop
tombstone and the incarnation floor supersede what predates them, an unknown
collection is counted. Those checks read collection metadata through read
transactions, which see the state before the open run — which is safe,
because a run contains no schema change by construction. The one piece of
metadata a document write does touch, an index's multikey flag, is re-read
through the write transaction by `index::maintain_remote` and
`index::mark_multikey` themselves, so an entry that flips it is seen by the
next entry in the same run. A losing entry writes nothing and the run carries
on; it is still witnessed (ADR-054). The unique-violation entry is minted after
the run commits, in its own transaction, as it was — the merge must not fail
because the report did (ADR-029) — and the run is published once, after its
commit, in entry order, so nothing reaches a change stream before it is
durable.

**Why DDL ends a run.** `apply_ddl` reaches the collection, index and vector
`_inner` functions, each of which opens and commits transactions of its own,
and redb has one writer: the run has to be committed before any of them can
begin. Threading a transaction through every `_inner` would make this the
change that touches every DDL path in the engine for the sake of a case that
is rare in a batch — schema changes are a handful of entries in a log of
documents — and would give up the property that a replicated schema change is
applied exactly as a local one is. Splitting at the DDL keeps the DDL path
untouched and costs one extra commit per schema change in a batch.

**Alternatives.** *Leave the per-entry transaction and rely on `coalesced`.*
It shares the fsync but not the commit: redb still serialises N transactions
through its single writer, each waiting at the barrier, and the default class
is `durable`. *Open one transaction for the whole batch and apply DDL inside
it.* Rejected above. *Commit per run and per entry for the witnessed vector as
well.* The vector is derived bookkeeping and rides in whatever transaction
commits last; there is no reason it should ever be a commit of its own when a
run is open. *Batch the coverage vector separately in `apply_peer_batch`, as
before.* One more commit per round for nothing; `coverage_after_batch` is pure,
so folding it into the last run is free.

**Cost.** A run holds redb's single writer for as long as its entries take to
apply — up to a full batch of 1,024 entries — where before it was released
between entries. A local write on a replica waits for the run rather than for
one entry; at the measured rate that is milliseconds of CPU rather than the
seconds of fsync it used to wait behind, but it is a longer *single* wait,
and `kimmy_runtime_stall_seconds` is where it would show. A run that fails to
commit loses the whole run rather than one entry; the round fails as it did
before, the next round re-delivers, and re-delivery is idempotent. On a
replica `kimmy_commits` and `kimmy_fsyncs` now rise per batch rather than per
document, so any dashboard that read a replica's commit rate as its document
rate reads a much smaller number; `kimmy_replication_lag_seconds` and the
`cluster.sync` span's `applied` are the document-rate figures. A run publishes
its entries to change streams in one burst after its commit rather than one
at a time as each committed, and the live feed's ring holds 1,024 events —
the same as `MAX_BATCH` — so a full batch that also mints a `UniqueViolation`
entry can overrun a subscriber that is not keeping up in one go. That is lag,
not loss: a subscriber that falls behind the ring resumes from the oplog,
which is what `a_lagging_consumer_recovers_from_the_oplog_without_losing_events`
defends, and the entries are durable before any of them is published. A run
also holds a second copy of each applied entry until it commits — the
`Pending` list carries the entry and the document id, with the collection
metadata shared through the batch's memo rather than copied — bounded by the
batch size, 1,024 entries. Snapshot restore still applies one document per
transaction; it is a one-time path and is left as it is.

---

## ADR-120 — JSON object key order is preserved through the HTTP and MCP boundary

**Decision.** The workspace's `serde_json` is built with `preserve_order`, so
`serde_json::Map` keeps insertion order — the order the keys arrived in the
bytes — instead of sorting them. Every JSON object that crosses into the
server (a request body over HTTP, a tool argument over MCP, which shares the
same `exec` layer and the same `Value` type) becomes a BSON document with the
same key order, and every document that leaves is rendered in the order BSON
holds it. Two places that had been shaping output in an order of their own,
invisibly, are brought into line with it: an inclusion projection answers in
the document's field order rather than the projection's, and every write path
stores `_id` first. Nothing else changes: the parsers in `kimmy-query` already
iterated their documents in order, and the storage layer already kept it.

**Why.** Found on 0.19.1 over HTTP. `docs/query-language.md` and
`docs/deviations.md` say that two update operators writing the same path apply
in the order written and the last one wins, and `update::parse_with_filters`
does exactly that; its tests, built with `doc!`, pass. Over the wire the order
was ignored: `{"$set": {"a": 1}, "$inc": {"a": 5}}` on `a: 0` gave `1`,
`{"$min": {"a": 3}, "$max": {"a": 10}}` on `a: 5` gave `3`, and `{"$set":
{"a": 7}, "$mul": {"a": 10}}` gave `7` — six operator pairs, both orders each,
always the alphabetical result. The cause was the boundary, not the parser.
Every request body is deserialised as `serde_json::Value` and converted with
`json::json_to_bson`, and without the `preserve_order` feature `serde_json::Map`
is a `BTreeMap`: the keys of every object were sorted before the query language
saw them. `cargo tree -e features -i serde_json` confirmed nothing in the graph
enabled the feature. The same sort was silently applied to every stored
document's fields, to the keys of every filter, projection and sort document —
`{"sort": {"b": 1, "a": 1}}` sorted by `a` first — and, on the way out, to the
fields of every document rendered back. MongoDB preserves field order in all
of those places; BSON is an ordered sequence of elements, and its document
comparison and equality are field-order sensitive because of it.

The fix belongs at the boundary rather than in the parsers because the parsers
were right: they read a `bson::Document`, which is ordered, and the tests that
pin the ordered semantics were passing against it. Teaching each parser to
sort would have been the wrong direction — it would have made the wire order
irrelevant everywhere and contradicted MongoDB — and there is no way to
recover an order from a map that has already forgotten it. The only place the
order was lost is the `serde_json` map, and the feature flag is the whole of
that fix. It is set once in the workspace `Cargo.toml` so that every crate
that touches a `serde_json::Value` — the API, the MCP server, the CLI, the
client library, the fuzz harness — sees the same map type; features are
unified per build, so a crate could not opt out even if one wanted to.
`preserve_order` pulls in `indexmap`, pure Rust, already in the graph through
other dependencies.

The two follow-on changes exist because the sorted map had been hiding them.
`shape::project` built an inclusion projection's output by walking the
specification's paths, with the implicit `_id` appended last by the parser,
so `{"alpha": 1, "zeta": 1}` over `{_id, zeta, alpha}` produced `{alpha,
zeta, _id}`; MongoDB answers in document order with `_id` first, and once
order is visible the two disagree. The picking is unchanged and the result is
reordered to the source document's order afterwards, at every level the
projection reached into, so the array and dotted-path rules stay as they
were; `find`, `findAndModify` and the `$project` stage all go through it. And
`replace_if` appended `_id` to a body that left it out, while the insert path
put a generated `_id` first and a client-supplied one wherever it was; both
now go through one helper that stores `_id` first whatever the body said,
which is MongoDB's rule too, and keeps the client's value as written so an
`Int32` id does not come back an `Int64`.

**Alternatives.** *Deserialise request bodies straight into `bson::Document`
through `bson`'s own serde support and skip `serde_json::Value`.* That
preserves order too, but it discards the Extended JSON layer in `json.rs` —
`{"$oid": …}`, `{"$date": …}`, whole numbers as `Int32` — which is a contract
`docs/http-api.md` makes and the fuzz harness checks; rebuilding it on the
BSON side is the same code in a second place. *Sort every document
canonically on write, and document that field order is not preserved.* That
is a deviation from MongoDB with no benefit: a client that writes `{"zeta":
1, "alpha": 2}` and reads back `{"alpha": 2, "zeta": 1}` has been told
something it did not say, and the update-operator promise cannot be kept at
all under it. *Leave projections in specification order.* That is an order a
client could in principle want, but it is not MongoDB's, it contradicts the
promise `docs/compatibility.md` now makes about stored order, and `_id`
landing last was never a choice anyone made.

**Consequence.**

- *Update operators apply in wire order.* `{"$set": {"a": 1}, "$inc": {"a":
  5}}` on `a: 0` leaves `6`; the reverse order leaves `1`. A client whose JSON
  encoder does not preserve insertion order gets whichever order its encoder
  emitted; `docs/deviations.md` says so and tells such a caller to serialise
  deliberately. The deviation itself — that the pair is not refused as
  MongoDB refuses it — stands.
- *Sort documents mean what they say.* `{"b": 1, "a": 1}` sorts by `b` first.
  Before, it sorted by `a` first; a client that wrote a multi-key sort in
  non-alphabetical order was being answered in a different order than it
  asked for, and now is not.
- *Stored documents keep the order they were written in, `_id` first.* A
  document written before this release was stored with its fields sorted
  alphabetically and stays that way; one written after keeps the order it
  arrived in. Nothing rewrites the old ones, and a collection may hold both.
  Field order is not visible to a path lookup, so `find`, `count`, projection
  and every operator that addresses a field by name behave the same over both.
- *Whole-document comparison is field-order sensitive, as in BSON.*
  `canonical_cmp` compares documents element by element and has since M1, so
  `{"a": {"x": 1, "y": 2}}` as a filter matches a stored `a` of `{x: 1, y:
  2}` and not `{y: 2, x: 1}` — which is MongoDB's rule. Before this release
  both sides of that comparison had been sorted, so the comparison was
  order-insensitive by accident. The one visible change: a document whose
  embedded field was stored before this release is in sorted order, and a
  filter that spells the embedded document in another order no longer matches
  it. The same holds for `$in` over documents, `$eq` inside `$expr`, and the
  order two document-valued keys take in an index or a sort. Rewriting such a
  document — a replace, or any update — stores the order the request carried.
- *Responses render in stored order.* Every `documents` array, `explain` and
  the MCP tool text now show fields as stored, and an envelope built with
  `json!` renders in the order the source writes it. `describe` is the
  exception and deliberately so: its field list is a `BTreeMap` keyed by path
  and stays alphabetical, which is the right order for a summary; only the
  sample documents it returns changed. The audit that went with the change
  ran the whole suite under the new map: one test had asserted the key order
  of a `hybrid_search` match — `_id`, `chunk`, `score`, `text` — as the
  "response shape", and that order was the sorted map's, not a choice; it
  compares the key set now. Every other test compares `serde_json::Value`s,
  whose equality ignores order, and none compares rendered text or a golden
  JSON file; `/metrics` is Prometheus text and untouched.
- *`serde_json::Map::remove` is now a swap-remove.* Under `preserve_order`
  the map is an `IndexMap` and `remove` moves the last entry into the hole,
  which disturbs the order of everything after it. Production code must not
  use it to reshape a response or a document — `bson::Document::remove` is
  the order-preserving one, and reshaping belongs on the BSON side anyway.
  Today only tests call it, on values whose order they do not then assert.

**Cost.** A behaviour change in a `0.MINOR`, recorded in the changelog: a
client that relied on the accidental alphabetical application of update
operators — writing `$inc` and `$set` on one path and getting `$set`'s value
regardless of order — gets the order it wrote now, and a client that read
projected fields positionally, in specification order, reads them in document
order. `indexmap` replaces `BTreeMap` behind every `serde_json::Map`; lookups
are hashed rather than tree-walked and iteration is a vector scan, which is
not slower on any path this server takes. An inclusion projection pays one
pass over the source document's top-level keys per result to reorder, and
`shift_remove` on the picked document, which is small. Two orders of stored
document now coexist on a node that was upgraded, and the comparison case
above is the only place that shows.

---

## ADR-121 — A request body with a field the route does not define is refused

**Decision.** Every request shape the API deserializes carries
`#[serde(deny_unknown_fields)]`: the bodies of `find`, `count`, `aggregate`,
`update`, `delete`, `find_and_modify`, index creation, collection creation,
login, the user, role and webhook routes, vector upload and both searches, and
the shapes nested inside them — an index's field entry, a search's fusion
weights. A field the route does not define is refused `422` with the
`bad_request` code in the standard envelope, and the message is serde's, which
names the field and lists the ones the route takes. The rule is stated once,
on `JsonBody<T>`, the extractor every request body passes through. A document
body — insert, replace, bulk — is content, not a shape, and is not held to it.
A grant inside a user or role body arrives as `GrantInput`, a closed mirror of
`kimmy-auth`'s `Grant` that converts into it, so the persisted type stays open
and the request does not. Query strings are held to the same rule through
`QueryParams<T>`, a wrapper over axum's `Query<T>` whose rejection is the
envelope: `?limt=5` is refused by name and `?limit=abc` is no longer bare
text. The status there is `400`, not `422` — a query string is part of the
request line, not an entity the server failed to process, and `400` is what
axum already answered — so nothing a client had learned to expect on a `GET`
changes but the envelope.

The MCP tools' argument structs carry the same attribute: rmcp deserializes
them with serde and returns a failure as the tool's error result with the text
intact, so the refusal reaches the model by name, and schemars turns the
attribute into `additionalProperties: false` in every tool's schema.
`docs/openapi.yaml` says the same of every request schema, and the contract
test that keeps response schemas open now also insists that request shapes are
closed.

**Why.** `POST .../find` with `{"limitt": 5}` answered `200` and returned the
default page. `{"explain": true}` on `aggregate` answered `200` without a
plan. A misspelt `if_stamp` on `update` made a conditional write an
unconditional one, which is the opposite of what the field is for. Each of
these is a typo on exactly the field where a silent no-op costs the most, and
none of them could be seen from the response. One route refused the same
thing: `POST .../vector`, because `VectorConfig` and `ProviderConfig` in
`kimmy-core` carried the attribute since they were written, so a client met a
`422` for an unknown field on one route and a `200` on every other. (That was
true of every provider kind but one: `ProviderConfig::Byo` was a unit variant,
which `deny_unknown_fields` does not reach, and stayed open until ADR-134 gave
it an empty struct body.) The reference documented neither. The compatibility
page described the refusal as the behaviour a client should expect from an
older node — "several request bodies reject unknown fields deliberately" —
which was true of two structs. Making it true of all of them is making the
documented contract the actual one.

The argument for refusing is the same one ADR-057 made for the error envelope:
a client can only act on what it can see. A field the server does not read is
a request the server cannot honour; answering as though it had is the one
failure a client cannot detect, test for, or retry. A refusal that names the
field is a fix in one edit. And the cost of refusing falls only on a request
that was already wrong.

**Alternatives.**

- *Warn, or accept and report the ignored fields in the response.* Rejected.
  A warning field is one more thing a client must know to read, and the
  clients most likely to send a typo are the ones least likely to read it.
  The pre-1.0 policy allows a tightening behind a `0.MINOR` bump with a
  release note, which this is; a deprecation window would be a release of
  silent no-ops nobody asked for, and the project does not ship
  compatibility shims (ADR-058).
- *Leaving query strings open.* Rejected, though it was the first draft.
  Axum's `Query<T>` rejects as bare text outside the envelope, so closing the
  structs alone would have made an unknown parameter the one refusal a client
  cannot branch on — the defect `JsonBody` was written to remove. The fix was
  the same one: an extractor of our own with the envelope as its rejection,
  after which the rule costs nothing to apply. Answering `422` there, to match
  bodies, was considered and dropped: it would change a status clients had
  seen since the routes existed for no gain, and the envelope is what a
  client branches on.
- *Closing `Grant` itself.* Rejected. A grant is a request shape inside the
  user and role bodies, but it is also the persisted, replicated form and the
  shape every response that lists grants returns; a stored record must keep
  reading under a later version that adds a field, and a rolling upgrade
  cannot afford an older member refusing what a newer one wrote. But leaving
  the request side open was the worst case of the whole finding: `collection`
  defaults to `*`, so a misspelt `colection` did not lose a field, it granted
  every collection in the database and returned a grant that looked
  deliberate. A request-only mirror in `kimmy-api` closes the boundary
  without touching the type.
- *Closing `VectorConfig` in the specification.* Not done, for the mirror
  reason: `GET .../vector` and `describe` return it, and a response schema
  must stay open so a new response field is additive. The server refuses an
  unknown field in it — it always did — and the request body says so in
  prose.

**Cost.** Breaking for a client that sends a field or a query parameter the
route does not define, and a `0.MINOR` bump for it. One more shape to keep in
step with `Grant` — three fields and a `From` — and a second extractor beside
`JsonBody`. The first-party Rust, Python and Go clients, the CLI, the MCP
server, the conformance scenarios, the examples and every request example in
the documentation were audited against the server's field lists and send
nothing the server does not define; a third-party client that hand-rolls a
body with an extra field meets a `422` that names it. The MCP `hybrid_search`
arguments spell out the search fields rather than flattening
`vector_search`'s, because serde cannot refuse unknown fields across a
flatten; the schema the model reads is the same. Two more contract tests,
`every_request_shape_is_closed` and
`every_operation_with_a_query_parameter_documents_the_400`, and the
closed-schema test now distinguishes a component only a request reaches from
one a response does.

---

## ADR-122 — The replication lag gauge measures age behind, not the span of missing history

**Decision.** `kimmy_replication_lag_seconds` is how far behind in time this
node is: for every origin where a peer's head is ahead of what this node has
applied, the time since the newest entry this node *has* applied from that
origin was written, `now − held`; the maximum over origins and over the peers
reached in the round. `lag_behind_ms` in `crates/kimmy-storage/src/sync.rs`
takes the current wall time and computes exactly that; the transport passes
the same physical clock the engine stamps with. Zero when no peer holds
anything newer; an origin this node has never seen contributes nothing, as
before. The stale-rejoiner verdict (`behind_ms`, `lag_beyond_horizon_ms`,
ADR-085 and ADR-097) is a different question and is not touched. The `HELP`
text, the golden `/metrics` tests, the OpenTelemetry description and the
operations table all say the new thing.

**Why.** On a three-member cluster running 0.19.1, the gauge read 0.0 on
every member for the whole of a 78-second window in which one replica held
351 of the 1,000 documents a peer had, and again for a later 492-second window
in which it trailed by 4,000. It only rose — to 697 s, then 1,496 s — once
several writers had spread their writes over many minutes. The formula was
`theirs.wall_ms − held.wall_ms` per origin: the span of origin wall-clock
time between the newest entry this node had applied and the peer's newest. A
bulk insert of 1,000 documents mints all its stamps within a few hundred
milliseconds, so a 900-document backlog of it is "0.3 s of history", and the
gauge read 0 however many minutes the replica took to drain it. The transport
also measures against the peer's vector as of the round's start, which makes
the number a floor; that is not the cause and is unchanged.

**Alternatives.** Two measures were on the table, and each answers a real
question. *The span of missing history*, `theirs − held`: how wide is the
window of the origin's timeline this node still lacks. It is what the gauge
was, it is exact from the two head vectors alone, and it does not move
between rounds. But its width says nothing about how long the node has been
lacking it, which is what an operator alerting on "replication lag" wants to
know, and for the write pattern that produces a backlog in the first place —
a burst — it is a fraction of a second by construction. *Age behind*,
`now − held`: how long ago the newest thing this node has from the origin was
written, given a peer has newer. At thirty seconds into a bulk backlog it
reads about 30 s where the span read 0; caught up it reads 0; holding
everything but an entry written two seconds ago that has not been pulled
yet, it reads about 2 s, which is the truth. Chosen. *Age of the oldest
unapplied entry* would be more precise still, but the peer advertises only
its head per origin, and carrying an oldest-unapplied stamp per origin per
peer is a wire change for a number the age-behind measure approximates from
above; not done. *Keep the span and add a second gauge for age* was rejected
because nobody was found who asks the span's question, and two gauges called
lag invite alerting on the wrong one.

**Cost.** The number changes meaning, and anyone alerting on it
should know: it now grows with the clock while a backlog drains, so an alert
threshold that used to be reached only by long-spread writes is reached by
any backlog that outlives the threshold, which is the behaviour the threshold
was written for. A known limit, stated plainly: an origin that is quiet for
hours and then writes once shows the length of that silence for one round on
each peer, until the entry is pulled. The span had the same one-round spike —
ADR-097 records it being mistaken for a stale rejoiner — so this is not a
regression, and the head vector carries no oldest-unapplied stamp that would
remove it; the doc comment on `lag_behind_ms` says so. The measure now reads
the wall clock, which the old one did not; it is the same clock the engine
stamps with, and a clock stepped backwards saturates to zero rather than
wrapping. It also crosses clocks, which the span never did: `held` is a
remote origin's HLC wall time and `now` is this node's, so a peer whose clock
runs ahead of this node's makes an origin this node genuinely trails saturate
to zero and the gauge under-reports by the skew, and one running behind adds
it. The error is bounded by the skew members already tolerate in their HLCs,
and under-reporting by seconds is a smaller lie than reading zero through a
backlog that lasts minutes. Every unit test that called `lag_behind_ms` passes a time and
asserts the new meaning, and one reproduces the finding: stamps spanning
300 ms, held at the first, the peer at the last, thirty seconds on — 30,000
where the span gave 300.

---

## ADR-123 — A dropped index leaves a tombstone, and a schema change a replica cannot apply is skipped, counted and exported

> **Amended by [ADR-141](#adr-141--a-drop-mints-its-entry-wherever-it-lands-and-a-declined-drop-is-counted).**
> The cost paragraph's last sentence — a local drop of an index that is not
> there records nothing — is withdrawn: such a drop now mints its entry and
> records its tombstone under a fresh stamp, so it reaches the members that
> hold the index. The reason given, that a tombstone no peer hears of would
> leave this node refusing a definition every other member accepts, does not
> survive the entry being minted.
>
> **Amended by [ADR-139](#adr-139--a-document-an-index-cannot-key-is-stored-and-filed-unkeyed-not-refused).**
> The refusal class below no longer holds a definition this node's
> *documents* cannot be built under: such a definition is now built, with
> those documents filed unkeyed under it, on every path — the replicated
> create, the snapshot page, and the write that arrives after the definition.
> What the class keeps is a definition this build cannot apply and a rival it
> cannot arbitrate. "Why a two-array compound index is not refused at
> creation" below is still the reasoning, and its conclusion — the check runs
> where the pair meets — now ends in a filing rather than a refusal.
>
> **Amended by [ADR-132](#adr-132--an-index-carries-the-stamp-of-its-creation-and-a-drop-and-a-rival-are-both-settled-by-it).**
> Not superseded: two claims below are withdrawn, and each only where a
> creation stamp is there to decide it. Where **both** definitions carry one,
> two members that create one index name with different definitions now settle
> on the later stamp instead of each keeping its own, and
> `kimmy_sync_ddl_refused_total` does not rise for that case. And a replayed
> drop older than the index standing under its name now records its tombstone
> and leaves that index alone, rather than removing one nobody dropped. Where
> a definition carries **no** stamp this record still holds exactly as
> written — but an index is not fixed that way: one holding no stamp adopts
> the stamp of a peer holding the same definition *with* one, on the next
> round and with no operator action. Everything else it decides is unchanged:
> the tombstone, the refusal class and its skip-and-count behaviour, the rule
> that every other error fails the round, the snapshot route's classification,
> the replicated unique backfill, and the three counters.

**Decision.** Three things, one finding. *First*, dropping an index records a
tombstone: `indexes_dropped`, keyed by collection id and index id, holding the
drop's originating stamp, kept for `tombstone_retention_secs` beside the
document and collection tombstones, carried by backups under a new tag, and
consulted by `apply_remote_index` — a `CreateIndex` stamped before the
tombstone is history: counted as applied, not built, and **not appended**,
exactly as a `CreateCollection` is against `collections_dropped` (ADR-034),
so this node does not re-serve onward an entry it has decided is history;
the drop's own entry, appended when it applies, is what carries the ordering
to a third member. `drop_index_inner` takes the
originating stamp when replicated and mints one when local, and the same
parameter decides whether to log and which stamp the tombstone records; the
replicated path records the tombstone even when the index is not here to
drop, and even when the collection is gone. *Second*, `apply_ddl` sorts the
result of a replicated `CreateIndex`, `DropIndex` or `ConfigureVectors` into
applied, gone, or **refused**: a refusal is `InvalidQuery`, `IndexExists` or
`Unsupported` — a refusal of the request, decided by this node's data — and
is skipped, logged at warning with the database, collection, index and
reason, counted in `SyncOutcome::ddl_refused`, witnessed but not appended;
every other error still fails the round. The snapshot route applies the same
classification: `restore_collection` runs each index definition on a page
through `settle`, a refused one is warned and counted in
`SnapshotApplied::ddl_refused`, which the transport folds into the round's
`SyncOutcome::ddl_refused`, and the documents on the page still restore. A
snapshot is served to exactly the peer most likely to hold documents a
definition cannot be built under — one that was away long enough to write
on its own past the origin's retention — so leaving that route on a bare
error would have kept the wedge open where it was likeliest. A replicated
unique index whose
backfill finds keys already shared is built in full and its collisions are
recorded as a merged write's are. *Third*, the replication loop reports every
tick through a new `on_round` hook — `RoundReport { failed, backing_off,
ddl_refused }` — and the daemon maps it to `kimmy_sync_failures_total`,
`kimmy_sync_peers_backing_off` and `kimmy_sync_ddl_refused_total`, with
OpenTelemetry observers and rows in the operations table. `on_lag` is
unchanged.

**Why.** Observed on a three-member cluster running 0.20.0. A member created
a compound index over two array fields on a collection in which no document
held both — accepted, `multikey: true` — and dropped it 28 seconds later.
Both replicated. A document with arrays at both paths was then inserted,
which is legal once the index is gone. From then on, every anti-entropy round
that re-served the window holding the `CreateIndex` entry — windows overlap
by design, because `entries_for_peer` serves in global stamp order from a
single threshold — rebuilt the index through `apply_remote_index`, whose
backfill met the two-array document and raised `index_keys_observed`'s
"cannot be built for this document" error. That error is not
`CollectionNotFound`, so `gone()` did not absorb it; `apply_one` propagated
it; `apply_batch_absorbing` abandoned the run and its witnessed vector;
coverage never advanced; and the identical window was re-requested for ever
with backoff to 300 s. The `DropIndex` later in the same window was never
reached. A second instance minutes later: a unique index created and dropped
a second apart, whose replayed create collided on the puller's copy
("existing documents already violate it"). Throughout,
`kimmy_replication_lag_seconds` read 0, because `on_lag` is called only
after a round that succeeded, and `/v1/topology` showed every member live.
Writes on one member never reached the others again.

**Why the tombstone, and why it was needed before retention ran.** ADR-034
gave collections a tombstone because the `DropCollection` entry aged out and
a rejoining peer recreated the collection. An index had the same gap and a
worse consequence: the entry did not need to age out, because overlapping
windows replay the create routinely, and a replayed create is not merely a
resurrection — it *backfills over the current documents*, which may by then
hold exactly what the definition forbids, and the backfill's failure took
the round down with it. The tombstone makes the replay history; the refusal
class makes the round survive it; each is needed without the other. Keyed by
the id derived from the name, so the drop — which carries only the name —
and the create — which carries the definition — compute the same key.
Snapshots carry no collection tombstones today and carry no index
tombstones either; a node restored from a snapshot has the state as of the
snapshot and no history to replay against, and the case where it later
receives an aged-out create from a peer is the case ADR-034 already accepts
for collections. Not extended here.

**Why a refusal is skipped and everything else still fails the round.**
`gone()`'s stance — a round that quietly skips what it cannot understand is
how corruption becomes convergence — stands, and the refusal class is
defined so as not to breach it. A refusal is a deterministic function of the
definition and this node's data: `InvalidQuery` from the backfill (a
compound index a document here spans two arrays of; a document that would
fan out to more than 1,000 entries; the TTL and partial-filter shape checks),
`IndexExists` (the name is taken here by a different definition, because two
members created it concurrently), `Unsupported` (an enforcement mode this
build does not implement). Re-delivering the entry unchanged can never
succeed, so failing the round buys the wedge and nothing else. Skipping is
safe on three conditions, all met: the entry is witnessed by
`apply_batch_absorbing`, so it is not re-served; it is not appended, so this
node does not propagate a definition it does not hold; and the skip is
counted, logged with the reason, and exported, so the divergence is a
visible state rather than a silent one. A storage error — redb, I/O, a record
that will not decode — is a failure of the node rather than of the request,
may succeed on retry, and still fails the round.

**Why a two-array compound index is not refused at creation.** It was
considered and rejected. The rule — "a compound index may span at most one
array field", MongoDB's "cannot index parallel arrays" — is a property of a
(definition, document) pair, and a schemaless store holds no fact about
documents it has not seen. Refusing the definition at creation whenever two
of its paths *could* hold arrays would refuse every compound index. So the
check runs where the pair meets: creating over a collection with no such
document succeeds; a later write of such a document is refused `400`, naming
the index; a create over an existing such document is refused. On a replica
the same check runs over the replica's documents, and a definition the
origin accepted can be one the replica cannot build — which is the refusal
class above, not a reason to move the check. `docs/indexes.md` now says when
the rule bites.

**Why a colliding unique backfill is built rather than refused, on the
replicated path only.** ADR-020: uniqueness is not I-confluent, so a
replicated write that breaks it converges with the violation recorded rather
than diverging by being refused. A replicated *definition* is the same
choice one level up. Refusing it leaves this node without an index every peer
holds, for ever: writes the peers check go unchecked here, and index-backed
queries answer differently on different members — a divergence that is
permanent and, without this ADR's counter, silent. Building it in full,
every entry added, keeps queries complete (the reason `maintain_remote` adds
the colliding entry rather than skipping it), keeps the constraint live for
every later local write, and reports the keys already shared through the
same machinery a merged write uses: counted in `kimmy_unique_violations`,
warned, minted as `UniqueViolation` entries (ADR-029) that
`live_unique_violations` reads (ADR-087), naming every holder of the key.
The record's `merged` field names the holder the backfill met last, which is
the document whose presence turned a key with one holder into a collision —
the role the merged write plays. The local path is unchanged: a client
creating a unique index over data that violates it is told so, because it
is there to be told.

**What is left as a counted divergence.** Two members creating the same
index name with different definitions during a partition. Neither is wrong;
the second to arrive is refused with `IndexExists`, counted, and named in the
log, and each member keeps its own. A "newer definition wins" rule would
resolve it, but needs a creation stamp on `IndexMeta` to compare, which the
definition does not carry and which would change what `CreateIndex` puts on
the wire; not done here. The same gap shows on a replayed *drop* that arrives
after a newer creation of the same name: with no creation stamp to compare,
the drop applies. The tombstone it records is older than the creation, so a
replay of the creation rebuilds it — but only where the creation can be
replayed. A node never replays its own oplog into itself, so when the newer
creation is this node's own, the index stays dropped here until a third
member that holds the creation re-serves it, or until someone recreates it
explicitly; the pair does not converge on its own. Accepted for the same
reason: the fix is the creation stamp, and the case needs a drop from before
a recreation to arrive after it, which is a re-served window across a
recreation on the same name.

**Amended 2026-09-05 — the creation stamp exists, and both halves of this
paragraph are settled by it.** ADR-132 put `created` on `IndexMeta`, which is
the fact this paragraph says the fix needs, and with it the "newer definition
wins" rule declined above: where both definitions carry a stamp, the later one
stands on every member, the loser is rebuilt under it, and nothing is counted.
The same stamp is what a replayed drop is now compared against, so it leaves a
newer index of the same name alone. What is written above holds only where a
definition carries no stamp.

**Why a counter, not a zero lag, is the failure signal.** ADR-122 stands: an
unreachable cluster has *unknown* lag, and `on_lag` is not called for a tick
that reached nobody, because overwriting the last reading with zero reports
the outage as health. That is the right rule for the gauge and it is why the
gauge cannot carry this signal — a round that fails leaves the gauge exactly
where the last good round put it, which for a cluster that was caught up
when it wedged is 0. A counter of failed rounds has no last-good-value
problem: it rises on every failure, from any cause, and a counter rising
while the gauge sits at 0 is precisely the shape of the wedge. `on_round` is
called every tick, reached peers or not, for the same reason `on_lag` is
not.

**Alternatives.** *Widen `gone()` to swallow every error.* Rejected; that
is the convergence-by-corruption `gone()` was written not to be. *Retry the
refused entry on the next round.* It cannot succeed, and retrying it is the
wedge. *Append the originating entry on refusal, so the version vector
advances.* It would propagate through this node a definition this node
does not hold, and the witnessed vector already stops the re-request
(ADR-054). *Have the origin refuse the two-array compound index at
creation.* Rejected above. *Refuse the colliding unique backfill on the
replicated path as on the local one.* Rejected above, by ADR-020's own
argument. *Carry the tombstone in the `DropIndex` entry's lifetime alone.*
That is what was there. *Put the failure signal into the lag gauge as a
sentinel value.* A dashboard cannot alert on "0 means healthy or wedged",
which is the finding.

**Cost.** One more tombstone table, one more backup tag (11; never reused),
one more retention pass over a table that holds one row per dropped index.
`drop_index_inner`'s signature changes from a `log` flag to an
`Option<Stamp>`, as `drop_collection_inner`'s did, and `create_index_inner`
returns the collisions it found beside the definition; the local wrapper
asserts the list is empty. The replicated backfill of a unique index keeps
every key's holders in memory for the length of the build — one document
key per key more than the local build, which keeps its `HashSet` of keys and
pays nothing new. `apply_snapshot_page` returns `SnapshotApplied` rather
than a count. Recording a backfill's violations follows `commit_run`'s
shape — every violation reported, everything minted published, the first
error returned afterwards — because the index is committed by then and a
re-delivery would not find the collisions again. A replica can now hold a unique index over data
that violates it, as it already could through merged writes, and the
violations route says so. A refused definition is a divergence the cluster
does not repair on its own; the counter and the warning make it an
operator's decision rather than an outage. Three new series on `/metrics`,
pinned by the golden tests. `SyncOutcome` and `RoundReport` gain a field
each. A local drop of an index that is not there still records nothing — it
mints no entry, so a tombstone would be a decision no peer hears of.
## ADR-124 — A route that reads no query string refuses every query string

**Decision.** One layer over the REST route table,
`routes::refuse_unread_query_string`, answers `400 bad_request` in the
envelope to any request whose query string is non-empty and whose
`(method, matched route)` is not in `routes::QUERY_STRING_ROUTES` — the
seven operations whose handlers take a `QueryParams<T>`: `GET .../docs`,
`PUT` and `DELETE .../docs/{id}`, `GET .../describe`, `GET .../violations`,
`DELETE .../vector` and `GET .../watch`. The message names the first
parameter, percent-decoded, and says the route takes none, in the shape of
the serde message `QueryParams<T>` refuses an unknown parameter with. A bare
`?`, or a query string made only of separators, names nothing and passes.
The layer is applied inside `routes`, to the merged timed and streaming
tables, so `/mcp` — merged afterwards by `router_with_limits` — is outside
it. It answers **before authentication**: it is a `Router::layer`, and
authentication is the `Auth` extractor a handler takes, so
`POST /v1/users?zz=1` with no token is `400`, not `401`. That is acceptable
because nothing is learned and nothing is touched — the route templates are
public in the specification, the message echoes only the caller's own
input, and no handler runs. The second-order consequence is that a request
the guard refuses never reaches the per-principal budget, which is spent
inside `Auth` (`state.rs`), nor a login-attempt limiter, which is spent
inside the login handler: harmless, since the guard is cheaper than either
and examined no credential, and stated here the way ADR-099 stated where
the deadline sits relative to the work it bounds. A test holds the
placement, so it cannot drift to the other side of authentication without
someone deciding it should. `docs/openapi.yaml` documents the shared `400` on every operation and
states the rule in its preamble beside the body rule, and the contract tests
hold the table equal to the set of operations the specification gives a
query parameter, hold every operation to documenting the `400`, and drive
`?zz=1` at every documented operation over a real socket.

**Why.** ADR-121 closed query strings through `QueryParams<T>`, and its own
motivating example was a misspelt `if_stamp` making a conditional write
unconditional. But an extractor runs only on a handler that takes it, and a
handler with no query parameters never looked at its query string at all —
so the closure reached exactly the seven routes that already read one.
`POST .../update?if_stamp=<stale stamp>` answered `200` and rewrote the
document: `if_stamp` is a body field there, the parameter was never read,
and the write the caller had made conditional was not. `?bogus=1` on
`find`, `count` and `bulk`, and `?multi=true` on `update`, answered `200`
having ignored the parameter, while `GET .../docs?limt=5` was the
documented `400`. The reference said "query strings are held to the same
rule", which was true of a quarter of the routes. Found by a test round
against a three-member cluster running 0.20.0.

The argument is ADR-121's, and it applies with more force here: a parameter
the server does not read is a request the server cannot honour, and the one
it did not read in this case was the condition on a write. A refusal that
names the parameter is a fix in one edit; the `200` was a silent overwrite.

**Alternatives.**

- *A `QueryParams<NoParams>` on every handler that takes no parameters.*
  Rejected, and it is the important rejection. It closes the routes that
  have one today and leaves the next handler open, because forgetting it is
  invisible — the route compiles, answers, and ignores. The two designs
  differ only in what forgetting does: with an extractor per handler a
  forgotten route is open, with a layer over the table and a list of what is
  open a forgotten route is closed, refuses on the first request with a
  parameter, and says why. Fail-closed by construction is the property that
  makes ADR-121 hold without anyone remembering it.
- *A table of routes that refuse a query string, rather than of routes that
  read one.* Rejected for the same reason: a route absent from a deny-list is
  open. The table lists what is opened, so an entry is only right beside a
  `QueryParams<T>` that reads it, and the contract test holds the table
  equal to the operations the specification documents a parameter on — a
  parameter documented on a route the table does not open, or opened and
  documented nowhere, is a failing test rather than a route that quietly
  behaves otherwise.
- *Deciding on the raw path rather than `MatchedPath`.* Rejected; it would
  re-implement routing. The matched route template is what the table holds
  and what axum has already decided, and it is what the request span is
  named from for the same reason.
- *Covering `/mcp` too.* Rejected. MCP is a separate transport with query
  semantics of its own — the streamable HTTP specification, not this API —
  and rmcp reads its requests; the REST rule has no standing there, and
  `docs/openapi.yaml` already leaves it out for the same reason. The guard
  is applied in `routes` rather than in `router_with_limits` so that the
  merge order which puts `/mcp` inside the counter and outside the deadline
  (ADR-099) also puts it outside this.
- *`422`, to match bodies.* Rejected in ADR-121 and not reopened: a query
  string is part of the request line, `400` is what the seven opened routes
  already answer, and one status for "the query string was refused" is worth
  more than symmetry with the body.
- *Exempting the health probes.* Considered and not done. A probe with a
  cache-busting parameter is refused like everything else; an exemption is
  a route the rule does not reach, which is the shape of the defect this
  closes, and a probe that needs a parameter has something to say that this
  server should hear.

**Cost.** Breaking for a client that sends a query parameter to a route
that takes none, and a `0.MINOR` bump under the pre-1.0 policy. The
first-party Rust, Python and Go clients, the CLI, the conformance scenarios,
the examples and every request example in the documentation were audited
and send a query string only to the seven routes that read one, with
parameters those routes define; the MCP server calls the shared `exec`
layer in-process and sends no HTTP at all. A health check configured with a
cache-busting parameter now answers `400` and needs the parameter removed.
One table to keep beside the route table, held by a contract test in both
directions, and one string comparison per request that carries a query
string. Because the layer wraps each route's method router, whose own
fallback is the `405`, a wrong method on a guarded route *with* a query
string answers `400` for the query string rather than `405` for the method;
without one the `405` stands. A parameter with no name, `?=1`, is refused as
malformed rather than named. Twenty-seven operations gained a documented `400` they could not
answer before, and a contract test now insists every operation documents
it.
## ADR-125 — The embedding worker checkpoints its position by deadline, not per entry

**Decision.** The embedding worker no longer writes its oplog position after
every entry it has nothing to do with. The position is held in `Pending`,
beside the batches, with the instant it was first held, and a new
`POSITION_WAIT` of one second bounds how long it may wait. `Pending::deadline`
is the earlier of the batch deadline — unchanged, `opened + max_wait` — and
`held_since + POSITION_WAIT`, and `None` only when there is neither a batch nor
a held position; the timed wait in `drive` and its timeout branch already
flush on that deadline, and `flush` now clears both clocks. The deadline is
also checked after every entry, not only when the timed wait elapses: the
change stream returns without waiting while the arrival index has entries
queued, so during a drain the timer never fires, and a deadline evaluated
only there would hold the position for the whole backlog — a member coming
back to an hour of entries would checkpoint nothing until it had caught up.
Checked per entry, a held position is written at most `POSITION_WAIT` after it
was first held whether the stream is quiet or draining, and a partial batch
whose entries were slow to prepare goes at `max_wait` rather than past it; a
full batch goes the moment it fills, as before. Every
`Prepared::Done` outcome — a skip, a delete, a deferral, a backfill — holds the
token instead of committing it; `Prepared::Embed` records it as before. So the
position is written when a batch flushes, when a held position has waited
`POSITION_WAIT`, at stream end, on invalidation, and before a
`ConfigureVectors` backfill — never per entry. The constant is not a setting.
Defended by
`a_burst_of_writes_the_worker_skips_costs_one_position_write_not_one_each`,
`a_lone_skipped_entrys_position_is_recorded_within_the_position_wait`,
`a_burst_of_skipped_entries_does_not_delay_the_embeddable_document_behind_it`,
`a_held_position_is_checkpointed_during_a_long_drain_not_only_after_it` and
`the_deadline_is_the_earlier_of_the_batch_wait_and_the_position_wait`.

**Why.** Measured on a three-member cluster running 0.20.0. A 1,000-document
bulk insert into a collection with no vector configuration converged on every
member in 3–5 s: ADR-119 works, and the replica applies the batch in one
transaction. But afterwards all three members — the writer included — kept
committing and fsyncing at a steady ~18/s until each had added about 1.2–1.3
commits per replicated document: roughly 1,320 commits over about 75 s per
member for 1,000 documents, and for small bulks a replica paid exactly n + 1.
The replication lag gauge read hundreds of seconds on the replicas while the
trickle ran. The cause was one line in `drive`:
`Prepared::Done(_) if pending.is_empty() => put_consumer_position(...)`. A
document in a collection with no vector configuration is
`Prepared::Done(Outcome::Skipped)`; `pending` is empty whenever nothing is
being embedded; and `put_consumer_position` is `begin_write … commit` — one
counted commit and one fsync per oplog entry. The worker runs on every member
(`worker_enabled: true`, `WatchScope::Cluster`) and consumes the local arrival
index, so it sees the member's own writes and every replicated one alike, and
nothing throttles the stream: ~18/s is simply the single-writer fsync rate.
ADR-119's claim that a replica's commit rate is not its document rate held
only for the sync batch itself. On a cluster with the default worker a
replica's commit rate *was* its document rate — it had moved from `apply_batch`
to the worker, one transaction later, where nothing counted it against any
request. The single-node form of the same cost was already known: the write
gap measured in [Benchmarks](benchmarks.md) (2.00 commits per insert),
pinned by a test whose comment said its passing was not an endorsement, and
reserved as a decision in the roadmap because the oplog-consumer contract is
where this project has had three separate bugs.

**What the corrected cost is.** A replicated batch on a member now costs one
commit per run of document entries (ADR-119) plus one position checkpoint per
`POSITION_WAIT` while entries are arriving, however many arrive — a batch
published in one burst is one checkpoint. A local bulk insert into a
collection with no vector configuration is the same: its one commit plus one
checkpoint. A lone entry's position lands within about a second. A steady
trickle costs at most one checkpoint a second, on top of whatever the
trickle's own commits are, where it cost one per entry. The position still
never runs ahead of an entry whose vectors are not on disk: it is written by
the same flush, after the batches.

**Why one second.** A burst is coalesced by any wait at all; it is the trickle
case that sets the bound, and one checkpoint a second is the amplification
the trickle is allowed. What a longer wait would buy is fewer checkpoints
under a trickle; what it would cost is the window a crash re-processes on
restart, which is at most the held window plus a batch. Re-processing is safe
— embedding is idempotent through `vectors_are_stale`, and skip, delete,
defer and backfill are all re-derived from what is stored rather than from
having seen the entry — but it is not free: it is storage reads, and on a
member that owns a collection it is provider round trips for anything whose
vectors did not land. A second of replay is a handful of either. It is not a
configuration setting because nothing an operator knows would move it: the
trade is between the worker's own restart replay and the worker's own commit
amplification, and a second decides it the same way on every deployment.

**Alternatives.** *Record the position only when there was work to do.*
Rejected in the roadmap before this was measured, and still: a position that
advances only on embedding work is stranded behind retention on a member
whose owned collections are quiet, and the lost-position recovery is a full
rescan. *Fold the position write into the batch flush only, with no deadline
of its own.* The same failure in a different place — a member that embeds
nothing never checkpoints — and a restart replays the whole retained log.
*Write the position under the `coalesced` durability class.* It shares the
fsync but not the commit: redb still serialises each one through its single
writer in front of the next foreground write, and the default class is
`durable`. *Make it a setting.* Rejected above. *Run the worker on the
collection's owner only.* It would remove the replica's share of the cost and
leave the writer's, and a non-owner has to see every entry anyway to defer
it; the deadline removes the cost on every member at once.

**Cost.** A restart re-processes up to a second of stream, plus a batch, that
it used to skip; the replay costs reads, not writes, except where vectors
never landed, where it costs the provider call that was owed anyway. The
position now trails the newest entry by up to a second when the worker is
otherwise idle, which changes what a test may assume: `worker_is_idle`'s
notion of "started" now waits for that first checkpoint, and every test that
measured commits around the worker was re-read for it. `kimmy_commits` on a
member that only replicates is now dominated by the sync batches and one
checkpoint a second under load; a dashboard that had, without knowing it,
been reading the worker's trickle as the document rate reads a much smaller
number, and the document-rate figures remain `kimmy_replication_lag_seconds`
and the `cluster.sync` span's `applied`. The measured benchmark numbers in
[Benchmarks](benchmarks.md) are left as they were taken, with a note that the
second commit is gone.

---

## ADR-126 — A batch's entry cap is spent after the filter, not before it

**Decision.** `Engine::entries_for_peer` counts only the entries it will
actually ship towards the batch limit. The oplog read takes a predicate —
`Engine::read_oplog_from_where`, which `read_oplog_from` is now a thin call
into — and stops when it has `limit` *retained* entries or reaches the end of
the oplog, rather than reading `limit` raw entries and dropping some of them
afterwards. A window truncated at the cap therefore holds `limit` shippable
entries again, and a shorter one really is the end of the peer's oplog.

**The defect.** The cap was spent before the `UniqueViolation` filter ran:

```rust
self.read_oplog_from(from, limit)?          // stops at `limit` entries
    .into_iter()
    .filter(|entry| entry.kind != OpKind::UniqueViolation)   // then drops some
```

So a window truncated at 1,024 could return 1,019, with the peer's tail
nowhere near reached. `coverage_after_batch` read any batch shorter than the
limit as "the peer's whole tail" and absorbed the peer's **entire** version
vector — which is the right answer for a genuine tail and a catastrophe for a
truncated window. Every entry behind that window was then witnessed without
being applied, `VersionVector::behind` reported nothing missing, and nothing
ever re-served them. Both preconditions are ordinary: a member more than one
batch behind, and one collision anywhere inside the window.

**Why nothing caught it.** The behaviour is a blind spot between two
deliberate designs, both of which stay. ADR-029 withholds violation entries
from peers because every node observes the same collision independently.
ADR-082 absorbs the peer's advertised vector — including stamps it holds but
never ships — precisely so a withheld violation cannot pin `behind` at its
floor and re-serve one window for ever. Neither anticipated that a withheld
entry also *shortens the batch*, which is the signal the third piece used to
decide the window was complete. Nothing errored, so ADR-123's counters could
not see it either: `kimmy_sync_failures_total` 0, `kimmy_sync_ddl_refused_total`
unmoved, `kimmy_replication_lag_seconds` 0, and not one log line on the member
that lost the data.

**Observed** on a three-member cluster running 0.21.0, 2026-09-03. Two
collections existed on the member that created them, holding documents, and
answered `404` on both peers forty-five minutes later — the peers did not list
them at all. Both were created while the peers were 130–260 s behind and after
the cluster's first cross-member unique collision, so every truncated window
on it contained one. Documents were lost the same way and are the worse half:
a full `_id` comparison across the three members found one collection of 2,018
documents holding 1,518 on one member and 1,501 on another. The **500
contiguous ids missing from the first are a subset of the 517 missing from the
second** — one bulk insert, accepted on a third member, discarded from the
window remainder by *both* pullers, with the second also missing a separate
17-run. A second collection was missing five documents on each of two members,
four of the five the same on both. Settled and unchanging across three samples
spanning ninety seconds, with every health signal green. The overlap is the
signature: independent losses would not share a run, and two pullers dropping
the same remainder is what one truncated window on the origin produces.

**Cost.** The scan may read past `limit` raw entries to fill the window, so the
work per batch is no longer bounded by `limit` reads at all — it is bounded by
the oplog, and in practice by `limit` plus however many entries the predicate
rejects in that stretch. That is a bound removed rather than widened, and it is
accepted deliberately: the rejected entries are violations, which are rare by
nature and are a cluster with a much louder problem when they are not, and the
scan is a range read that was happening anyway. Serving fewer entries than the
limit while the tail is unreached, the alternative, is what caused this.

**One other caller changed with it.** Webhook delivery reads through
`entries_for_peer` in `dispatch::dispatch_once`, for the same reason a peer
does — it wants the oplog minus this node's own violation entries — and so now
also gets a scan capped on what it keeps. It re-filters that page for the
subscription's collection and operations and re-caps it, so the change is
invisible beyond reading a slightly longer stretch of log to fill the page it
asked for. Nothing was wrong there before: a shorter page was simply a shorter
page, and progress advanced over exactly what it received.

**Cost of the alternative considered: ship violations and have receivers drop
them.** ADR-082 already declined it — it moves ADR-029's exclusion to the
wrong side and doubles the wire cost of every collision — and it would fix
only this filter, leaving the next one to reopen the hole. ADR-127 closes that
door properly.

Defended by `a_withheld_violation_does_not_make_a_truncated_window_look_like_a_tail`,
`a_collection_created_inside_a_truncated_window_reaches_every_member` and the
property test `a_window_that_is_not_a_tail_never_witnesses_past_what_it_delivered`.

---

## ADR-127 — The peer reports where its window ended; the receiver never infers it

**Decision.** `Message::Entries` carries `scanned_to: Hlc` and
`exhausted: bool` beside its entries — the last stamp the sender's scan
examined, and whether it stopped there because the oplog ended rather than
because the batch filled. `coverage_after_batch(theirs, scanned_to, exhausted)`
absorbs `theirs` when the window was exhausted, and otherwise raises each
advertised origin to `min(their_max, scanned_to)`. It no longer sees the
entries or the limit at all. `Engine::entries_for_peer` returns an
`OplogWindow` carrying all three, so the fact travels from the reader that
knows it to the rule that needs it without being reconstructed on the way.

**Why, given ADR-126 already fixes the bug.** The count was never the fact;
it was a *proxy* for "the tail was reached", true only while nothing could
shorten a batch for another reason. ADR-126 makes the proxy honest again
against today's one filter. It does not stop the next one from breaking it —
a second withheld entry kind, a size-based trim, a per-collection ACL, a
redaction rule — and the failure mode when it breaks is silent, permanent
document loss with every health signal green, discovered forty-five minutes
later by hand. The sending side knows exactly where its window ended. Saying
so costs one stamp and one bool per batch, and moves the hazard from "a filter
nobody thought about shortens the batch" to "a sender lies about its window",
which is a thing one line of arithmetic can check.

**And it is checked, because an assertion on the wire is not a fact.** The
window's end used to be something the receiver computed and is now something
the peer states, so `apply_peer_batch` clamps it: a window that is **not**
exhausted claims no more than the last stamp it actually carried, and one that
carried nothing claims nothing at all. A correct sender is unaffected — its
scan stops on the entry it last kept, so the clamp is arithmetic that changes
nothing.

What it forecloses is a sender that trimmed a batch in place while reporting
the end it had scanned to, which would witness away everything it dropped.
Measured on this code before the clamp: three entries served with the oplog's
head as the window's end left the receiver holding 2 documents of 20 with
`behind()` reporting nothing missing, and an **empty** batch served the same
way absorbed the peer's entire vector in exchange for nothing — the same
defect, with a worse exchange rate. `Message::BatchTooLarge`'s doc comment
already told implementers not to trim in place; the clamp is that sentence as
an invariant, and the `Fits::Only` path is exactly the place the temptation
arises. `coverage_after_batch` stays a pure function of what it is told.

**The empty case is clamped rather than exempted**, on the argument that makes
it safe to clamp: `read_oplog_from_where` stops only after keeping an entry, so
a window that is not exhausted holds exactly `limit` entries and a correct
sender **cannot** emit an empty one. The state is therefore always a broken or
hostile peer, and the honest answer to a claim with nothing behind it is to
claim nothing. Exempting it left the clamp covering everything except the case
it was written to defend against.

**What cannot be checked, and is trusted.** `exhausted` itself. Absorbing the
peer's advertised vector on exhaustion *is* ADR-082, and this node holds
nothing to test the claim against — it cannot know how much oplog the peer has.
A peer that reports `exhausted` falsely still absorbs the receiver's view of
it, exactly as before this ADR. That residual is forced by the design rather
than chosen: the alternative is to stop absorbing on exhaustion, which is the
livelock ADR-082 exists to prevent. So the claim this ADR makes is bounded —
the *window's end* is now stated and checked; whether the log ended is stated
and taken on trust.

**Why `scanned_to` counts entries the sender withheld.** The scan has read
past them, and a stamp the receiver can never be sent must not be able to hold
the window open — that is ADR-082's cure, stated at the window's edge instead
of only at its end. It is safe for the same reason ADR-082 gives: the sender
serves contiguously in stamp order from the point asked for, so nothing inside
the window was skipped except entries deliberately withheld, and claiming
those is correct because every node observes a violation independently
(ADR-029). Bounding by the peer's own coverage still means the receiver never
claims history the peer does not hold.

The property is only *visible* when a rejected entry is the last one read —
while the scan stops on a kept entry the two coincide — so it is pinned
directly, in `a_window_ends_at_the_last_entry_the_scan_read_not_the_last_it_kept`,
rather than left to fall out of a sync test. Moving one assignment inside the
predicate's branch used to break nothing in the workspace, which is not a
property this ADR should be asserting.

**A breaking wire change, and no shim.** `Entries` was a newtype variant and is
now a struct variant; a 0.21.0 node and a node carrying this cannot replicate
in either direction, so this is a `0.MINOR` and the changelog says so. Pre-1.0
the project does not carry compatibility shims or negotiate versions
(`docs/compatibility.md`): `AskEntries::held` was added as an optional field
because it could be, and this cannot be — a default of `exhausted: true` from
a sender that never sets it is exactly the wrong answer, and a default of
`false` wedges every round against an older peer. An upgrade rolls the members;
a member that has not been rolled yet fails its rounds loudly, which is the
behaviour to want here.

**What it does not change.** `BatchTooLarge` still refuses to serve fewer
entries unasked: the requester asks again for the count that fits, the sender
re-reads the window at that limit, and the end it reports matches what it
sends. Trimming in place would report having scanned past entries it did not
send, which is the same silent gap by another route — and is now clamped
rather than only discouraged.

**Cost.** One stamp and one bool per batch on the wire, against a batch of up
to 1,024 oplog entries, plus one comparison per batch for the clamp.
`coverage_after_batch` gets simpler, not more complex.

Defended by `an_exhausted_window_proves_the_whole_advertised_vector`,
`a_truncated_window_proves_every_origin_up_to_the_stamp_it_reached`,
`a_window_short_of_the_tail_claims_only_what_it_scanned`,
`a_window_ends_at_the_last_entry_the_scan_read_not_the_last_it_kept`, and, for
the clamp, `a_peer_that_over_reports_its_window_claims_only_what_it_sent` and
`a_window_that_carried_nothing_and_is_not_a_tail_claims_nothing`.

`a_violation_stamp_does_not_pin_behind_for_ever` keeps ADR-082's guard honest:
this fix must not be reachable by reverting to "absorb only what was
delivered". Converging is *not* what proves that, and the test says so — once
the scan reaches a withheld stamp at the oplog's head, clipping every origin
to it gives the same answer as absorbing the peer's vector, because a peer's
advertised vector does not normally exceed its own log. The two rules differ
only where it does, which happens for real: a snapshot grants coverage for
entries the node will never hold (ADR-036), and retention collects a log out
from under a vector that persists (ADR-097). So the test takes the *converged
round's own exhausted window* and evaluates the rule against a vector
advertising an origin above it, where absorbing answers the granted stamp and
clipping leaves that origin pinned at the window's end for ever.


## ADR-128 — An explicit JSON `null` for a declared field is refused, not read as absent

**Decision.** Every optional field of a closed request shape (ADR-121) that
is not itself meant to be nullable is deserialized through
`json::non_null_field`, a `deserialize_with` defined once beside `JsonBody<T>`
in `crates/kimmy-api/src/json.rs` — the same file, and the same reasoning, as
ADR-121's own locus for the rule it states once. Serde's derive routes every
`Option<T>` field through `Deserializer::deserialize_option`, and for JSON
that method treats a present `null` and an absent key identically: both call
the visitor's `visit_none`, and neither ever reaches `T`. `non_null_field`
supplies its own visitor whose `visit_some` hands the value straight to `T`,
unchanged from what `Option<T>` already did, and whose `visit_none` — reached
only when the key was present and its value was `null`, since an absent key
never invokes `deserialize_with` at all — answers
`invalid_type(Unexpected::Unit, …)`, the same call serde_json's own
deserializer makes for a `null` given to any type that cannot hold it. The
`422` and its envelope come from the same `JsonRejection → ApiError`
conversion every other malformed body already goes through, so the message
reads exactly like `if_stamp: invalid type: integer \`123\`, expected a
string` does today: `if_stamp: invalid type: null, expected a non-null
value`. An absent key still yields `None`; nothing about omission changes.

The attribute is on every `Option<T>` field of every route struct behind
`JsonBody<T>`: `FindRequest` (`filter`, `sort`, `projection`, `limit`, `skip`,
`cursor`), `FindAndModifyRequest` (`filter`, `sort`, `update`,
`returnDocument`, `projection`, `if_stamp`), `UpdateRequest` (`filter`,
`if_stamp`), `DeleteRequest` (`filter`, `if_stamp`), `CreateIndexRequest`
(`name`, `enforcement`, `expireAfterSeconds`, `partialFilterExpression`),
`webhooks::RegisterRequest` (`operations`), and `vectors::SearchRequest`
(`query`, `vector`, `filter`, `k`, `per_document`, `weights`, `min_overlap`) —
every closed shape `find`, index creation, webhook registration and both
searches, all of which ADR-121 names as closed, add to the four write shapes
finding 12 itself demonstrated the hole on.

`POST .../vector` is on that list too — ADR-121 names "a vector
configuration's `provider`" as a nested shape it closes, for every provider
kind but `byo` until ADR-134 — but not by putting the attribute on
`kimmy_core::VectorConfig`/`ProviderConfig` themselves. Those two are not only
this route's request body; they are also the stored form, inside
`CollectionMeta`, and the replicated one, inside `VectorSet`'s BSON — and
several of their fields (`ProviderConfig::{OpenAi,Cohere,Gemini}`'s
`endpoint`, `CustomHttp`'s `api_key_env`) have `#[serde(default)]` but no
`skip_serializing_if`, unlike `dimensions` and `max_tokens` beside them, so an
unset one has always serialized as a literal `null` rather than an absent
key. Putting `non_null_field` on `VectorConfig` itself would refuse to load
or replicate-apply exactly the records that shape has always produced by
leaving a field at its default — turning an upgrade into a node that cannot
read its own configuration. So the refusal lives on
`kimmy_api::vectors::VectorConfigInput` — a request-only mirror of
`VectorConfig`, with `ChunkConfigInput` and `ProviderConfigInput` mirroring
its two nested shapes — deserialized from the fresh HTTP body and converted
with `From` before anything stores or replicates it, exactly the shape
ADR-121 already used for `GrantInput` over `kimmy_auth::Grant`, and for the
same reason: a wire shape and a persisted one are not always the same
closure, even when they are (there, and here until now) the same type.
`kimmy-core/src/vector_meta.rs` carries a test,
`a_null_endpoint_or_key_variable_still_decodes_as_absent`, pinning that the
real type stays exactly as permissive as every version before this one.

The MCP tools carry the same attribute on the same fields, named for
`kimmy_api::json::non_null_field` rather than redefined: `DescribeArgs`
(`sample`), `FindArgs` (`filter`, `sort`, `projection`, `limit`, `skip`),
`CountArgs` (`filter`), `SearchArgs` (`query`, `vector`, `filter`, `k`),
`HybridSearchArgs` (`query`, `vector`, `filter`, `k`, `weights`,
`min_overlap`), `UpdateArgs` (`filter`), `DeleteArgs` (`filter`), and
`CreateIndexArgs` (`name`) — the same `{"filter": null, "multi": true}`
deletes-everything case, reachable through `delete`'s tool argument the same
as through the REST body, since rmcp deserializes both with plain serde and
neither the HTTP path nor the tool path is more closed than the other by
right. `InsertArgs`'s `document`, `BulkInsertArgs`'s `documents` and
`AggregateArgs`'s `pipeline` are content the same way a REST document body
is, and are untouched for the same reason.

Each of those fields also carries `#[schemars(required)]` alongside
`skip_serializing_if = "Option::is_none"`. schemars derives a tool's
`inputSchema` from the field's Rust type, not from `deserialize_with`, so an
`Option<T>` field's advertised schema is `["T", "null"]` with
`"default": null` by default — accurate before this decision, since `null`
genuinely was accepted, and wrong afterwards: `delete.filter`'s schema would
have kept telling the one client that reads it first, the model, that `null`
is not just valid but the *default* value of the argument whose `null`
emptied a collection. `#[schemars(required)]` asks schemars for the plain,
non-nullable schema of the inner type instead of `Option`'s; pairing it with
`skip_serializing_if` (inert for these `Deserialize`-only structs at
runtime, read only by schemars) drops the `null` default that
`#[serde(default)]` would otherwise report. The property is still not in
`required`, since omitting it is unaffected — only its `null` is. The schema
test in `tests/mcp.rs` drives every field in the table above through
`schema_forbids_null` as well as the runtime refusal, so the two cannot
drift apart unnoticed again.

No field opts into nullability today; one that genuinely would keep the bare
`Option<T>` derive and say so where it is declared. Query-string structs are
untouched: a query string cannot carry a JSON `null` — `?if_stamp=` is an
empty string, refused already as a malformed stamp — so ADR-124's closure
needed nothing here. Neither does a document body: `insert`, `replace` and
bulk insert take `JsonBody<Value>` or `JsonBody<Vec<Value>>` directly, with no
declared fields to hold this rule, and a document's own content, a filter's,
or an update operator's operand is read straight into `Value`, which
legitimately holds `null` — this rule reaches only a request shape's own
declared fields, never what a `Value`-typed field's document holds. It also
does not reach a field that is not itself optional: `update`'s `update` on
`POST .../update` is a required `Value`, so `null` there becomes
`Value::Null` and is refused downstream at `400`, by that field's own type,
rather than by `non_null_field` — which only ever runs on a field the derive
would otherwise default to `None`. Both are refused; only the status and the
mechanism differ, and `a_null_required_field_is_refused_by_its_own_type_not_by_non_null_field`
pins the distinction so it reads as deliberate.

One nested shape names the wrong field when it refuses: a value inside a
`#[serde(tag = "kind")]` enum, `ProviderConfigInput` among them, is refused
as `"provider: invalid type: …"` rather than `"provider.endpoint: …"`.
`serde_path_to_error` tracks a path by wrapping the `Deserializer` the
top-level call uses; once an internally-tagged enum's own tag is matched, the
rest of that value is re-read from a buffered `Content` tree through a
second, unrelated `Deserializer` the wrapper never sees, so nothing after the
tag is matched can extend the tracked path. This is not new to this decision
and not particular to `null`: a wrong-typed `dimensions` inside the same
`provider` object truncates identically, and always has —
`a_wrong_value_inside_a_tagged_enum_names_the_enums_own_field_not_the_inner_one`
pins both alongside the ordinary, untagged `chunk.max_tokens`, whose path
reports in full. Documented in `docs/http-api.md` and `docs/openapi.yaml`
rather than worked around: the refusal and its status are both right, only
the name is short, and reaching further would mean threading a second,
scoped path tracker across serde's own enum-tag buffering — a fix for
`serde_path_to_error` and internally-tagged enums generally, not something
this decision's scope extends to.

A test in each of `kimmy-api` and `kimmy-mcp` enumerates every shape and
field this decision claims and drives it over a real socket, asserting `422`
(`kimmy-api`) or the tool's own `isError` (`kimmy-mcp`) for an explicit
`null` and success for an absent key — the table-driven guard the review that
returned this unit asked for, so a field added to a closed shape later
without the attribute fails the suite rather than shipping quietly. Nothing
shorter of a proc macro that refuses to compile a bare `Option<T>` on a
closed shape is fully fail-closed by construction the way ADR-124's route
table is; this is the nearest practical approximation, and is named as a
residual below.

**Why.** `{"if_stamp": null}` on `update`, `delete` or `find_and_modify`
answered `200` and wrote unconditionally — the opposite of what the field is
for, and the exact failure ADR-121 named in its own motivation, one door
further in. Worse, `{"filter": null, "multi": true}` on `/delete` answered
`200 {"deleted": <every document>, …}`: an optional `filter` defaults to "no
filter", which is "match everything", so a caller whose JSON encoder writes
an unset field as `null` — the default behaviour of many — sent what it
believed was a scoped delete and emptied the collection. The same shape on
`/update` rewrote every document instead. Every *other* malformed value of
these same fields was already refused: `if_stamp: 123` and `if_stamp: true`
both `422`, `if_stamp: "not-a-stamp"` `400 malformed stamp`, and `multi:
null` on the non-optional `multi` field was already `422` because a required
field with no `Option` wrapper has no null-shortcut to fall into. `null` was
the one value serde's own machinery does not already refuse for an optional
field, on every route that declares one — found independently by two areas
of the same test round, one from `if_stamp` and one from `filter`, neither
aware of the other's result.

**Alternatives.**

- *Patch `if_stamp` alone, by hand, on each of the four write shapes.*
  Rejected, and stated as the failing outcome this decision exists to avoid:
  it leaves `filter: null` open, which is the destructive case. Four
  independent write shapes carrying the identical hole is what makes a
  per-field patch the wrong shape of fix.
- *Walk the raw JSON body and refuse any `null` found anywhere, ahead of
  typed deserialization.* Rejected. `JsonBody<T>` is generic over every
  request shape the server has, including `insert` and bulk insert, which
  take `JsonBody<Value>` and `JsonBody<Vec<Value>>` directly — a document
  *is* its own top-level body on those routes. A walk with no notion of
  which keys are declared fields of a shape and which are a document's own
  content cannot draw ADR-121's boundary; it would refuse `{"note": null}` on
  `insert` and break the routes ADR-121 named as deliberately open.
- *Change the field's declared type instead of its deserialization —
  `Option<NonNull<T>>` or similar.* Rejected. The null-shortcut lives in how
  serde's derive calls `deserialize_option` for any `Option<U>`, whatever
  `U` is; wrapping the inner type changes nothing; a JSON `null` never
  reaches `U::deserialize` regardless of what `U` is, wrapped or not. Only
  supplying a different visitor for `deserialize_option` itself — which is
  what `deserialize_with` lets a field do — intercepts it.
- *A distinct, type-specific message per field ("expected a string", "expected
  a document").* Rejected for the same reason ADR-121 keeps its message
  generic: one function reused everywhere is worth more than wording tuned
  per call site, and "a non-null value" already says what a client needs to
  fix.
- *Put `non_null_field` on `kimmy_core::VectorConfig`/`ProviderConfig`
  directly, since deny_unknown_fields already lives there.* Rejected, for
  the reason ADR-121 gave for leaving `VectorConfig` out of the
  specification's closed-request treatment, one level more serious here: a
  response schema staying open so a new field is additive is a documentation
  concern, but a stored or replicated record decoding under a later version
  is a durability one. `ProviderConfig::endpoint` already serializes an unset
  value as a literal `null` and always has, so closing the type itself would
  refuse to load metadata this exact version of the server wrote. `kimmy-core`
  cannot depend on `kimmy-api` to reach `non_null_field` even if this were
  safe, which is the surface version of the same problem: the type is used
  where the rule must not reach.
- *Move `non_null_field` down into `kimmy-core` so `VectorConfig` could use it
  directly, gated some other way from the storage and replication paths.*
  Rejected as more machinery for less clarity than a mirror: it would need a
  second entry point, or a flag threaded through every deserialization call
  site, to tell "this is a fresh request" from "this is a stored record" —
  exactly the distinction `VectorConfigInput` draws for free by being a type
  that only ever exists on the request path. `GrantInput` already established
  the pattern for a request shape that happens to coincide with a persisted
  one; reusing it costs a `From` impl, not a new mechanism.

**Cost.** Breaking for a client relying on the bug: one that sends `null` for
an unconditional write dressed as a conditional one, or a `null` filter
meaning "everything", now meets a `422` (or, over MCP, the tool's own
`isError`) instead of a silent write. A `0.MINOR` bump under the pre-1.0
policy, no compatibility shim — sending `null` was never documented to mean
anything, and `openapi.yaml` already typed every one of these fields without
a `null` branch, so *that* schema was already correct and needed no change;
only the server's behaviour was not. The MCP tool schemas were a different
story and did need one: schemars derives `inputSchema` from the field's Rust
type regardless of `deserialize_with`, so every one of these fields
advertised `null` as valid — accurately, before this decision — and fixing
the server without telling schemars would have shipped a contract that lied
to the one reader who checks it first, an agent, worst on `delete.filter`,
whose schema would have called the one value that used to empty a collection
its *default*. `#[schemars(required)]` plus `skip_serializing_if` on the same
twenty fields closes that, at the same per-field cost as the attribute that
opened it. One helper function, reused wherever a field needs it — kimmy-mcp
names it from kimmy-api rather than redefining it — one line per field
naming it, and one mirror type for `POST .../vector`'s three shapes: the same
order of cost ADR-121's `deny_unknown_fields` attribute has today, plus what
`GrantInput` already cost once. The first-party Rust and Python clients, the
CLI, the MCP server's own use of these tools, the conformance scenarios and
every request example in the documentation omit an unset optional field
rather than encoding it as `null`, so none of them are affected.

The Go client is not quite in that list, and not uniformly fixed the same
way. `Count`'s existing `nil`-to-`{}` guard is right to keep and right to
extend to `UpdateIf` and `DeleteIf`: neither takes `multi`, so a `nil`
filter there is bounded to one document by `if_stamp` regardless, exactly as
harmless as `Count`'s always was. `Update`, `UpdateWith` and `Delete` are the
opposite case: each takes `multi` as the caller's own choice on every call,
so applying the same guard there would have reproduced the exact defect this
decision closes, one layer further out — `Delete(ctx, db, coll, nil, true)`
would send `{"filter": {}, "multi": true}` and empty the collection, where
sending `nil` as JSON `null` now gets the caller a `422` instead. A `nil`
Go map is precisely what a caller gets from *forgetting* to build a filter,
which is finding 12's own scenario arriving through a client rather than the
wire; guarding it there would have converted a mistake the server now
catches back into a silent one. So those three are left to send `filter`
exactly as given, `nil` included, and the doc comment on each says so.


## ADR-129 — An aggregation stage operand with a fixed key set is closed; a field-path map stays open

**Decision.** A pipeline stage document that has a *fixed* key set —
`$unwind`'s document form (`path`, `preserveNullAndEmptyArrays`,
`includeArrayIndex`), `$lookup`'s both forms (`from`, `as`, `localField`,
`foreignField`, `let`, `pipeline`), `$replaceRoot` (`newRoot`) — refuses a key
it does not define, `400`, naming the field and listing the ones the stage
takes. `crates/kimmy-query/src/aggregate.rs` gets one helper,
`deny_unknown_keys(stage, doc, allowed)`, called at the top of each of those
stages' parsers, before anything else about the document is read.

**`$match` and `$project` are not put through it, and never will be.** Both
take a *field-path map*: every key is a document field name the caller chose,
not a word this codebase defines, so there is no fixed vocabulary to check a
key against — `{"$match": {"bogusFieldName": 1}}` is not a typo of anything,
it is a filter on a field called `bogusFieldName`, and refusing it would
refuse most ordinary pipelines. `$sort`'s keys and a `$group` stage's output
field names are field-path-shaped the same way and are left alone for the same
reason; `$group`'s `_id` and its accumulator arguments are expressions, open
by the same rule expressions have always followed. This is the line ADR-121
already drew for the request body — a document body is content, not a shape —
extended one level down: a stage operand is a shape when its keys are
vocabulary, and content when its keys are data.

`includeArrayIndex` — a real MongoDB `$unwind` option that this database
silently dropped — is implemented rather than refused by name: it is the name
of a field to hold the position of the array element that produced each output
row, `null` on a row that was not produced by fanning one out (an unwound
scalar, or a document kept by `preserveNullAndEmptyArrays`). It is trivial
here because `unwind`'s expansion loop already knows the element's position
the moment it produces a row; adding the field costs one more `path::set`
alongside the one it already makes. Two more names are refused, on KimmyDB's
own internal consistency rather than on parity: a name beginning with `$`,
because this language's own field-path syntax reads `"$name"` as the field
called `name`, never one called `$name` — a document beginning with `$` is an
operator (`aggregation.md`'s own rule) — so `$unwind` could write such a field
and no later stage could ever read it back; and a name equal to `path` itself,
because it would silently overwrite the element `$unwind` just placed there
with its own index.

**A closed key set is not the whole hazard; a closed key's *value* is
another.** `preserveNullAndEmptyArrays` reached `Bson::as_bool`, which returns
`None` — read here as `false` — for anything that is not literally a boolean.
So `{"preserveNullAndEmptyArrays": "true"}` or `: 1` answered `200` with the
option silently reverted, the exact failure mode finding 11 is named after,
moved from the key to the value. It is now refused by name, matching
`includeArrayIndex`'s existing type check three lines below it in the same
parser — leaving one and not the other would have been the same
inconsistency this ADR closes elsewhere, in miniature.

**The same defect class, found in two expression operands while this unit
was in the file.** `$dateToString`'s `format` and `$switch`'s `branches`
(and each branch's `case`/`then`) are fixed-key documents too, in
`crates/kimmy-query/src/expr.rs`, and were not closed: `{"$dateToString":
{"date": "$t", "formt": "%Y"}}` — the finding's own shape, one character
short — silently kept the default ISO-8601 format in every row rather than
naming the typo, precisely the hazard `aggregation.md` already argues for an
unknown date specifier. Both now go through `Expr::named_spec`, the helper
`$filter`, `$map`, `$reduce` and `$let` already used for the same purpose;
closing them cost two more calls to it, not a new mechanism.

**Why.** ADR-121 states the rule reaches "the shapes nested inside" a request
body — an index's field entry, a search's fusion weights — and gives, as its
own motivating example, a misspelt `colection` in a grant that silently
widened it to every collection in the database. An aggregation stage operand
is exactly such a nested shape, and it was not closed: `{"path": "$x",
"preserveNullAndEmptyArray": true}` — one character short of
`preserveNullAndEmptyArrays` — answered `200` with the option silently
reverted to `false`, dropping documents the caller asked to keep, and
`includeArrayIndex` on a real pipeline answered `200` with the field silently
absent. `$group` already refused an unknown accumulator by name; the
inconsistency was that the rest of `aggregate`'s own stage operands did not
get the same treatment ADR-121 gives everything else nested in the request.

The reason ADR-121's mechanism — `#[serde(deny_unknown_fields)]` on a typed
struct — does not reach here is structural, not an oversight to route around:
`AggregateRequest.pipeline` is `serde_json::Value`, because a pipeline stage's
shape depends on which operator names it, which `kimmy-api` does not decide —
`kimmy-query` does, by hand, off a `bson::Document`, the same way `$group`
already refuses an unknown accumulator. So the closure is enforced where the
stage is actually parsed, with the same `Error::InvalidQuery` and `400` that
`$group`'s refusal and every other malformed-pipeline error already use, not
`422` — `422` is `JsonBody<T>`'s status for a shape serde itself rejects, and
a stage operand was never one of those.

**Alternatives.** *Give every stage a typed, `deny_unknown_fields` struct and
deserialize each `Document` into one via `bson`.* Rejected: stages are parsed
by hand today specifically because several of them are not one-shape-fits-all
— `$unwind` takes a string or a document, `$project`'s values are flags in one
branch and expressions in another, `$lookup` is two mutually exclusive shapes
sharing two keys — and a serde struct would have to re-express all of that
through its own attributes for four call sites, at the cost of the manual
parser's existing, readable error messages. A small helper called at each
parser's entry gets the same closure for a fraction of the code. *Silently
drop `includeArrayIndex` and document it as unsupported.* Considered, since
the finding permits it — but the loop that would carry it already has the
element and its position in hand, so refusing was pure cost for no benefit.

**Cost.** A `0.MINOR` behaviour change: a pipeline that sent an unrecognized
key to `$unwind`, `$lookup`, `$replaceRoot`, `$switch` or `$dateToString` and
relied on it being ignored now gets a `400` instead of a silent `200`, and one
that sent a wrong-typed `preserveNullAndEmptyArrays` or an
`includeArrayIndex` that begins with `$` or names `path` gets the same.
`$unwind`'s document form gained a field (`include_array_index:
Option<String>` on `Stage::Unwind`), threaded through `apply_with_vars` and
`unwind`. Tests in `crates/kimmy-query/src/aggregate.rs`: the load-bearing
misspelling pair, the same pair moved to the *value* of a correctly-spelled
key, `includeArrayIndex` actually working and its two new naming refusals, an
unknown `$lookup`/`$replaceRoot` key refused, and a control proving `$match`
and `$project` still take any field name; two more in
`crates/kimmy-query/src/expr.rs` for `$switch` and `$dateToString`.
`docs/aggregation.md#stages` documents the document form,
`preserveNullAndEmptyArrays`, `includeArrayIndex` and the closed/open line;
none of it was written down anywhere before. `docs/http-api.md` states the
`400`-not-`422` distinction this ADR draws, since its own status-code table
would otherwise read as a blanket `422` for every unknown request field.

---

## ADR-130 — `$unwind` refuses a path that crosses an array rather than writing nowhere

**Decision.** `crates/kimmy-query/src/aggregate.rs` gets a helper,
`crossing_array(doc, field)`, that decides — for one document, before
anything at `field` is read — whether writing to `field` would fail, and if
so, where: it walks `field`'s segments against `doc`'s actual structure,
read-only, the same way `path::set` would, and returns `(array_path,
remainder)` the moment a non-terminal segment lands on an array whose next
segment names a field rather than a numeric position — `path::set`'s one
failure mode, and what "the path crosses an array" means here. A segment
that is missing, a scalar, or an array reached by an index that is not
already a document is what `path::set` would vivify or overwrite rather than
fail on, so the walk stops there and reports no crossing, without needing to
actually perform a write to find out — `crossing_array` never clones or
mutates `doc`. `unwind` calls it once per document, at the top of its loop,
unconditionally: if it reports a crossing, the document — and the whole
request, `$unwind` is not a per-document filter — is refused, `400`, naming
`$unwind`, the field path, the array it crosses (`array_path`), and the
concrete remedy: unwind `array_path` first, then read `remainder` on each
resulting row — the caller who wrote `$unwind: "$items.sku"` is told to
write `$unwind: "$items"`, not handed `path::set`'s internal vocabulary
about non-numeric segments. This holds **regardless of what `field` turns
out to contain**: a non-empty array, an empty one, a scalar, `null`, nothing
at all. Only once the check passes does `unwind` go on to read `value_at`
and decide, by the existing rules, whether to expand, drop, preserve or pass
a document through; every write on that path is now guaranteed to succeed
structurally, so `path::set`'s `Result` there is still propagated with `?`
rather than unwrapped — a proof that holds today is not a reason to let a
future change panic instead of refuse. `preserveNullAndEmptyArrays` and
`includeArrayIndex` are unaffected by any of this: the crossing check runs
before either is consulted, and a document it refuses never reaches them.

**This is a revision, made under independent review, of what this ADR first
proposed.** The version first shipped kept `value_at`'s ordinary read and
refused only when `path::set` failed *while expanding an array `value_at`
had found* — i.e., only when the value sitting at the far end of the crossed
segment happened itself to be an array. That is not a rule a caller could
state or predict: whether `$unwind` refused depended on the *type* of one
crossed element, decided by the data, not on the path crossing an array at
all. Two consequences followed, both wrong answers of the exact shape this
ADR exists to prevent. `a: [{b: 9}, {b: [1, 2]}]` with `$unwind: "$a.b"`
answered `200` with one row that looked unwound but was not — `value_at`
found `9` at the first element, a scalar, so the array-expansion branch, and
therefore the write, was never attempted. And `items: [{sku: "a", qty: 1},
...]` with `$unwind: "$items.sku"` — finding 10's own row 5 — kept answering
`200` unchanged exactly as it had at `26944f8`: `sku` is a scalar at the
first element, so this defect's own reported case was still not fixed by
that version of the fix. The check now runs on the path's *structure* alone,
independent of what is found, so both refuse.

**Why refuse rather than MongoDB's silent skip.** The finding gave two
options: resolve the path without descending into arrays, so a path that does
not land on a single array is treated as absent (dropped, or kept once under
`preserveNullAndEmptyArrays`); or refuse. MongoDB does the former. This
project does the latter, for reasons specific to what this codebase already
decided:

- **ADR-116 did not reach this case, and its reasoning for the case it did
  reach argues for a refusal here.** ADR-116 decided how `$unwind`'s path is
  *read* — the single, non-fanning value, "because `$unwind` needs a single
  place to write each element back to" — but it never decided what happens
  when that single place does not exist; a path crossing an array was not a
  case its rule set covered, and this ADR is the first to decide it. Its own
  reasoning points the same way it always would: a missing field and an
  unwritable one are different failures, and only one of them is silent by
  design. **This does cost ADR-116's own compatibility claim for this one
  shape** — *"a pipeline that runs there runs here with the same result"* —
  which this codebase is no longer trying to hold as a general bar (parity is
  not the bar this project is held to; a divergence is not by itself a
  defect), but which ADR-116 stated as its reason, so it is named here rather
  than left standing unqualified: a pipeline whose path crosses an array does
  not run the same here as there, deliberately, because refusing loudly beats
  running silently wrong.
- **Every other stage in this file that cannot honour what it was asked
  refuses rather than approximates**, and says so in its own comments: a
  blocking stage over the cap is "an error naming the stage rather than a
  truncated result" because a partial `$group` "looks exactly like one over
  all of it"; `$lookup` run by the pure pipeline refuses rather than passing
  its input through, because that "is a wrong answer wearing a right answer's
  shape." A silently dropped document is the same shape of wrong answer:
  correct-looking, `200`, and undetectable by the caller — exactly what
  produced this finding when the row was duplicated unchanged instead. MongoDB
  accepts that cost because the drop is documented and expected there; this
  codebase's standing preference, stated in `deviations.md` and repeated
  through ADR-121 and ADR-124, is that a request the server cannot honour is
  refused rather than answered as though it had been.
- **It is the smaller change.** The refusal mirrors `path::set`'s own
  traversal rule read-only, once per document, with no new resolver and no
  second notion of what a path "means"; the silent-skip contract would need
  a genuinely different reader — one that fails to resolve at all through an
  array, distinct from both `value_at` and `path::set` — solely to
  reproduce, on purpose, the same "found nothing" outcome a missing field
  already gets, and to decide, independently, what `preserveNullAndEmptyArrays`
  should mean for a case that is not actually missing.

**What is unaffected.** A path that never crosses an array on a given
document — a top-level array field, a dotted path through plain
subdocuments (`$unwind: "$y.b"` where `y: {b: [1, 2]}`), or a numeric-indexed
segment into an array (`$unwind: "$a.0"`, which addresses a position rather
than crossing) — writes back exactly as it always has; `crossing_array`
reports nothing for any of these, because `path::set` only fails on a
non-terminal *non-numeric* array segment. **A path that crosses an array into
a scalar is no longer unaffected — this is the behaviour change this
revision makes, named plainly**: `$unwind: "$items.sku"` where `items` is an
array of `{sku, qty}` now refuses, `400`, on every document where `items` is
an array, whatever `sku` holds there. It is a bigger break than the version
first shipped, and it is the point: it is the only contract that closes the
finding's own row 5 rather than leaving it standing.

**This makes the refusal data-dependent, and the cost of that is stated in
full below rather than waved past.** Whether `{"$unwind": "$a.b"}` is legal
depends on whether `a` is ever an array in the documents it meets, which a
schemaless collection does not fix in advance and does not enforce; the same
pipeline can be correct today and refuse tomorrow after an ordinary write
adds one document shaped that way, with nothing about the pipeline having
changed, and the refusal reaches every document in the request, not only the
one shaped that way.

**What protects ADR-116's non-fan-out reading of `$unwind`'s path, now that
this test no longer runs `$unwind` over the fixture that used to.**
`unwind_and_lookup_keys_read_a_field_path_and_do_not_fan_out`'s `$unwind`
assertion (`out.len() == 2`) was removed, not weakened: over its fixture
(`a: [{b: [1, 2]}, {b: [3]}]`), a *fanning* reader would compute `$a.b` as
`[[1, 2], [3]]` (ADR-116's own array rule) — also length 2 — so the old
assertion held identically under the reading ADR-116 chose and the one it
rejected, and never had the power to pin that choice; it pinned the defect's
row count. `lookup_keys` in the same test still does: a fanning reader would
make `lookup_keys(&input, "a.b")` a single `[[1, 2], [3]]` key, not the two
flat integers the assertion checks, and `$lookup`'s key extraction reads
without ever writing, so it is untouched by anything in this ADR.

For a **non-numeric** crossed segment, `$unwind` itself needs no test of its
own reader choice, because the uniform check refuses on exactly the documents
where a fanning and a non-fanning reader would ever disagree there: an array
on a non-terminal segment whose next segment is not numeric is both
`path::set`'s one failure condition and where fanning would diverge from the
single-value read, so both readers are refused alike, `items.sku` included.

**A numeric segment is the one place this does not hold, and ADR-116 already
named it as such** — *"the one place an expression path and a filter path
disagree: the filter language reads `items.0` both ways"* — and `value_at`
(`path::resolve`) follows the filter's rule, reading a numeric segment as
both an index and a field name, while the fanning expression reader reads it
only as a field name, never an index. A numeric segment after a crossed
array is therefore not refused — `path::set` succeeds by index, the same
"single place" a numeric-indexed write always has — and `$unwind`'s own
output *does* distinguish the two readers there:
`unwind_over_a_numeric_segment_into_a_crossed_array_reads_by_index_not_by_fanning`
pins it. Over `a: [{b: [1, 2]}, {b: [3]}]`, `$unwind: "$a.0.b"` reads `a.0.b`
as `[1, 2]` (found once, by index) and produces two rows, `a[0].b` set to `1`
then `2`; the fanning reader over the same path finds no element literally
named `"0"`, gets `[]`, and would produce none. `$unwind: "$a.0"` in "What is
unaffected" above is exactly this case one segment shorter, and is itself
the counter-example to the broader "unobservable" claim an earlier revision
of this ADR made here — corrected under further review, which is why this
paragraph, unlike most of this document, describes a mistake made and fixed
within the same unit rather than a decision reached once.

**Alternatives.** *MongoDB's silent-skip contract*, rejected above. *Refuse
only when the value found at the crossed segment happens to be an array* —
this ADR's own first version, rejected above as not a statable rule and as
leaving the finding's own row 5 unfixed. *Detect "crosses an array" up
front, at parse time, before any document is read.* Not possible here:
whether a given document's `a` holds an array is data, not schema, in a
document database with no required shape — the same document under a
different `_id` might hold a plain subdocument at the same path. The
refusal is necessarily per-document, discovered by a structural walk, which
is exactly what `crossing_array` does as early as it can.

**Cost.** A `0.MINOR` behaviour change, larger than first estimated. A
pipeline that unwound a path crossing an array by a named field previously
got a `200` with byte-identical duplicate rows (N = the first element's
array length, nothing actually unwound) and now gets a `400` naming
`$unwind`, the crossed array and the field to read from each of its
elements — on *any* document shaped that way, not only ones where the
crossed element also held an array. **This is not limited to a pipeline that
was already producing wrong rows.** `$unwind: "$a.b"` over a collection
where every document has `a` as a plain subdocument runs, and returns
correct rows, exactly as before; the same pipeline over a collection where
even one document has `a` as an array — a shape that never triggered the
original defect, because the crossed element might have been a scalar — now
refuses that request entirely. A field a pipeline unwinds must be one that
is never an array on any document reaching the stage, unless every segment
past it addresses a position by number rather than by name;
`docs/aggregation.md#unwind` states this as the operational rule a pipeline
author needs, not only as an implementation detail, and describes the
numeric exception and what it produces. Seven tests in
`crates/kimmy-query/src/aggregate.rs`: the finding's shape refusing; a
scalar found first no longer skipping the refusal (`a: [{b: 9}, {b: [1,
2]}]`); the non-crossing control (`$unwind: "$y.b"`); the real corpus shape
(`$unwind: "$items.sku"`), now refusing rather than passing through; a
single-element array refusing identically to a multi-element one, since the
check never inspects length; and the numeric-segment residue
(`unwind_over_a_numeric_segment_into_a_crossed_array_reads_by_index_not_by_fanning`).
An eighth, over HTTP in `crates/kimmy-api/tests/api.rs`
(`unwind_refuses_a_path_that_crosses_an_array_over_http`), reproduces the
finding's own route rather than only the parse layer. `docs/aggregation.md`
states the actual rule — including the numeric carve-out, which an earlier
revision of this documentation omitted, over-claiming that *every* crossing
refuses — in `$unwind`'s own value table and in `#arrays`, names the
data-dependence and the whole-request blast radius explicitly, and
`docs/http-api.md` draws the `400`-versus-`422` line ADR-129 also needs.
`crossing_array` walks the document read-only rather than cloning it to
probe a write, so the fix costs no allocation on top of what `unwind`
already made per row, and its error names the crossed array and the
remaining field directly — *"unwind `$items` first, then read `sku` on each
resulting row"* — rather than surfacing `path::set`'s internal vocabulary.


## ADR-131 — `explain: true` on `update` and `delete` plans the write; it does not perform it

**Decision.** `explain: true` on `POST .../update` and `POST .../delete` no
longer executes the write it was asked to describe. Both routes now run the
same read-only scan `find` and `count` already use — `exec::visit_matching`
— and report its `QueryStats` as `explain`, exactly as `find` does. Nothing
is written and no write transaction opens. The write-outcome fields keep
their existing names and existing meaning — "what was written" — so under
`explain` they report that nothing was: `matched`, `modified` and `commits`
are `0` on `update`; `deleted` and `commits` are `0` on `delete`; `stamp` is
absent on both — in every case identical to what a write that matched
nothing already reports today. What the write *would* touch is
`explain.documentsMatched`, the field `find`'s own `explain` has always
carried for the same question asked of a read. `explain` cannot be combined
with `if_stamp`, and is refused `400` with it: a plan checks no version, so
it cannot honestly answer whether a *conditional* write would happen — the
document it reports as matched may be exactly the one the real write,
checking the same stamp, refuses `409 stale` on. Covered by
`explain_plans_a_write_without_performing_it`, which drives a
single-document and a `multi: true` case on both routes, asserts the
response fields, asserts the engine's own commit counter is unmoved
(`state.engine.commits()`), and reads the document(s) back to confirm
nothing changed — plus a `find`-with-`explain` control, which was never at
risk — and by `explain_refuses_if_stamp`.

`update` and `delete` still plan their own write independently, through
`candidates_for` — unchanged by this ADR, and still primary-key, then
index, then scan, the same order `find` plans in. `visit_matching` does not
call `candidates_for`; it re-derives the same choice from
`plan::choose_primary_key`/`plan::choose` on its own. The two are **two
implementations of one policy, not one shared code path**, so nothing
forces them to keep agreeing — a change to one that silently stopped
choosing an index, say, would not be caught by anything that only drives
`explain`, because `explain` no longer touches the write's planner at all.
`the_write_planner_and_the_read_planner_choose_the_same_access_path` pins
the agreement directly: for an `_id` filter, an indexed equality, an
indexed `$in`, and an unindexed filter, it asserts `candidates_for` chooses
`Keys`/`Index`/`Index`/`Scan` and that `visit_matching`'s reported
`strategy` for the same filter is `idLookup`/`index`/`indexUnion`/
`collectionScan` — the corresponding answer, filter for filter. Without it,
nothing in the repository fails when `candidates_for` forgets how to plan
entirely: the existing index-routing tests
(`update_uses_an_index_when_one_applies`,
`delete_uses_an_index_when_one_applies`,
`without_an_index_the_write_paths_still_scan_and_still_agree`,
`a_single_update_still_touches_exactly_one_document`,
`an_indexed_update_over_an_array_field_still_matches_every_document`,
`a_targeted_write_on_id_also_takes_the_fast_path`) drive the plan through
`explain` — which, correctly, exercises only the *read* planner now — and
then perform the write as a second, unexplained request, which proves the
write still produces the right documents but not which access path it used
to get there: a full scan gives the same matches an index does, only
slower. Those tests keep the write executing and its result correct; the
new test is what keeps its *plan* honest.

**Why.** Found by the 2026-09 test round: `POST .../update` and
`POST .../delete` performed their write whenever `explain: true` was set,
`multi: true` included. A `multi: true` `delete` with `explain: true`
deleted every document in the collection; the equivalent `update` rewrote
every one. `explain` is a declared field on both routes, so this was not the
unknown-field gap ADR-121 closes — the documentation simply never said these
two routes execute. `http-api.md`'s only statement of purpose was "to see
whether an index was used"; the `Explain` schema's was "how a query **was**
answered" — past tense, with no dry-run language anywhere. The word itself
promises a description, not an act. The natural, careful use of `explain`
is to inspect a broad `multi` write before committing to it, and that use
was exactly the one that performed it: a `200` with
`matched`/`modified`/`commits` in the body, indistinguishable from success,
on a request whose entire purpose was to ask first.

The information `explain` wants was reachable without a write transaction
before this: `candidates_for` already chooses a filtered write's access path
— primary key, then index, then scan — with no engine call at all. What was
missing was a way to walk that access path and count without writing.
`find` and `count` already have one — `visit_matching`, built on the same
read-only primitives (`get_record_by_encoded_key`, `visit_index_candidates`,
`for_each_record_after`) the storage engine exposes outside any write
transaction — so `update` and `delete` now call it instead of reading the
plan back out of `ModifyManyOutcome` after the engine had already written.
This is not merely avoiding the write's side effect; it also stops
`explain` from conflating two different questions. "What would this filter
match" and "what did this write touch" used to be answered by the same
number, because the write always ran first; now the two routes ask the read
question exactly the way `find` and `count` do, and only run the write
question when there is a write to ask it of.

**What the response reports, and why.** `matched`/`modified`/`deleted`/
`commits`/`stamp` answer "what did the write do", and they keep exactly
that meaning — an `explain` response is not a second protocol bolted beside
the first, it is the ordinary response for a write that touched nothing,
because nothing was touched. Overloading `matched` to also mean "what the
plan admits" was considered and rejected: it would break the existing
invariant `modified == matched` for a real write, silently, and a
`{"matched": 5, "modified": 0}` reply reads as a partial failure long before
it reads as "asked, and not done". `explain.documentsMatched` already
existed, already means exactly the right thing on `find` and `count`, and
now means it identically on `update` and `delete` — a caller that wants "how
many would this touch" reads one field regardless of which of the four
routes it asked.

**Alternatives.**

- *Document that it executes, and name the effect in the response* — the
  finding's second-choice fix. Rejected: it keeps a route named `explain`
  doing the one thing `explain` conventionally never does, on the two routes
  where doing it by accident destroys data. A documentation fix closes the
  gap between the code and the page; it does nothing about the gap between
  the word and what a careful caller brings to it.
- *A separate `dryRun` field, leaving `explain: true` executing as before.*
  Rejected: two flags asking overlapping questions is worse than one that
  asks the right one, and it does not close the trap — a caller who reaches
  for `explain` first, which is the natural name to reach for, is still
  caught by it.
- *Report the plan's `documentsMatched` as the top-level `matched` too, so
  `explain` "previews" the write's would-be counts.* Rejected above: it
  breaks `modified == matched` exactly where a careful client is reading
  most closely, and duplicates a number `explain.documentsMatched` already
  carries.

**Cost.** Breaking, and named plainly in the changelog: a client that relied
on `explain: true` performing the write — indistinguishable, before this,
from not setting it at all, apart from the added `explain` field — now gets
a plan instead. A client sending `if_stamp` alongside `explain: true`, which
used to get a `200` (and a write), now gets `400`. `0.MINOR` under the
pre-1.0 policy, alongside the round's other tightened refusals.
`indexEntriesRead`, previously documented as absent for `update` and
`delete` because their `explain` was read back out of the write's own
bookkeeping, can now appear: the read-only scan is the same one `find` runs,
and reports the same thing when an index answers it — a wire-visible new
field on two response shapes, named in `CHANGELOG.md`. `docs/openapi.yaml`
and `docs/http-api.md` are updated with this ADR.

---

## ADR-132 — An index carries the stamp of its creation, and a drop and a rival are both settled by it

**Decision.** `IndexMeta` gains `created: Option<Stamp>` — the stamp of the
`CreateIndex` entry a local create mints, or of the entry a replicated one
arrived on. It is stored in the collection metadata and travels in the
`CreateIndex` payload, because the payload *is* an `IndexMeta`. Two things
follow from it, and neither is decidable without it.

*First*, a replicated `DropIndex` older than the index standing under its
name is history: the drop records its tombstone, which never moves backwards,
and leaves the index alone. That is the `DropCollection` arm's incarnation
rule (ADR-081) one level down, for the same reason — a recreated index derives
the same id as the one it replaced, and overlapping windows are re-served as a
matter of course.

*Second*, two members that created one name with different definitions settle
on the **later creation stamp**, by `Stamp::wins_over`, which is how two
concurrent writes to one document already settle (ADR-020, ADR-029). The
loser's entries are removed in the same transaction that builds the winner, so
a winning definition this node's documents cannot be built under aborts back
to the index this node already had — not to neither — and is skipped, counted
and warned exactly as ADR-123 says. The tombstone the replacement records is
under the *winner's* stamp, so the loser's own create cannot come back through
a re-served window while the winner's re-delivery, at exactly that stamp, is
not history. The snapshot route (`restore_collection`) follows the same rule,
in place of the silent skip it did for any name already taken. The **local**
path is unchanged: a client creating a conflicting definition is still refused
`IndexExists`, because it is there to be told.

*Third*, two members that created the **same** definition under one name
converge their creation stamps as well, forward, by the same comparison. That
is not a conflict — the definitions agree — but after the first two rules the
stamp is the sole arbiter of whether a replayed drop applies, so one
definition under two stamps answers one drop two ways and the members split.
Both have already witnessed the other's create by then, so nothing re-serves
it and the split is permanent; and the drop is `Applied` on both sides, so no
counter moves and the lag gauge reads 0 — the exact signature this ADR exists
to remove. The merge takes the **later** stamp because that is what
`created` means: the incarnation standing under the name. Both members hold an
index that has existed continuously since the later creation, and a drop
stamped before it was aimed at neither of them. Taking the earlier stamp would
converge just as well and would let a drop older than the incarnation delete
it — which is, verbatim, the residual this ADR exists to close, arrived at
through the merge instead of through the absent stamp. The first rule above
and this one are therefore one rule, not two that happen to agree.

An index holding no stamp adopts the peer's, which is the definition's true
creation rather than an invented one, and ends the ambiguity without waiting
for a recreation. A **local** create of a definition already present is
untouched — it is idempotent and mints no entry, so moving the stamp there
would be a decision no peer ever hears of.

*Not* the reason, though it is the first one that suggests itself: that the
earlier stamp would move `created` **backwards** as older creations arrived.
It would, and it costs nothing, because the tombstone that this ADR's own
declined drop records bounds the merge from below — `apply_remote_index`
turns away a creation older than the tombstone before
`create_index_inner` is reached, so no creation old enough to reopen an
already-declined drop reaches the merge at all. The two directions are
indistinguishable under re-delivery. They differ on the drop's *first*
delivery, and there the meaning of `created` decides it.

**An index stored without a creation stamp reads as older than every drop and
every rival, until it learns one.** A replayed drop removes it and a rival
definition is refused and counted — which is ADR-123's behaviour exactly, so
nothing a caller could predict from the reference before this changes for an
index that already exists. The exception is the merge above, and it is the
only one: a peer holding the *same* definition with a stamp hands it over, and
that stamp is the definition's own creation rather than an invented one, so
the index stops being ambiguous without any operator action. Where no member
has a stamp — an index every member created before this release — the sentence
holds as written until someone recreates it. It is deliberately not backfilled
at open from a local clock: an invented stamp would sort after drops that
genuinely superseded the index, and would win comparisons this node knows
nothing about. The ambiguity otherwise ends the first time the index is
recreated. Same `None`, and the same argument, as a
collection's `incarnation_floor` before ADR-081.

**Why.** Both halves are the residuals ADR-123 recorded and left open, and the
0.21.0 cluster round of 2026-09-03 found each of them doing damage. A
collection listed **no indexes on any member** though three stood on all three
an hour earlier — the shape of a drop re-served across a recreation of the
same name, applying because there was nothing to compare it against. And two
collections held **different index sets per member**, with the refusal counter
standing at 9 / 15 / 9: the counted divergence working as designed, and
staying divergent for as long as the cluster lived. ADR-123 named the fix for
both in the same sentence — "a 'newer definition wins' rule would resolve it,
but needs a creation stamp on `IndexMeta` to compare, which the definition
does not carry" — and this is that stamp.

**Why last-writer-wins, and not a second conflict rule.** KimmyDB already
resolves two concurrent writes to one document by the later stamp, node id
breaking the tie, and it resolves them the same way on every member so that
they converge without coordination. A schema change is a write; two members
creating one name are two writers of one key. Inventing a different rule here
— alphabetical on the definition, first-arrival, most-restrictive-wins — would
mean the cluster held two conflict rules, and a caller could predict neither
from the other. The stamp is also already the thing every other ordering
decision in the replication path is made on, so it needs no new machinery: the
comparison is `Stamp::wins_over`, unchanged.

**What of ADR-123 stops being true, and what replaces it.** ADR-123 promised,
under "what is left as a counted divergence", that two members creating one
name with different definitions each keep their own, that the second to arrive
is refused with `IndexExists`, and that the refusal is counted in
`kimmy_sync_ddl_refused_total`. That promise is withdrawn for the case where
both definitions carry a creation stamp: they now converge, and nothing is
counted. It still holds exactly as written where either definition carries
none. Everything else ADR-123 decided stands and is unchanged — the index
tombstone, the refusal class (`InvalidQuery`, `IndexExists`, `Unsupported`)
and its skip-and-count behaviour, the rule that every other error still fails
the round, the snapshot route's classification, the replicated unique
backfill, and the three counters. `kimmy_sync_ddl_refused_total` keeps its
meaning and its alert; what changes is that one of the three classes feeding
it now mostly resolves instead of arriving.
`a_definition_that_wins_the_stamp_but_cannot_be_built_leaves_the_one_it_would_replace`
is the regression test that keeps the guard honest: a definition that wins
the comparison and still cannot be built is skipped, counted, and does not
wedge the round — so this
fix cannot be made by reverting to "refuse every rival".

**Why the resolution is not counted on `/metrics`.** It was considered. A
superseded definition is the conflict rule working, not a divergence to alert
on, and KimmyDB counts no metric when two concurrent document writes resolve
either — counting one here would say that a schema conflict is an incident
while a data conflict is not. The operator's signal is a warning line naming
the database, collection, index and which parts of the definition moved, on
the member whose definition lost. What an operator *would* alert on is
unchanged: `kimmy_sync_ddl_refused_total` still rises for every definition a
member cannot apply, which is the case nothing repairs.

**Why the payload carries the stamp, when the entry already does.** On the
oplog route it is redundant, and provably so: the origin mints its entry's
stamp first and records that stamp on the index it builds, and `apply_ddl`
appends a replicated entry under the stamp it arrived with, so the payload's
copy and the entry's stamp cannot differ. Reading either gives the same
answer, and neither is observable from the other. The field is carried for the
**snapshot** route, which has no entry at all: `restore_collection` sees only
`CollectionState`, and the definition's own stamp is the only ordering fact
that reaches it. `apply_remote_index` reads the payload first and falls back
to the entry's stamp, so both routes read the creation stamp from the same
place; the fallback covers a payload minted by a build that recorded none, and
recovers exactly the value that build would have used.

**Alternatives.** *Compare the drop against the tombstone alone.* That is what
was there: the tombstone records when the index was **dropped**, and says
nothing about when the index now standing under the name was **created**.
*Refuse a `DropIndex` for an index whose definition differs from the one the
drop was aimed at.* A drop carries only the name — deliberately, since the
name is the identity — so there is no definition to compare. *Make the drop
carry the creation stamp it was aimed at.* It would work for a drop minted
after this change and not for one already in an oplog, and it puts the
ordering fact in the entry that destroys state rather than on the state
itself, so a snapshot would carry no answer at all. *Take the earliest
creation stamp when two members create one identical definition, since that
is when the index first existed anywhere.* It converges just as well, and it
is wrong for the reason the first rule of this ADR is right: `created` names
the incarnation standing under the name, and lowering it to a creation that
incarnation succeeded lets a drop older than the incarnation delete it — the
residual, reintroduced through the merge. Not rejected for
non-monotonicity: `created` would indeed move backwards, but the declined
drop's own tombstone stops any creation old enough to matter from
reaching the merge, so re-delivery is idempotent either way. The two differ on
the drop's first delivery, and that is the case that decides. *Resolve
concurrent definitions by merging them — keep the union of the fields, the
stricter uniqueness.* A merged definition is one no member asked for, and it
is not idempotent under re-delivery. *Backfill a creation stamp for stored
indexes at open.* Rejected above. *Keep refusing, and add a repair command.*
A schema that stays divergent until someone notices is what the round
observed; the
counter made it visible and nobody was watching for four runs.

**Cost.** One optional stamp per index, in the collection metadata and on the
wire. A replicated create of a definition already present now writes the
collection metadata where it used to return early, which is one small commit
on a path that previously took none — only when the arriving stamp is the
later one, so a settled cluster pays nothing. That commit is its own
transaction rather than the batch's, as every `_inner` on the DDL path is
(ADR-119), and it is sound to separate because the merged stamp is **monotone
and derived from the arriving entry alone**: a batch that fails after it has
committed leaves a value the same entry, re-delivered, computes again and does
not move. It is the one write on this path with no state to reconcile on a
retry. `create_index_inner` takes a `CreateOrigin` in place of its `log` flag —
the change `drop_index_inner` made in ADR-123, for the same reason, widened
only because a replicated *create* may carry no stamp — and returns an
`IndexCreated` so that "a later definition is already here" is a decision the
caller can see rather than an error. The comparison of two definitions moves
to `IndexMeta::differences`, which the create path and the supersede warning
share. A superseding create pays one index rebuild, which is what a create
costs anyway. An index whose entries are removed and rebuilt in one
transaction holds both in the write transaction briefly; the build already
did. Nothing is added to `/metrics`, to the HTTP index listing, or to
`docs/openapi.yaml`: the creation stamp is a fact about replication, and
surfacing it would put a stamp a client cannot use in every index listing.

Defended by `a_replayed_drop_does_not_remove_a_newer_index_of_the_same_name`,
`a_drop_replayed_after_this_nodes_own_recreation_leaves_it_alone`,
`a_drop_that_follows_the_creation_it_names_still_removes_the_index`,
`a_drop_still_removes_an_index_that_carries_no_creation_stamp`,
`concurrent_definitions_under_one_name_settle_on_the_later_stamp`,
`the_loser_of_a_concurrent_creation_does_not_come_back_through_a_replayed_create`,
`a_rival_definition_with_no_creation_stamp_is_still_refused_and_counted`,
`a_definition_that_wins_the_stamp_but_cannot_be_built_leaves_the_one_it_would_replace`,
`two_members_creating_one_identical_definition_converge_on_one_creation_stamp`,
`an_identical_definition_is_not_re_stamped_by_a_local_recreation`,
`an_unstamped_index_learns_its_stamp_from_the_peer_that_has_one`,
`a_losing_definition_is_not_re_served_to_a_third_member`,
`a_snapshot_definition_under_a_taken_name_settles_on_the_later_stamp` and
`a_creation_stamp_crosses_the_replicated_bson_boundary`, beside ADR-123's own
`a_replayed_index_that_cannot_be_built_does_not_stop_the_entries_behind_it`,
which is unchanged.

---

## ADR-133 — A periodic cross-member check makes a divergence no counter can express visible, without repairing it

> **Amended by [ADR-145](#adr-145--the-count-half-compares-against-a-peer-that-is-behind-but-standing-still-and-the-divergence-gauge-says-how-old-its-reading-is).**
> Defect 2's gate below — the count probe is dropped whenever the peer has
> not yet witnessed something this node has — now drops it only while the
> peer is also *advancing*. A peer whose position behind this node has come
> back unchanged on three consecutive checked contacts is compared. The
> argument below is about ordinary lag, which moves on every round; a
> member whose inbound replication has stopped does not, and under the
> unamended gate its count was never compared for as long as it stayed
> stopped. The dropped probe is now counted, so a count half that never
> runs is visible. Everything else here stands, the cap-truncation skip
> included.

**Decision.** Every anti-entropy round whose pull reaches the peer's true
tail — whether because there was nothing left to pull, or because this
round's own batch was not truncated by the cap — also asks that peer what it
holds, on the connection already open for that round: every collection id,
and the live document count of one collection named in the request. The
requester compares the peer's answer against its own state, subject to a
second guard against the peer's own lag (below), and exports
`kimmy_sync_divergent_collections`, a gauge, once a collection has been
found divergent twice running — two consecutive *contacts* with the same
peer, for the existence half; two consecutive *probes of that collection*
against the same peer, for the count half, which are not the same thing (see
defect 5 below). No document or collection name appears in the metric — the
gauge is a bare count, holding `/metrics`' standing property that a name
never crosses that boundary.

This ADR went through four rounds of review, each re-running every probe
against the fix rather than reading an account of it. The first found four
defects in the first cut, three of them gauge-defeating; the second found a
fifth in exactly the half the first round's fixes did not touch; the third
found a sixth in exactly the case the second round's fix did not sweep for;
the fourth found a seventh in the one input the third round's own fix was
not written to handle. What follows is the corrected design; the defects and
the reasoning behind each fix are recorded under their own headings because
each is a decision worth being able to find again, not just a bug that got
fixed.

**What is compared, and why not everything a full reconciliation would.**
Two things:

- **Collection existence**, one-directional: only "the peer holds it and I
  do not" is reported, never the reverse. The reverse is the peer's own
  discovery to make when its own loop reaches the identical gate pulling
  from this node; checking both directions from one side would flag a
  collection the instant it is created locally, before the peer has had any
  chance to catch up, which is the flapping the gate below exists to
  prevent. This half costs a metadata scan — the database and collection
  tables, never a document — so it runs on every check regardless of data
  size, and it needs no guard beyond the gate that gets a caller into the
  check at all.
- **One collection's live document count**, chosen in turn from the
  requester's own collection list (`next_probe`), so a check pays for at
  most one collection's scan rather than the whole database. Names alone
  would have missed the more serious half of what finding 14 actually lost:
  two collections were missing entirely, but 500 and 517 documents were also
  missing from collections that existed, correctly named, on every member.
  This half needs its own guard, below — the existence half's protection
  does not extend to it.

**Defect 1: the check went dark under sustained write load, which is
finding 14's own precondition.** The first cut ran the check only in the
branch where `VersionVector::behind` read `None` — nothing left to pull.
Under a continuously busy cluster (a modest 50 writes between each of 20
rounds, in the case that found this), every round has *something* new to
pull, so `behind` never reads `None` and the check never runs: 20 rounds, 0
reaching the branch, gauge pinned at 0 for the whole run. Finding 14's own
precondition was "the puller is more than one batch behind" — the corpus
load that produced it was the exact shape this defect went blind for.

**Fix: `sync_once` also runs the check whenever a round's own pull reaches
the peer's true tail**, using the `exhausted` flag ADR-127 already computes
and had been discarding after `apply_peer_batch` consumed it —
`SyncOutcome` now carries it. A round pulling a handful of new entries well
under the 1,024-entry batch cap reaches the tail and is checked; a round
whose pull is itself truncated by the cap is not, because that is precisely
the state a truncated window can fake without it being true, and checking
on the strength of a truncated pull would reopen the same hole one level up.
This closes the probed gap — the busy-cluster case above now confirms
within a handful of rounds — without opening the excluded one: a backlog
that stays deeper than one batch on every single round is still not
checked, honestly, because nothing about that state can be trusted either.
`kimmy-cluster/tests/replication.rs` pins both halves of the boundary:
`a_round_that_does_not_reach_the_peers_tail_skips_the_check_entirely` and
`a_round_that_reaches_the_peers_tail_runs_the_check_and_finds_nothing_wrong`,
plus `the_check_still_runs_while_a_round_keeps_finding_new_entries_to_pull`
reproducing the original busy-cluster probe against the real replication
loop.

**Residual, stated rather than hidden: a backlog that never dips under the
batch cap is still not checked, for as long as that holds.** A `0` reading
on `kimmy_sync_divergent_collections` during sustained heavy write load —
the 43-batch corpus load that finding 14 stopped on, at forty times the
volume, would be exactly such a load — means *not checked*, not *not
divergent*. The gauge degrades gracefully with backlog depth rather than
going fully dark the moment anything is pulled, which is what the first cut
did; it does not claim coverage a genuinely saturated cluster cannot afford.

**One state that `exhausted` alone cannot resolve: an empty, non-exhausted
window.** ADR-126's own proof — over every arrangement of withheld entries
and every batch limit — is that a correct sender can never answer with zero
entries while also reporting its tail was not reached: the scan that
produces `exhausted == false` only stops there after pushing at least one
kept entry. A peer that does both is not offering an ordinary capped pull
with nothing new to add; it is claiming, in the same message, both "nothing
here" and "more exists", which nothing downstream can safely read as
progress. Folding it into the same "not exhausted, do not check" bucket as
a genuine capped pull would make a malfunctioning or malicious peer's claim
indistinguishable from an unremarkable one. `sync_once` treats it as a
malformed round instead — the same failure class `expected Entries, got
...` already is — so it surfaces in `kimmy_sync_failures_total`, not as a
silent skip and not as a clean reading.

The counter alone is not the whole answer, though: the generic per-peer
failure debounce that decides whether a round failure is worth a `warn!`
line can route this occurrence's first sighting to `debug` if the same peer
already had an unrelated failure recently, leaving a bare counter increment
with nothing explaining it — for a condition whose entire premise is that it
should never happen at all. `sync_once` logs it at `warn!`, unconditionally,
at the point of detection, rather than leaving it to that debounce. The
predicate this depends on — `entries.is_empty() && !exhausted` — is a named,
tested function (`is_unreachable_from_a_correct_sender`) rather than an
inline condition, specifically because the shortcut `!exhausted` reads as
equivalent and is not: it would refuse every ordinary capped pull on a busy
cluster, the common case this state must be kept apart from.

**Defect 2: a peer merely behind was flagged, and the two-contact
confirmation does not filter it out.** The gate above protects this node's
own belief that it is not behind the peer. It says nothing about the
opposite direction: whether the *peer* is behind *this node*. A peer that
has simply not yet pulled this node's own recent writes answers a probe with
a stale, lower count for any collection those writes touched — a real
difference, but ordinary replication lag, not a divergence — and
`compare`'s count check is symmetric, so it reports the mismatch regardless
of which side is stale. Worse, a lagging peer reproduces the identical
mismatch on *every* consecutive contact, so the two-contact confirmation
that exists to filter out a one-off race does nothing here: on the round
whose peers steady-state lagged 130–260 s behind each other, this would have
been 26 to 52 consecutive confirmed ticks of a firing alert on a cluster
that was, at the time, healthy. An alert that cries wolf on ordinary lag is
the failure mode that gets an operator to disable it, which lands the
cluster in exactly the state this gauge exists to prevent.

**Fix: `divergence_probe_for` checks the reverse direction explicitly**
before trusting a count. Given `theirs` (the peer's version vector, fetched
at the top of the round) and `mine` (this node's own vector, as of just
before asking), `theirs.behind(&mine).is_some()` means the peer has not yet
witnessed something this node has — the peer is behind — and the probe is
dropped for that contact (`None`) rather than compared. The existence half
is unaffected: it never depends on the peer being caught up on anything of
this node's, only on this node's own belief about the peer, which the
existing gate already covers. Pinned by
`a_peer_that_has_not_pulled_this_nodes_own_writes_is_not_flagged_divergent`,
constructed with a real, genuine count difference across the wire that the
guard must suppress rather than report.

**Defect 3: above roughly twice the fanout, the gauge could never leave
0.** The first cut's confirmation state was one global pending/confirmed
pair, folded in once per *replication tick* from the union of every peer
reached that tick. `PeerHealth::select` hands each tick a `fanout`-sized
window of the known peers and advances it, so two ticks in a row share a
peer only while `2 × fanout > N` (peers) — with the default fanout of 3,
five members or fewer. Past that, no single peer is ever the one reached on
two consecutive ticks, so a *global* tracker keyed by tick can never see the
same peer's finding twice running, however badly that peer has diverged: at
six peers and up the gauge is structurally pinned at 0. The three-member
cluster the check was built and tested against could not have shown this.

**Fix: `DivergenceTracker` is keyed per peer**, not per tick.
`observe(peer, seen)` folds in one peer's finding from one contact with
that peer; confirmation needs the same peer's *own* two most recent
contacts to agree, however many other ticks or other peers fall between —
which `PeerHealth::select` eventually guarantees for every known peer
regardless of cluster size. A finding against one peer clears only when a
later contact with that *same* peer no longer sees it; a tick in which a
peer simply was not contacted leaves its last state untouched, because
silence about a peer is not evidence it has reconciled. Pinned by
`divergence.rs`'s
`confirmation_survives_ticks_where_the_peer_was_not_contacted_at_all` (the
fanout-scaling case directly), `a_peer_reconciling_does_not_clear_a_different_peers_finding`
and `the_same_collection_confirmed_against_two_peers_counts_once` (the
gauge answers "how many collections", not "how many peer pairs").

**Defect 4: the load-bearing test's silence half was vacuous.** The
original regression test asserted `lag_ms`, `applied`, `superseded`, `ddl`,
`ddl_refused` and `unknown_collection` were all zero on the branch under
test — but every one of those fields is `Default::default()` on that
branch's return path regardless of input, so the assertions could not fail
for any input and proved nothing about the "false belief reads as healthy"
claim they were named for. Finding 14's silence was specifically in the
*exported* metrics and the *absent* log lines, which live in `kimmy-cluster`'s
`peers.rs` and are pushed into `kimmy-api`'s `Metrics` by `kimmyd`; a test
against `sync_once`'s return value alone cannot reach either.

**Fix, two parts.** The mechanism-level test
(`the_divergence_check_finds_a_collection_the_witness_wrongly_claims_to_cover`)
now asserts only what it can actually prove — that `outcome.divergent` names
the stranded collection — and says so in its own comment rather than
implying more. A new test,
`the_replication_loop_reports_a_stranded_collection_while_every_other_signal_stays_healthy`,
drives the real `replicate()` loop with `on_round` and `on_lag` wired up —
the exact hooks `kimmyd::spawn_cluster` feeds into `Metrics::record_sync_round`
and `Metrics::set_replication_lag_secs` — and asserts, across every report
received while waiting for the gauge to confirm, that `failed`,
`backing_off` and `ddl_refused` stay at 0 and every `on_lag` reading is 0.
That is the actual signal path finding 14's silence was observed on, tested
directly rather than through a proxy that could not carry the claim.

**Defect 5: a count divergence could never confirm on any node holding more
than one collection.** A second round of review, having re-run every probe
against the fix for defects 1–4 rather than reading the account of them,
reproduced finding 14's more serious half directly — a document-count
divergence in a collection that exists, correctly named, on every member,
with no local writes on the affected member and a quiet, converged
cluster — and found the check detected it correctly on every single contact
in which the collection was probed, while the gauge never moved. At one
collection the confirmation worked; at two it stopped working entirely, and
stayed broken at every collection count above that.

The cause was the same shape as defect 3, one axis over. Defect 3's fix
keyed `DivergenceTracker` per peer, so a finding is not cleared by a contact
that did not examine that *peer*. But the count half is also gated by
`advance_probe`, which rotates to a *different collection* on the very next
contact — so on a node holding N collections, a given collection's count is
probed against a given peer roughly once every N contacts, not on every
contact the way existence is. The tracker's confirmation rule, "two
consecutive contacts", is correct for existence, which is checked in full
every contact, and wrong for count, which is checked for exactly one
collection per contact: a count finding was visible on one contact in N and
absent from the "seen" set on the other N−1, so it could never appear on two
*consecutive* contacts once N exceeded one. Detected on schedule, confirmed
never — which means the gauge reported existence divergence only, and the
count half — the half ADR-133 itself named as the reason to compare document
counts at all, "the more serious half of what finding 14 actually lost" —
was never wired to the gauge on any cluster with more than one collection.
The round that motivated this ADR had roughly 57.

**Fix: the two halves are tracked, and confirmed, separately.**
[`compare`][crate::divergence::compare] now returns `Findings { existence,
count }` rather than one merged set, and `DivergenceTracker::observe` keeps
independent state for each: existence confirms across two consecutive
*contacts* with a peer, unchanged from defect 3's fix; count confirms across
that specific `(peer, collection)` pair's two most recent consecutive
*probes*, keyed and cleared independently of how many other collections are
rotated through in between, and independently of the existence state for the
same peer. `confirmed_count()` unions both, so a collection found divergent
by either half, or both, still counts once.

Two other shapes were considered and rejected. **Holding the rotation on a
divergent collection** until its finding resolves — never advancing
`advance_probe` away from it — would confirm faster, but a permanent
divergence, by definition, never resolves without an operator, so a single
permanently divergent collection would starve every other collection from
ever being probed again: a design that finds one problem by creating a
second, larger one. **Comparing every collection's count on every
contact** — the "one cursor walk" the round's own supervisor did by hand —
is the cost this design exists to avoid paying forever on a live cluster,
restated from the cost section below. Tracking the two halves apart, so each
confirms on the cadence its own check actually runs at, was the only
considered shape that fixes the confirmation without reopening either the
starvation risk or the cost bound.

Pinned by `divergence.rs`'s
`a_count_divergence_confirms_despite_rotating_through_other_collections`
(the exact defect, at the tracker level), `a_gap_between_probes_of_the_same_collection_never_confirms_a_count`,
`a_contact_that_does_not_probe_the_collection_leaves_its_count_state_untouched`
and `a_confirmed_count_divergence_clears_on_the_next_clean_probe_of_it`; by
two composition tests added specifically because this defect and defect 3
share their shape — `kimmy-storage`'s own unit tests already pinned `observe`
correctly in isolation, and still missed this — `divergence.rs`'s
`a_count_divergence_confirms_despite_rotating_through_other_collections`
drives the real `advance_probe` against the real tracker, and `peers.rs`'s
`a_peer_confirms_despite_a_fanout_smaller_than_the_cluster` drives the real
`PeerHealth::select` against it; and by
`kimmy-cluster/tests/replication.rs`'s
`a_count_divergence_confirms_through_the_real_loop_despite_other_collections`,
which reproduces the original P6 probe end to end against the real
`replicate()` loop with four collections in rotation.

**Defect 6: a confirmed count finding against a since-dropped collection
never clears.** A third round of review, verifying the defect 5 fix rather
than reading the account of it, found the one case its own design left
uncovered: drop the divergent collection, and the gauge stays at its
confirmed value for the rest of the process, a hundred clean ticks later. The
cause is a straightforward asymmetry, not a new mechanism. `observe`'s
existence half is recomputed from a fresh `existence` set on every contact —
`confirmed_for_peer.retain(|id| existence.contains(id))` — so a collection
absent from that set is dropped immediately, no matter why it is absent. The
count half has no equivalent: it only ever mutates inside `if let Some((id,
mismatched)) = count`, so once `advance_probe` stops naming a dropped
collection, nothing ever touches its `count_pending` or `count_confirmed`
entry again — not a peer going quiet, which is genuine silence the design
deliberately does not read as reconciliation, but a fact this node already
knows for certain, in the same set `advance_probe` is handed fresh every
tick.

This mattered more than its size suggested, because the realistic trigger is
an operator following the gauge's own advice: it fires, they investigate,
they remediate the way `operations.md` says to — reset or recreate the
collection — and the alert they just resolved never goes out. Both this ADR
and `operations.md` promise the opposite in as many words: *"a resolved
divergence stops moving it rather than leaving a permanent scar."* An alert
that cannot be cleared by fixing the thing it reported is the one an
operator disables, which is precisely the argument defect 2's fix already
made at length.

**Fix: `advance_probe` sweeps both count structures against `mine` before
choosing the next probe.** It already receives the full, current set of
collections this node holds on every call; a collection missing from that
set is provably gone, not merely unheard from, so `count_pending` and
`count_confirmed` are filtered against it unconditionally, every tick,
regardless of whether that tick's probe lands on the affected collection at
all. The same sweep closes a second, subtler case: `CollectionId` is derived
from `(db, name)`, so a drop followed by a recreation of the same name
reuses the identical id, and without the sweep a *pending* (not yet
confirmed) mismatch against the old incarnation could combine with one
mismatched probe of the new incarnation to falsely confirm on what is really
each incarnation's first sighting.

Pinned by `divergence.rs`'s `a_confirmed_count_divergence_clears_when_its_collection_is_dropped`
and `a_pending_count_mismatch_does_not_survive_a_drop_and_recreate`.

**Defect 7: defect 6's own sweep, on the one input it was not written for,
silently discarded every live count finding.** A fourth round of review
found this: `peers.rs` handles a failed `engine.all_collection_ids()` by
logging and passing an empty set into `advance_probe`. Before defect 6 that
was harmless — an empty set just made the rotation return `None` for this
tick. After defect 6, `advance_probe` sweeps its count-side state against
whatever it is handed on the premise that a collection missing from that
set is provably gone; an empty set now reads as "this node holds no
collections at all", and every confirmed and pending count finding was
swept away on a single transient storage error. Self-healing — the existence
half is untouched, and a genuine finding re-confirms once its collection is
probed twice more, on the order of `2 × (collection count)` ticks — but
still a silent under-report inside the one feature whose whole thesis is
that a divergence must never be silent, introduced by the very fix meant to
stop the gauge from sticking. The comment at the call site had said
"skipping this tick's probe rotation" throughout; after defect 6 it no
longer skipped, it reset, and the comment did not change to say so.

**Fix: a read failure never reaches `advance_probe`.** `advance_probe_on`
holds the decision on its own — `Ok(ids) => tracker.advance_probe(&ids)`,
`Err(_) => None` — so a failed read leaves the rotation's cursor and every
piece of count state exactly where they were, and the next tick's read gets
a clean attempt. Pulled into its own function for the same reason
`is_unreachable_from_a_correct_sender` (defect 6's must-fix) was: the
interaction is exactly the part a future edit is likely to touch by
accident, and it is cheap to give it a test independent of the async loop
and the engine around it. Pinned by `peers.rs`'s
`a_read_failure_leaves_confirmed_count_findings_untouched`.

**Confirmed on two consecutive checks, not one — "consecutive" meaning a
different thing for each half, per defect 5.** The gates above already rule
out ordinary lag in both directions, but the peer's answer is still built
from two separate reads a message apart — its version vector, read by the
round already under way, then its collection list and probe count, read a
moment later on the same connection. A collection created on the peer in
that gap can in principle outrun the vector the gate was judged against.
Requiring the same finding to recur before it counts closes that window: a
live divergence recurs every time it is checked, because nothing here
repairs it, and a race between two reads a message apart does not recur
back to back. For existence that means two consecutive *contacts* with a
peer, because existence is checked in full on every contact; for count it
means two consecutive *probes of that collection* against that peer, which
can be many contacts apart once more than one collection is in rotation.
The gauge is a level either way: a resolved finding clears the moment its
own next relevant check — a contact, for existence; a probe of that
collection, for count — no longer sees it, rather than leaving a permanent
mark for a cluster that has since been fixed by hand.

**Cost, stated as a bound — corrected.** Per contact with a peer, in the
branch where the check runs: one metadata scan of this node's own database
and collection tables (independent of collection size), one metadata scan
on the peer's side, and exactly one collection's document count on each
side — never more than one collection, and never the whole database, *per
contact*. The first cut of this ADR stated the per-round total as "exactly
one collection's document count on each side... never more", which is true
per peer but not per round: a node with several pulling peers runs one such
exchange per peer it contacts that round, since each requester's rotation
is independent. The total is still bounded by the number of peers a tick
contacts (itself bounded by `cluster.fanout`), and still independent of
total data size — a node does not scan more of one collection because it
has more peers — but it is not literally one scan process-wide.

**A known, deliberately deferred cost inside that one scan.** `Engine::count`
walks `for_each_doc`, which decodes every document's record — including the
body — purely to increment a counter; it does not stop at a key-range scan
that would skip the body. That cost already existed everywhere `count`
already runs (the `count` route and aggregation's `$count`), and this check
does not add a new instance of it, but it does put it on a five-second
forever loop on both sides of a probed contact, which those call sites do
not. A key-range count would cost a fraction. Left as measured and not
fixed here: the change belongs to `Engine::count` itself, not to this
check's use of it, and reworking a shared primitive is a larger unit than
adding one caller of it.

**What this does not do.** It does not repair a divergence it finds — a
member found to hold less than its peers still needs an operator to decide
what to do about it, exactly as any other divergence this cluster can
report does. Automatically resetting or re-seeding a member on this signal
would be a much larger decision than "make the state visible", and design
rule 4 for this round is explicit that a bug fix does not get to make that
call quietly. It is recorded here as a considered alternative and left to a
future round if wanted.

**What it cannot catch**, stated plainly against what the code actually
does rather than against what an earlier draft of this ADR claimed:

- A document present in equal numbers on every member but with different
  content — a lost update that still counts, rather than a lost document.
- **A count divergence, until the affected collection has been probed twice
  running against the same peer** — not merely probed once, which detects it
  but does not confirm it, and does not move the gauge. Reaching a given
  collection's turn at the probe once takes as many *checked* contacts with
  that peer as the cluster has collections; confirming it takes reaching
  that same collection's turn twice in a row, with nothing in between that
  probes it *and* finds it clean. Other collections being probed against the
  same peer in between do not affect it either way — the two-in-a-row rule
  is scoped to the one `(peer, collection)` pair, per defect 5 above, which
  is what this bullet used to understate. See the next point for what
  "checked" excludes.
- **A round whose pull did not reach the peer's tail does not run the check
  at all** — see defect 1's residual above. A gauge reading `0` during a
  backlog that never drains under the batch cap is not evidence of
  convergence; it is evidence the check has not run.
- Anything on a peer this node's fanout is not currently pairing it with —
  bounded by `cluster.fanout`, the same bound anti-entropy itself is subject
  to.
- A collection this node holds that a peer does not — by design, that
  direction is the peer's own check to make when its own loop reaches the
  same gate pulling from this node.
- A shadow collection (a vector index's own storage) stranded on one member.
  `Engine::all_collection_ids` excludes shadow collections from the
  existence and count comparisons, because their lifecycle deliberately
  trails the collection they serve; a genuine divergence in one is as
  invisible to this check as it is to the collection listing routes.

**A second wire change in this release, weighed against not making one.**
This is the *second* protocol addition U1 (ADR-126/127) shares the release
with, and rule 4 asks for the alternative to be named rather than skipped
over. The alternative was deriving existence and counts from data the wire
already carries — `AskVersions`/`Versions` and `Entries` — and it does not
work: a version vector is per-origin high-water marks, not a collection
list or a document count, and no combination of history already exchanged
recovers either without replaying it, which is exactly the cost this design
exists to avoid paying every round. `AskDivergence`/`Divergence` is new
messages for a genuinely new question the existing wire cannot answer.

Unlike `Entries`, which fails loudly and immediately on a version mismatch
(a malformed frame, counted in `kimmy_sync_failures_total`, on the very
first round after a mixed-version pair meets), an `AskDivergence` mismatch
only surfaces on a round that reaches the branch this check runs in — a
converged round, or one whose pull is exhausted. A freshly mixed pair still
actively catching up looks completely healthy; only once it converges does
every subsequent round to the unstamped peer fail. Operationally this still
resolves the same way U1's cutover does — roll every member — but the
failure's onset is delayed and worth stating rather than assuming it
matches `Entries`' shape.

**Cost of the alternative considered: compare every collection's count every
round.** This is what the round's own supervisor did by hand to confirm the
loss — "one cursor walk" — and it is exactly what is unaffordable run
forever on a live cluster: the cost scales with total document count across
every collection, on every round, on every member, which is the walk this
design declines to automate. The rotation trades detection latency —
bounded by collection count on a cluster whose backlog stays under the
batch cap, unbounded while it does not, per defect 1's residual above — for
a bound independent of data size.

Defended by `crates/kimmy-storage/src/divergence.rs`'s unit tests —
`a_collection_the_peer_holds_and_this_node_does_not_is_divergent`,
`a_collection_only_this_node_holds_is_not_reported_here`,
`a_disagreeing_probe_count_is_divergent_even_with_matching_names`,
`a_caller_that_distrusts_the_probe_suppresses_only_the_count_half`,
`a_gap_against_the_same_peer_never_confirms`,
`a_confirmed_divergence_clears_the_moment_that_peer_no_longer_shows_it`,
`confirmation_survives_ticks_where_the_peer_was_not_contacted_at_all`,
`a_peer_reconciling_does_not_clear_a_different_peers_finding` and
`the_same_collection_confirmed_against_two_peers_counts_once` — and by
`crates/kimmy-cluster/tests/replication.rs`'s
`the_divergence_check_finds_a_collection_the_witness_wrongly_claims_to_cover`
(the mechanism, honestly scoped),
`the_replication_loop_reports_a_stranded_collection_while_every_other_signal_stays_healthy`
(the actual silence claim, against the actual hooks),
`the_check_still_runs_while_a_round_keeps_finding_new_entries_to_pull`
(defect 1's probe, reproduced and closed),
`a_peer_that_has_not_pulled_this_nodes_own_writes_is_not_flagged_divergent`
(defect 2's probe, reproduced and closed), and
`a_round_that_does_not_reach_the_peers_tail_skips_the_check_entirely` /
`a_round_that_reaches_the_peers_tail_runs_the_check_and_finds_nothing_wrong`
(the exhausted boundary, both sides).

---

## ADR-134 — A tagged variant that takes no configuration carries an empty body, so an unknown key beside it is refused

**Decision.** `ProviderConfig`'s `byo` variant is declared `Byo {}` — an empty
struct body — in both `kimmy_core::vector_meta::ProviderConfig` and its
request-only mirror `kimmy_api::vectors::ProviderConfigInput`, in place of the
unit variant it was. `#[serde(deny_unknown_fields)]` is enforced by the
field-matching visitor the derive writes for a variant's *body*; an
internally-tagged unit variant has no body, so the deserializer written for it
consumes whatever content is left beside the tag and discards it rather than
matching any of it against a field list, and the attribute both enums have
carried since they were written did not reach the one variant that took no
fields. Empty braces give it a body to enforce against. The rule
that generalises, for every enum after this one: **a variant of an
internally-tagged enum that takes no configuration is written `V {}`, never
`V`** — the braces are what put it under the enum's own closure.

**Why.** `{"kind":"byo","nosuch":1}` on `POST /v1/db/{db}/coll/{coll}/vector`
answered `200` and configured the collection, having read `nosuch` and thrown
it away; the same body under `"kind":"open_ai"` answered `422` naming the
field. That is ADR-121's own defect, surviving inside the one route ADR-121
cites as having been correct all along — and `byo` is the *default* provider,
so the kind most likely to be configured was the kind that was open. A client
cannot tell from a `200` that the server did not honour what it sent; that is
the whole of ADR-121's argument, and it applies here unchanged. The refusal is
now serde's own — *unknown field `nosuch`, there are no fields* — in the
standard envelope, and it falls only on a request that was already wrong.

The same hole was open at the operator's end. `[vector.providers.<name>]`
profiles deserialize into a `BTreeMap<String, ProviderConfig>` in `kimmyd`'s
configuration, so a profile written `kind = "byo"` with a misspelt key beside
it started the node, while every other `kind` refused to start and named the
key it did not know. A file that is parsed at startup, and before that by
`kimmyd check-config`, exists so a typo surfaces there rather than as
behaviour nobody asked for; one variant silently exempt from that is the
worst place for it.

**Both types, not only the request mirror.** ADR-128 reserves
`ProviderConfigInput` for refusals the stored form cannot bear, and that
constraint is real — but it is about `null`-versus-absent, where the stored
shape has always written a literal `null` for an unset defaulted field and
refusing it would stop a node reading its own configuration. An unknown field
is not that case: `deny_unknown_fields` has been on
`kimmy_core::ProviderConfig` since it was written, and seven of its eight
variants have always enforced it — on the request, on the replication wire, on
disk and in the profile map alike. Closing only the mirror would put the
eighth variant's closure somewhere other than where the other seven live,
which is where the next reader loses the thread, and would leave the
`[vector.providers.<name>]` map open, since that map reads the core type and
never the mirror.

**No shim, and no roll ordering: the encoded form does not move.** `Byo` and
`Byo {}` encode identically, measured against this workspace's own dependency
versions (serde 1, serde_json 1, bson 3, toml 1) rather than assumed:

- JSON — both serialize to `{"kind":"byo"}`, and `Byo {}` reads back a record
  written by `Byo`.
- BSON — `bson::serialize_to_document` yields
  `Document({"kind": String("byo")})` for both, and `Byo {}` deserializes a
  document produced by `Byo`. That covers `VectorSet` on the replication wire
  and `CollectionMeta` on disk.
- TOML — `kind = "byo"` parses into both, which is the profile map.

So nothing stored needs rewriting, a mixed-version cluster exchanges the same
bytes it did before, and there is no order in which members must be rolled.
Nor is there an upgrade hazard from the keys that used to be accepted: they
were *ignored*, never stored, so no `byo` record on disk carries one for the
stricter type to trip over. The only behaviour that moves is the one being
fixed.

**Cost.** Breaking, for a request or a profile that was already wrong. A `POST
.../vector` body carrying an unknown key beside `"kind":"byo"` answers `422`
where it answered `200`, and a `[vector.providers.<name>]` profile carrying
one stops the node at startup rather than running with a setting nobody wrote.
A `byo` configuration with no stray key is unaffected in every direction.
`docs/openapi.yaml` still cannot mark `ProviderConfig`
`additionalProperties: false` — `GET .../vector` and `describe` return it, and
ADR-121's rule keeps a returned schema open so a new response field stays
additive — so the refusal is stated there in prose, the way `VectorConfig`'s
own already is, and `docs/vectors.md` says it beside the provider that has no
fields to give.

---

## ADR-135 — The divergence gauge reports whether it looked, and ADR-133's cap-truncation skip stays because a backlog fakes exactly what removing it would report

> **Amended by [ADR-145](#adr-145--the-count-half-compares-against-a-peer-that-is-behind-but-standing-still-and-the-divergence-gauge-says-how-old-its-reading-is).**
> Three points below are revised. A round that *failed* is now counted as
> `skipped` as well as in `kimmy_sync_failures_total`, so `ran + skipped` is
> every round attempted and "both flat" no longer describes a member whose
> every round fails. The "seconds since the last check" gauge rejected under
> alternatives is added, as `kimmy_sync_divergence_check_age_seconds`, on
> terms that answer the objection: the never-checked case reads `0` beside
> a `ran` counter that also reads `0`, and it is the counter, not the gauge,
> that carries that case. And the count half's own suppression, rejected as
> a third `outcome` on this ADR's series, is counted as a series of its own,
> `kimmy_sync_divergence_count_probes_total{outcome="compared"|"deferred"}`,
> for the case the rejection did not weigh — a peer permanently behind, on
> which the count half never ran and nothing said so. The no-peer-label
> rule, the cap-truncation skip and the fold-then-count rule all stand.

**Decision.** Two things, one added and one deliberately left alone.

*First*, `/metrics` gains `kimmy_sync_divergence_checks_total`, a counter
split by a closed two-value outcome: `ran` counts contacts with a peer in
which ADR-133's cross-member check actually ran, `skipped` counts contacts
whose round completed without running it because the pull was truncated by
the batch cap. Both are counted per **contact**, not per tick, so a tick
reaching several peers contributes one to one of the two per peer. A round
that *failed* is counted in `kimmy_sync_failures_total` and in neither of
these, so `ran + skipped + failures` accounts for every peer a node
contacted. Neither series carries a peer label, or any other label but
`outcome` — see below.

*Second*, ADR-133's rule that **a round whose pull was truncated by the
batch cap does not run the check at all** stands, unchanged, and the worked
example below is recorded as the evidence for it rather than against it.

**Why the gauge needed a companion at all.** `kimmy_sync_divergent_collections`
reading `0` means two different things — *checked, and this node and the peer
agree* and *not checked* — and nothing an operator can scrape tells them
apart. ADR-133 states the residual in as many words ("a `0` reading during
sustained heavy write load means *not checked*, not *not divergent*") and
`operations.md` repeats it, but stating a limit in prose does not give an
alert rule anything to condition on. The rule the reference actually offers
is "alert above 0", and that rule is silent in exactly the state it most
needs to speak: the gauge is pinned at its healthy value and the operator has
no way to learn that nothing is looking. A counter of the checks that ran is
the smallest thing that makes the distinction observable, and it does it
without touching what the check does or when it runs.

**The worked example: 245 seconds during which the gauge read `0` on every
member, and was right to.** Observed on a three-member cluster running
0.22.0, under six concurrent writers. For 245 seconds across 49 consecutive
sampler rows, one member's `kimmy_collections` read 38 against 33 on the
other two, while `kimmy_sync_divergent_collections` read `0.0` on all three
members in every sample, `kimmy_replication_lag_seconds` climbed from 80 to
265 s at wall-clock rate on all three, and `kimmy_sync_failures_total` did
not move. Read at the endpoints that looks like the exact shape ADR-133 was
built for — a member holding collections its peers do not, with every other
health signal flat — and the first reading of it was that a real divergence
had been slept through, with a proposal to remove the cap-truncation skip so
that the cheaper half of the check (comparing collection ids) would still run
on a backlogged round.

**That reading was wrong, and this window is the case *for* the skip.** The
five collections were created on the third member and had simply not yet
arrived at the other two; they arrived at 219 s and 227 s and the gap closed
by itself with no operator action. So from either lagging member's side the
state was "the peer holds collections I do not" — which, on a pull the batch
cap truncated, is indistinguishable from "I have not yet applied the entries
that create them here." It was the second. The existence half is
one-directional by design (ADR-133: only "the peer holds it and I do not" is
ever reported), and that is precisely why a truncated window can fake it:
this node's belief about what it holds is complete, and its belief about what
it has *yet to apply* is exactly what a truncated pull leaves unsettled.

**And the confirmation gate would not have absorbed it.** Two consecutive
contacts with the same peer, at the default `cluster.sync_interval_secs` of
5 s, is roughly 10 seconds. The window was 245. An unskipped existence half
would have confirmed inside the first 2% of it and held the gauge above zero
for the remaining four minutes, on a healthy cluster doing nothing worse than
draining a corpus load — and would have done it again on every wave of every
such load. That is the flapping the gate exists to stop, and ADR-133's own
argument for the skip — "that is precisely the state a truncated window can
fake without it being true, and checking on the strength of a truncated pull
would reopen the same hole one level up" — is not conservatism but the
correct call, now with a measured demonstration behind it. Defect 2 of
ADR-133 already made this argument at length about ordinary lag and the count
half; this is the same argument, on the same cluster, about the existence
half. **The proposal to remove the skip is withdrawn.**

**What survives is narrower, and is what this ADR fixes.** Not "a divergence
was missed" — none was. What is true is that *during* a sustained backlog the
check does not run, cannot say so, and a genuine divergence created in that
window would be invisible for as long as it lasted; and that a backlog is a
plausible time for one to arise. The honest fix is not to make the check run
where it cannot be trusted. It is to make the blindness visible, so that
"quiet" and "blind" stop looking identical, and to prove the gauge can move
at all.

**Both outcomes, not one.** A checks counter alone answers "has the check run
recently" and leaves "why not" to guesswork; worse, a flat `ran` count is
ambiguous between a blind node and one with no peers, a node whose rounds are
all failing, and a process that has only just started. A skips counter alone
inverts the problem: rising means blind, but `0` is ambiguous between "every
contact was checked" and "nothing was contacted". Carried together, and read
beside `kimmy_sync_failures_total`, they account for every contact a node
made, so an operator can tell the four states apart without inferring any of
them. That is the whole point of adding a series here rather than a sentence
to the guide.

**One series with an `outcome` label, not two names.** Every counter in this
exposition that splits by a closed enumeration is written that way already —
`kimmy_responses_total{class}`, `kimmy_tls_reloads_total{outcome}`,
`kimmy_jwks_refresh_total{outcome}`, `kimmy_webhook_deliveries_total{outcome}`,
`kimmy_embed_provider_errors_total{kind}` — and `ran`/`skipped` is such an
enumeration: a completed round either ran the check or was truncated out of
it, with no third case and no case that can be added without a decision like
this one.

**No peer label, decided deliberately.** The finding this ADR answers
proposed counting checked rounds *per peer*. That is not done, for three
reasons, and the residual is stated rather than hidden.

- **Every label in this exposition is a closed set fixed at compile
  time** — `class`, `outcome`, `state`, `kind`, `le`. A peer label is open,
  and its members churn: every member ever replaced leaves a series that
  never receives another sample, on every other member, for the life of
  each process. Nothing else on this endpoint behaves that way.
- **It would put member identity on `/metrics`.** ADR-133 holds the standing
  property that no document or collection name crosses that boundary, and the
  gauge is a bare count for that reason. A node id is a different kind of
  name, and the argument does not transfer automatically — but the boundary
  is the same one, `/metrics` is unauthenticated by default and routinely
  shipped off the box, and per-peer facts already have a home behind
  authorization in `/v1/topology`, which is where the staleness record
  (ADR-085) went for the same reason. Extending the property rather than
  carving the first exception into it is the smaller decision, and this ADR
  takes it: **the two new series carry no name of any kind.**
- **It would not join to what it qualifies.** `kimmy_sync_divergent_collections`
  is itself an unlabelled level over peers — the gauge answers "how many
  collections", not "how many peer pairs", by ADR-133's own defect 3 fix. A
  per-peer breakdown of the diagnostic beside an aggregate of the thing it
  diagnoses gives an operator two series they cannot line up.

*Residual, stated:* the aggregate cannot single out one permanently truncated
peer among several that are being checked. A node whose `skipped` count is
rising at all has a backlog against *some* peer, which is enough to know the
gauge's `0` is not fully trustworthy.

Which peer is answerable, but **not from the sync warnings, and saying so
matters more than the reassurance an earlier draft of this paragraph
offered.** Neither warning can fire in this state, by construction. A
truncated round *succeeds*, so the `Err` arm's `sync round failed` never
runs. The stale-rejoiner warning fires on `outcome.behind_ms`, which
`behind_beyond_horizon` computes as how far the *peer* trails *this node* —
the opposite direction from a pull this node could not finish — so it stays
at roughly zero and silent, and `/v1/topology`'s stale-peer record is fed
from the same hook in the same direction. `kimmy_replication_lag_seconds`
does rise, and is a max over peers, so it says "some peer" and not which.
What answers it is the `cluster.sync` span: one span per peer per contact,
at info, carrying `peer` and `lag_ms`, which exists precisely because
"folding them into one span would lose which peer was the slow one — the
only thing anybody opens this trace to find out." A peer whose `lag_ms`
stays high round after round is the peer nothing is checking. That is a
weaker instrument than a labelled series — it is a trace rather than a
metric, and reading it is a step an alert cannot take on its own — and it is
the one this ADR is willing to pay for.

**Counted in the branch that folds, not beside it.** `sync_once` already
returns `divergent: Some(..)` when the round reached the peer's tail and
asked, and `None` when ADR-133's skip applied — including `Some` of an empty
set, which is the "checked, nothing found" case the gauge's `0` is supposed
to mean. The loop's two increments sit **inside the same two arms** that fold
the finding into `DivergenceTracker`, rather than in a sibling `if` reading
the same `Option`. A sibling would have been correct today and would have
been a second predicate to keep in step with the first: the fold is gated on
the peer having introduced itself as well, and a `divergent: Some(..)`
carrying no peer would have been counted as a check the tracker never
received. No such outcome exists — both places `sync_once` sets `divergent`
set `peer` in the same breath — but "the counter cannot report a check the
tracker was not told about" is a property worth holding structurally rather
than by an invariant asserted in one file and relied on in another.
`RoundReport` carries both per tick, on the same hook
`kimmy_sync_failures_total` and the gauge already ride.

**And the report crosses into `/metrics` whole.** `Metrics::record_sync_round`
takes `&RoundReport` rather than the six `u64`s it would otherwise have grown
to. Its only caller is a closure in `kimmyd::spawn_cluster` that no test
reaches — `kimmyd`'s own cluster tests scrape `/metrics` for three unrelated
series and are `#[ignore]`d besides — so a pair of same-typed positional
arguments transposed on that line would compile, satisfy every gate, and
publish one series' value under another's name until somebody read a
dashboard closely enough to disbelieve it. Passing the struct deletes the
category: the fields are named where they are read, the `usize` to `u64`
widening happens once inside the method instead of six times at the call
site, and a field added to the report cannot silently take another's place.
That `kimmy-api` may name a `kimmy-cluster` type is settled — it already
takes `kimmy_cluster::Members` in several signatures. The direction that is
deliberately *not* opened is the other one, `kimmy-cluster` knowing what a
caller does with a number: lag is pushed out through a callback on
`ReplicationConfig` rather than a metrics handle, the shape ADR-043 predicted
when it deferred the metric and ADR-046 recorded when it added one. Nothing
here moves that; the report still travels out through `on_round`, and
`kimmy-cluster` still has no idea a metric exists.

**A positive control, and why it belongs in an ADR rather than in a test
file.** Before this change the gauge had never been observed leaving `0` on
any cluster, and nothing in the suite drove it off `0` **and back**. A
detector in that state is indistinguishable from a dead one, and the correct
consequence — which this ADR records as a rule, not an aspiration — is that
**a `0` reading may not be cited as evidence of convergence by any test round
until a case exists that proves the gauge can move.** Two now do.
`the_divergence_gauge_leaves_zero_and_returns_to_zero_through_the_real_loop`
drives the real `replicate()` loop against a real peer over TCP: it waits for
the gauge to leave `0`, asserts it did not move until at least two contacts
had actually been checked (the existence half's confirmation rule, measured
through the new counter), then makes the repair `operations.md` prescribes
and waits for the gauge to fall back to `0`. Both directions are load
bearing and fail for different reasons: a gauge that never moves fails the
first, and a gauge that sticks fails the second — and a stuck gauge is the
worse defect, because an alert that cannot be cleared by fixing what it
reported is the one an operator disables, which is defect 6's argument
exactly. `one_checked_contact_is_pending_and_the_second_moves_the_gauge` pins
the same three transitions without timers, composing the real `sync_once` and
the real `DivergenceTracker` the way the loop does.

Each was checked against a deliberately broken gauge rather than assumed to
be sensitive: pinning the reported level to `0`, removing the clearing sweep
so a confirmed finding sticks, and confirming on first sight instead of on
two consecutive contacts each fail the control at the assertion written for
it. A positive control that passes against a dead gauge is worse than none,
because it converts an unknown into a false assurance.

**And the skip has a control of its own.**
`a_cap_truncated_round_counts_a_skip_and_never_a_check` drives a backlog
deeper than the batch cap through the real loop and asserts that the
truncated round was counted as a skip *before* any check was counted, and
that once the backlog drains nothing further is skipped. That ordering is the
whole operator-facing claim: if a truncated round could tick the `ran`
counter, the blind state would read as the healthy one and the new series
would be worse than nothing. Counting the truncated round as a check fails it.

**What this does not do.** It does not make the check run anywhere it did not
run before, does not change what is compared, does not change the
confirmation rules, and does not repair a divergence — ADR-133's deferral of
repair is untouched. It adds two sample lines and one branch per contact.

**Alternatives considered.**

*Give the gauge a third state instead of a second series* — a sentinel value
for "not checked". Rejected: a sentinel is a value every dashboard, every
alert expression and every aggregation has to be taught about, `sum()` over
members silently produces nonsense, and a gauge whose domain is "a count, or
one magic number" is exactly the ambiguity being removed, relocated. A
counter that has not moved is unambiguous without instruction.

*A "seconds since the last check" gauge*, which answers the alert question in
one series. Rejected on the case it cannot express: a node that has **never**
checked has to be given a value, and every available answer is wrong in a
different way — `0` reads as "just checked", the process uptime reads as a
stale check that once succeeded, and a very large number reads as a fault on
a node that simply has no peers. A counter at `0` says "this has never
happened here", which is the truth.

*Count the count half's own suppression as a third outcome.* The document-count
probe is dropped whenever the peer has not yet witnessed something this node
has (ADR-133, defect 2), and whenever the rotation has nothing to name. On any
busy healthy cluster that happens on most contacts, so the series would rise
in proportion to write traffic and mean nothing an operator could act on,
while the count half's real coverage limit is a latency bound — order twice
the collection count in *checked* contacts — that is a property of the design
rather than of a moment, and belongs in the guide where it already is.

*Remove ADR-133's cap-truncation skip so the existence half still runs on a
backlogged round*, on the grounds that comparing collection ids is the cheap
half. Rejected, at length, above: the 245-second window is a live
demonstration that a truncated pull manufactures exactly the finding the
existence half reports, that the confirmation gate is two orders of magnitude
too fast to filter it, and that the result would be minutes of firing gauge
per corpus load on a healthy cluster. Cost was never the reason for the skip
and removing it would not have found anything real.

*Do nothing, since every observed behaviour already matches the reference.*
It does — the members' `0` readings were correct throughout. Rejected for the
reason the gauge exists: the state it was built to make visible is one where
every other signal reads healthy, so "matches the reference" is precisely the
condition under which it must still be possible to tell a working detector
from a silent one. Leaving that unresolved would carry forward, in a new
form, the mistake of trusting a signal nobody had shown could move.

**Cost.** Two sample lines on `/metrics` and two `AtomicU64`s per process.
One extra `else` arm per peer contact, on a path that has just finished a
network round. Two more fields on `RoundReport`, and a changed signature on
`Metrics::record_sync_round`, which now takes the report by reference rather
than its fields one at a time. Nothing is added to the cluster wire, to
`/v1/topology`, or to `docs/openapi.yaml`: whether this node's own check ran
is a fact about this node's rounds, and no peer has any use for it.

Defended by `kimmy-cluster/tests/replication.rs`'s
`the_divergence_gauge_leaves_zero_and_returns_to_zero_through_the_real_loop`,
`one_checked_contact_is_pending_and_the_second_moves_the_gauge` and
`a_cap_truncated_round_counts_a_skip_and_never_a_check`; by
`kimmy-api`'s `the_render_is_byte_for_byte_what_a_scrape_receives`,
`the_snapshot_reads_the_same_atomics_the_render_does` and
`the_pushed_gauges_render_what_was_pushed`, which pin the two series' names,
labels, help text, position and counter semantics; and by
`kimmy-api/tests/api.rs`'s `the_metrics_body_exposes_exactly_these_series_in_exactly_this_order`, which
pins that they reach a real scrape in order. Beside them, ADR-133's own
`a_round_that_does_not_reach_the_peers_tail_skips_the_check_entirely` and
`a_round_that_reaches_the_peers_tail_runs_the_check_and_finds_nothing_wrong`
remain the boundary this ADR declines to move.

---

## ADR-136 — What a failed request logs is a property of its error code, and the property is actionability rather than HTTP class

> **Amended by [ADR-144](#adr-144--a-log-lines-event-name-and-its-message-are-two-fields-never-one-key-twice).**
> "One event message on every level" below stands as an argument and moves as
> a field: the `request failed` name is now written to an `event` field, not
> as the macro's format string. Written that way it *was* a field named
> `message`, beside the `message` field carrying the client-facing text, and
> the JSON layer wrote both. Read "event message" below as "event name".
>
> **Amended by [ADR-137](#adr-137--the-log-level-a-failure-can-ask-for-is-a-three-variant-type-not-tracinglevel).**
> Not superseded: every level below, and the reasoning for each, stands. What
> changed is the type they are written in. `log_level()` returns
> `Option<LogLevel>` and `level_override` holds one, where `LogLevel` is
> `Error | Warn | Info` — so what *"`None`, not a level the subscriber filters
> out"* below argues for is now held by the type: "quieter than `INFO`" is not
> a thing that can be written down, rather than a thing an assertion catches
> once it has been. Read `Level` below as `LogLevel`; `at_level` takes one too.

**Decision.** `ErrorCode` gains `log_level() -> Option<Level>`, a third
exhaustive match beside `as_str()` and `retry()`, and
`impl IntoResponse for ApiError` logs at that level instead of gating on
`status.is_server_error()`. `None` means the failure is not logged at all.
`ApiError` carries `level_override: Option<Level>`, set at construction where
the code alone cannot decide, and the level a request actually logs at is
`level_override.or(code.log_level())` — the same shape `retry_override`
already has over `retry()`.

The whole mapping:

| `error` | Status | Level | Whose |
|---|---|---|---|
| `internal` | 500 | `ERROR` | the operator's |
| `misconfigured` | 500 | `ERROR` | the operator's |
| `snapshot` | 500 | `ERROR` | the operator's |
| `timeout` | 503 | `WARN` | one or the other, and this node cannot tell |
| `provider_error` | 502 | `WARN` | an upstream's |
| `not_implemented` | 501 | `INFO`, or `ERROR` from one source | the caller's, or the operator's |
| every other code | 4xx | not logged | the caller's |

**Why.** A test round against 0.22.0 on a three-member cluster produced seven
`ERROR` lines across the whole round: four *coordinated unique enforcement is
reserved and not implemented*, one *an environment variable is not set, so the
provider has no API key*, and two *the request was not completed within 30
seconds and was abandoned*. Every one of them was a documented refusal that a
test case asked for on purpose, and every one of those cases passed. The
finding was not that the server did something wrong; it is that a single
client sending requests this API's own reference says will be refused writes
`ERROR` lines on whichever member refuses them. An operator alerting on error
lines — which is the first alert anybody writes, and the one
`docs/operations.md` is about to tell them to write — is paged by somebody
else's bad input.

**The discriminator is actionability, and HTTP class cannot express it.** A
`4xx` is a statement about the request and a `5xx` a statement about the
server, and neither answers the question a log level is for: **is the fix in
the operator's hands, or the caller's?** That question cuts *across* the 5xx
set. `501 not_implemented` for a reserved capability is a `5xx` no operator
can act on — no configuration turns it on, because the capability exists
nowhere. `500 misconfigured` is one only an operator can act on, and the
caller who tripped it has nothing to change.

This is not a distinction the status could have been made to carry, and the
reason is worth stating plainly because it rules out the obvious cheaper fix:
**4xx were already silent.** `status.is_server_error()` meant that a plan to
"move client-caused refusals to `INFO`" is a plan to change nothing — the
entire change lives inside the 5xx set, where the class is constant and
actionability is not. The status is the right answer to a different question
and stays exactly as it was; nothing on the wire moves.

**The substantive half is raising a case, not lowering one.** Read only as
"four noisy lines became `INFO`" this is a small tidying. The half that
matters is the opposite: `501 not_implemented` has a second source —
`VectorError::LocalUnavailable` / `ModelUnavailable` in `vectors.rs`, a node
that cannot build the local embeddings a stored vector configuration calls for
— and that is a member provisioned unlike its cluster. Every search of that
collection landing on it fails, the other members answer normally, and behind
a load balancer the failure is a fraction of requests with no member obviously
at fault. That is exactly the condition that should page, and until now it was
indistinguishable in the log from a caller asking for a feature that does not
exist. Blanket-lowering `not_implemented` to suit its commoner source would
have buried it. So it is *raised* — the only use of the override in the
server today.

**Why a per-instance override at all, given the enum is meant to be the single
source of truth.** Because a per-code property keys on the code, and
`not_implemented`'s two sources **share the code**. They share it
deliberately: a client cannot act differently on the two and should not be
asked to, which is why `retry()` gives them one conservative answer too (see
its comment). A property that must split them therefore cannot live on the
code alone. It does not need to be threaded, either — both sources are
explicit `ApiError::new` constructions, one in `error.rs`'s `CoreError`
mapping and one in `vectors.rs`'s `vector_error`, so the cause is still in
hand where the level is chosen and no call chain is touched. The default stays
on the enum, which is what keeps `operations.md`'s published list derivable
from the server rather than maintained beside it.

**`None`, not a level the subscriber filters out.** A 4xx could have been
given `DEBUG` and left below the default filter. It is `None` instead, because
"this is not a log event" is a property of the code and not of how the process
was started: `docs/operations.md` teaches `RUST_LOG=info,kimmy_storage=debug`
for debugging a running container, and an operator who raises the filter to
chase something unrelated should not thereby acquire a line per malformed
request. That would be an access log of nothing but the failures — half a
record, and one this server has never kept. The count is already in
`kimmy_responses_total{class="4xx"}` and the authorization decisions among
them are already in the audit log, neither of which costs a line per request.

**One event message on every level.** The three arms write the same
`request failed`, and the temptation to soften the INFO one to
`request refused` was refused. A wording that varies by level is a second
discriminator beside the level — one nothing publishes and no test pins — and
an operator or a log query grepping for one of them silently misses every line
written under the other. That is the same shape of trap this ADR exists to
remove, arriving through a different door. Severity is the level's job alone;
`code` is what says which failure it was.

**Each 5xx level, on its own terms.**

- **`internal` — `ERROR`.** A genuine fault: storage failed, or something that
  cannot happen did. Unchanged, and correctly loud.
- **`misconfigured` — `ERROR`.** An operator must set something. This member
  cannot build the provider a replicated vector configuration names while some
  other member could, which makes it a member configured unlike its cluster.
  Unchanged. Note it is silent until a caller happens to search that
  collection on this member, so the first occurrence is the whole warning
  anyone gets — one of the two lines the test round produced, and the one that
  was right to be loud.
- **`snapshot` — `ERROR`, decided here rather than carried over.** Asked the
  same actionability question directly, from what produces it:
  `VectorError::Snapshot` comes from `kimmy-vector`'s index snapshot I/O — an
  error writing under the snapshot directory, or a snapshot file whose
  metadata will not parse. All of that is this node's own disk, and no request
  body changes it, so the caller cannot be the owner. There is a second reason
  on top, from the mapping site's own comment: this is meant to be unreachable
  from a request path, because the cache discards a snapshot it cannot load
  and rebuilds the graph rather than letting the error escape. One reaching a
  response means that absorption did not happen — a fault in this node in
  addition to whatever the disk did. Both halves are the operator's, and the
  second is precisely the kind of thing that must not arrive quietly.
- **`provider_error` — `WARN`.** The upstream's fault; the `retry()` comment
  beside it already says so. An operator may end up acting — a quota, a
  revoked key, a provider that is down — but no single occurrence demands it,
  and a client retries on the `wait` the envelope already carries. A rise is
  the finding, which is what `WARN` means.
- **`timeout` — `WARN`, uniformly, and deliberately not split by cause.** The
  `retry()` comment records that the deadline is only ever reached while the
  request is *waiting* — for the rest of its body, or for an upstream provider
  — and those have different owners: a slow client is the caller's, a slow
  provider is the operator's. Splitting them was considered and is not worth
  its price. The deadline is enforced by `limits::enforce_timeout`, a
  middleware layer wrapping the whole handler, and `tokio::time::timeout`
  hands it an `Elapsed` that says only that the future did not finish; the
  cause is somewhere inside a future that has been dropped. Reaching it would
  mean every awaiting site reporting what it was waiting on, threaded out to a
  layer above all of them — a large change to pay for a log level, and one
  that would put a reporting obligation on every future await. `WARN` is
  honest for both: a rise in abandoned requests is operationally interesting
  even when each one is a slow client, and `WARN` keeps it visible without
  paging. Two of the seven lines were this, and `WARN` is where they belong —
  not silent, not a page.
- **`not_implemented` — `INFO` by default, `ERROR` from the local-embeddings
  source.** Argued above. `INFO` rather than `None` for the default because,
  unlike a 4xx, this is the *server* declining, and an operator sizing up what
  callers are reaching for should be able to see it at the default filter
  without turning on a firehose. Four of the seven lines were this.

**Alternatives.**

- **Log every 5xx at `ERROR` and fix the alert rule instead** — tell operators
  to exclude `not_implemented`. That pushes a decision the server is in the
  best position to make onto every operator who deploys it, and it is the
  decision they are least equipped to make: it requires knowing which codes
  have a second source. An alert rule that is wrong until it is tuned is a
  rule that is wrong in every deployment nobody got round to tuning.
- **Lower the whole `not_implemented` code** — one line, no override, no new
  field. Rejected because it hides the one occurrence of that code worth
  paging on, which inverts the finding rather than fixing it.
- **A level per construction site, with nothing on the enum** — the override
  mechanism alone. Rejected because the list an operator alerts on would then
  have no single place to be read from, and `operations.md` would carry a
  hand-copied table that drifts. The enum is what makes the published list
  derivable, and the compiler is what stops a new code shipping without an
  answer.
- **Splitting `timeout` by cause** — covered above.
- **A distinct error code for the local-embeddings case**, so the split lives
  on the wire and no override is needed. Rejected on the terms ADR-057 set:
  the code set is closed and a code is something a client *branches* on, and a
  client has nothing to do differently here. It would be a new public code
  whose only purpose is to carry an internal severity, which is the wrong
  place for severity to live.

**Why now, on a pre-release project with no operators.** Not operator pain —
there are none yet. The reason is that `docs/operations.md`'s alert rule and
the test round's own teardown check are being written *against current
behaviour* right now. Left alone, both get written to accommodate a level
scheme that is wrong, and fixing the levels later means rewriting them a
second time and re-teaching whoever read the first version. The cheapest
moment to make the levels right is before anything is documented on top of
them.

**Cost.** A new field on `ApiError` and a third match to answer when a code is
added — the same tax `retry()` already charges, and for the same reason: a
level that is not decided is a level decided by accident. What the change
touches is what a node logs, and nothing else: the status, the `error` code,
the `retry` class and the message in the response body are all untouched, so
the only difference is in the log. There, a deployment grepping for `ERROR`
sees fewer lines, all of them still worth reading, plus one condition that was
never distinguishable before.

**Held by** `crates/kimmy-api/src/error.rs`'s
`every_code_logs_at_the_level_its_actionability_earns` (the whole mapping,
written out so a level changes only on purpose),
`a_refusal_the_caller_caused_writes_no_line_at_all` (driven through
`into_response` against a capturing subscriber, so it is the real log site
being checked and not the table),
`the_log_gate_is_the_codes_level_and_no_longer_the_status_class` (a 500, a 503
and a 501 that used to render three identical `ERROR` lines, now rendering
three different ones) and `an_instance_can_be_louder_than_its_code`; by
`crates/kimmy-api/src/vectors.rs`'s
`the_two_sources_of_not_implemented_do_not_log_at_the_same_level`, which is
the only test module that can reach both sources; and by
`crates/kimmy-api/tests/docs.rs`'s
`operations_publishes_the_level_of_every_code_the_server_logs` and
`operations_names_every_code_that_is_never_logged`, which hold the published
list to the enum in both directions so an operator's alert rule cannot be
made wrong by a level moving underneath it.

---

## ADR-137 — The log level a failure can ask for is a three-variant type, not `tracing::Level`

**Decision.** `ErrorCode::log_level` and `ApiError::log_level` return
`Option<LogLevel>`, and `ApiError::at_level` takes a `LogLevel`, where

```rust
pub enum LogLevel { Error, Warn, Info }
```

`LogLevel::tracing()` converts, once, at the single log site in
`impl IntoResponse for ApiError`. `ErrorCode` and `ApiError` no longer mention
`tracing::Level` in a signature. Breaking for anything outside this crate that
called `at_level` or matched on `log_level`; nothing outside it does.

**Why.** ADR-136 gave `ApiError` a per-instance level override, because
`not_implemented` has two sources that share a code and only one of them should
page. The override took a `tracing::Level`, which has five variants, while
`into_response` handled three — so the two disagreed, and the gap was covered
by a fallback arm:

```rust
_ => {
    debug_assert!(false, "{code} asked for a level below INFO");
    info!(code, message, "{EVENT}")
}
```

`at_level` is public. `ApiError::new(…).at_level(Level::DEBUG)` therefore
**panicked a debug build** and, in a release build, logged the failure one level
louder than the caller asked for. Neither is a thing a caller should be able to
reach, and no caller wanted to: the only two overrides in the tree pass
`Level::ERROR` as a literal.

**The guard was the wrong tool, not a badly written one.** A `debug_assert!`
that is followed by a plausible-looking fallback is the shape this codebase has
spent a release removing — it panics where nobody is watching and silently does
the wrong thing where somebody is. Narrowing the type does not improve the
guard; it deletes the state the guard existed for. `into_response`'s match is
now exhaustive over three variants and carries no fallback, because there is no
fourth case to write.

**Why `log_level` narrows too, and not only `at_level`.** Narrowing one of them
would leave two vocabularies and a conversion between them in the middle of the
code that decides severity. Narrowing both makes the whole of ADR-136's rule
structural:

- *"`None` means this is not a log event"* — the `Option`.
- *"No code is ever quieter than `INFO`"* — the enum has three variants, so
  "quieter than `INFO`" cannot be written down.

`no_code_is_logged_below_info` survives as **documentation**, and says so. It
asserts what the type proves, which is worth stating where somebody adding a
code will read it, and it is the natural home for the one invariant still worth
checking: that `LogLevel`'s three variants map onto the `tracing` levels — and
render as the words — that `docs/operations.md` publishes and `tests/docs.rs`
compares against. That mapping is the only place a drift can now start.

**What was rejected.** Admitting `DEBUG` and `TRACE` to the match, which is what
the first attempt at this did (reverted in `b050d1f`). It reopens exactly what
ADR-136 decided — a failure meant to be quieter than `INFO` answers `None`,
because "do not log this" is a property of the code and not of how the process
was started — and it would create a *second* way for a failure to be quiet,
with different semantics from the first. Two quiet mechanisms is a worse
outcome than the latent panic. It also has no use case: nothing in the tree
wants a below-`INFO` failure line, so the argument would have to be made from
a hypothetical.

**Nothing on the wire moves.** The status, the `error` code, the `retry` class,
the message and the `request failed` event message are all untouched, as are
every code's level and the published table in `docs/operations.md`.

---

## ADR-138 — An orphaned vector shadow is compared for divergence; one beside its collection still is not

**Decision.** `Engine::all_collection_ids` — the set both sides of the
cross-member divergence check are built from (ADR-133) — excludes a vector
shadow collection only **while the collection it serves is present on the same
node**. A shadow whose base collection is absent is included.

`kimmy_core::vector_meta::base_name` is the inverse of `shadow_name` and is what
the rule is expressed in.

**Why.** ADR-133 excluded shadow collections wholesale, for a good reason that
still holds: only the member the rendezvous hash makes the owner builds one, so
a shadow present on the owner and absent on its peers is the design working, and
comparing them would report that on every round for every vector-enabled
collection.

The exclusion was wider than the reason. It also hid the case where a shadow is
the *only* thing a member holds, and that case is not lag — nothing builds a
shadow for a collection that is not there. It is residue, and there is an
ordinary way to produce it:

`DELETE /v1/db/{db}` drops each collection the node holds **at the moment it is
applied**. Issue it on every member at once — as an operator tidying up after a
test might, and as this project's own cluster test protocol prescribed — and
each peer drops what it has while the owner's shadow-creation entry is still in
flight. The entry lands afterwards and recreates the database on that peer,
holding nothing but the shadow.

Both sides of the comparison then filtered that shadow out, the difference came
out empty, and the check reported nothing. Measured on a live three-member
cluster at 0.23.1: a database present on two members and absent on the third for
over **60 seconds**, with `kimmy_sync_divergent_collections` at `0` on all three,
`kimmy_sync_divergence_checks_total{ran}` climbing about 24 times per member,
and `{skipped}` never leaving `0`.

That last part is what makes this worth an ADR rather than a patch.
`{skipped}` at zero with `{ran}` climbing is precisely the reading ADR-135 added
so an operator could tell *checked and agreed* from *not checked*. Here it said
"checked and agreed" about a state it could not see, which is a worse failure
than the one ADR-135 fixed — the earlier gauge was merely ambiguous, and this
one was confidently wrong.

**What this does not change.** The healthy case is untouched: an owner holding
`docs` and `docs.__vectors` and a peer holding `docs` compare equal, which
`a_shadow_beside_its_collection_is_not_compared` pins. The check stays
one-directional — a collection this node holds and a peer does not is still that
peer's own discovery to make — and ADR-133's cap-truncation skip is untouched.

**Alternative considered.** Compare shadow collections against the rendezvous
owner, so a shadow on a non-owner is a divergence. Rejected: it makes the
divergence check depend on ownership state that moves as membership changes,
and it would report a genuine divergence during every legitimate ownership
handoff. The orphan rule needs no such input — a shadow with no base is wrong on
any member, whoever owns it.

**Not fixed here.** The drop race itself. `DELETE /v1/db/{db}` still has no
database-level tombstone, so a late entry can still recreate a dropped database;
what changes is that the cluster can now *see* it. The drop contract in
`docs/http-api.md` now says the drop is applied where it lands and should be
issued once rather than per member.

---

## ADR-139 — A document an index cannot key is stored and filed unkeyed, not refused

**Decision.** An index never refuses a document. Where an index cannot derive
a finite, exact set of keys for a document — arrays at two of a compound
index's paths, more than 1,000 keys for one document, a `Decimal128` at an
indexed path — the document is stored and filed under the index's **unkeyed
run**: one entry per such document under the empty key, which no real key
can be and which sorts ahead of every real key. Every scan of the index reads
that run beside its ranges, in every delivery order, and rechecks each
document against the full filter as it rechecks any candidate. `index_keys`
keeps its strict form for the one place a refusal is right: a **local** write
against a **unique** index, and a local `createIndex` of a unique index over
such a document, both refused with `400`, because a unique index must be able
to key every document it covers and a client on this member is there to be
told (ADR-020). A replicated write a unique index cannot key is filed
unkeyed, takes part in no uniqueness check, and is warned about. The write
path, the replicated write path and the backfill all file the same way, so
the two orders in which a definition and a document can meet on a member
land in one state. TTL expiry reads keyed entries only, since a document with
no key holds no date to be expired by. Each unkeyed filing is logged at
warning with the database, collection, index and document id, counted in
`kimmy_index_unkeyed_total` (bridged as `kimmy.index.unkeyed`), reported per
index as `unkeyed` on the index listing, `describe` and `createIndex`, and
per query as `unkeyedCandidates` on `explain`. Amends ADR-123: a definition a
member's documents do not fit is no longer in the refusal class, because
it now builds; the class is left for a definition this build cannot apply
and a rival it cannot arbitrate.

**Why.** Observed on a three-member cluster running 0.23.2, twice in one
hour. A member created a compound index over two paths while no document held
arrays at both — legal, and correct. Seconds later a client, through a front
that spreads requests across members, wrote a document holding arrays at
both paths to a member that had not yet received the definition. That member
validated the write against the indexes *it* held, found nothing to refuse,
and committed it — also correct. The document replicated to the member
holding the index, whose `maintain_remote` raised the same `InvalidQuery` a
local write draws a `400` for. Nothing on the document path classified it:
the error propagated out of the run's transaction, the transport labelled it
a malformed frame, the witnessed vector was discarded, and the identical
window was re-requested with backoff to 300 s for the life of the process.
The member's entire inbound stream stopped, four collections diverged behind
it, `kimmy_replication_lag_seconds` held its last value, and the recovery was
an operator dropping the index directly on the stalled member. The second
occurrence wedged two members at once. ADR-123 had answered the mirror order
— a definition arriving after the document — by skipping the definition; a
document arriving after the definition had no answer at all.

**Why the rule goes rather than the classification.** The narrower fix is
ADR-123's, one level down: classify the document's failure as a refusal and
skip the document. It was considered and rejected, and so was every other
answer that keeps the rule:

- *Skip the document, count it.* The member never holds a document its
  peers hold. That is a data divergence, permanent until the document is
  rewritten elsewhere, and it contradicts ADR-020's own rule that a
  replicated write cannot be refused without abandoning convergence. The
  refused-definition case ADR-123 accepts costs a member an index; this
  would cost it a document. Order-dependent, too: the member that met the
  pair the other way holds the document and lacks the index.
- *Skip and quarantine for replay.* The same divergence with a repair path
  bolted on — a table, a backup tag, a route, an operator step — for a state
  the rule below never enters.
- *Drop the index on the member instead.* Converges, order-independently, to
  the state a fresh `createIndex` would report, and loses an index the
  operator created, cluster-wide, on the strength of one document. Consistent
  with ADR-020 and the least change that converges; rejected only because
  the rule below converges without losing anything.
- *Index the cartesian product.* Sound here, since every candidate is
  rechecked, and bounded by the 1,000-key cap — which is the second trigger,
  and cannot be indexed past. Two mechanisms for one class.

Behind all four is the same fact. A rule that can refuse a document because
of an index is a constraint, and ADR-020 already established that a
leaderless store cannot hold a constraint across members without coordination:
a member holding the definition and a member holding the document are each
valid alone and invalid merged. For uniqueness the product chose to accept
and record, because there is no merge function for it. For these three rules
there is a merge function, and it is the one every leaderless store with
secondary indexes already uses: the index is a projection of the local
documents, never a schema over them. A document the projection cannot express
is simply a document the index does not narrow, and the recheck — which this
codebase already runs on every candidate because an index "answers which
documents *might* match" — is what keeps the answer exact. The rules were
inherited from MongoDB, which can afford a refusal because every write goes
through one primary and an index build commits on a quorum; a store that
accepts writes on every member cannot, and the refusal bought a smaller index
and an early error for a badly shaped document at the price of the
cluster's convergence.

**Why an empty key in the same table, rather than a table of its own.** Every
real key begins with a type tag byte, so no document produces an empty key,
and an empty slice sorts first — the run sits at the front of an index's
entries, disjoint from every range a planner can ask for. Living in
`INDEX_ENTRIES` means a drop's range purge, a backup, and the id migration
cover it with no code of their own, and a scan reads it as one more range:
its entries are in document-key order and hold each document once, exactly
the shape of an exact probe, so the merged-runs delivery takes it as one
more head, the key-order pass takes its entries with the rest, and index
order reads it first. One seek says whether the run is empty, which it is
for almost every index, and then the scan is exactly what it was — the
single-run delivery included.

**Why unique keeps a refusal, locally.** A unique index that cannot key a
document cannot check it, and an index that reports a constraint it does not
hold is worse than no index. ADR-020's asymmetry answers both sides: a local
write is refused, because the client is there to be told; a replicated write
is a fact another member accepted, and is filed unkeyed and warned about
rather than refused, exactly as a replicated duplicate is recorded rather
than refused. The unkeyed document is outside the constraint, and the listing
says so.

**What still makes a member refuse a definition.** ADR-123's class shrinks to
what is true of the *definition*: a shape this build cannot apply (a TTL over
two fields, a partial filter it cannot parse, `coordinated` enforcement) and
a rival under a held name with no creation stamp to arbitrate (ADR-132). No
document can make a non-unique definition unbuildable, on any path, which
is what makes both arrival orders converge. The tests that built "an index
the receiver cannot build" out of a two-array document now build one out of
a definition no member can mint, so the skip-and-count machinery stays
covered.

**Companion.** `createIndex` will confirm the definition on every live member
before it answers, so that the ordinary case — a client creating an index on
one member and writing through a front seconds later — no longer opens the
window this finding came through. That is a protocol change with its own
record; it narrows the window, and this record is what makes the window
harmless.

**Alternatives.** *Refuse two-array compound indexes at creation.* ADR-123's
own argument against still holds, and it would not touch the other two
triggers. *Defer a write on a member with no index until a definition might
arrive.* The member cannot tell "no index" from "an index in flight", so it
would hold every write for a sync interval and still miss a partitioned
member. *Hold the document under the cartesian product up to the cap and
refuse past it.* Two mechanisms, and the cap case is the one ordinary data
reaches. *A separate table for the run.* Rejected above.

**Cost.** One seek per index scan to learn the run is empty; for an index
that holds unkeyed documents, every scan of it reads and rechecks them all,
which `unkeyedCandidates` reports and `unkeyed` on the listing predicts. A
document filed unkeyed under a unique index is not checked for uniqueness. A
client that relied on the `400` to police document shape has lost it, and
the changelog says so: the shape is now visible on the index rather than
refused at the write. `maintain` and `maintain_remote` take the engine, for
the counter and the warning. `IndexScanOutcome` gains `unkeyed`; the
`Index` schema gains a required `unkeyed`; `explain` gains
`unkeyedCandidates`. One new `/metrics` series, pinned by the golden tests
and the bridge guard.

## ADR-140 — A schema change confirms itself on every live member before its request answers

**Decision.** `createIndex` and `dropIndex` on a clustered node push the entry
they minted to every member SWIM considers alive, at once, and answer only
when each has applied or refused it or a deadline has passed. A new pair of
protocol messages carries it — `Push { entries }` and `Pushed { applied, ddl,
ddl_refused, unknown_collection }` — and the receiver applies a push through
the same `apply_batch` a pulled window goes through, so the entry is
witnessed (anti-entropy does not fetch it again), appended onward (the member
can serve it to a third), and, if the member cannot apply it, refused and
counted there exactly as it would have been on the pull; the serving side
takes a hook so a pushed refusal lands on the receiver's own
`kimmy_sync_ddl_refused_total`. The response carries `confirmation`:
`confirmed`, `refused` and `pending` members by node id, the last with a
reason. The deadline is `cluster.ddl_confirm_timeout_secs` (default 10; `0`
turns the confirmation off), applied per member with the pushes running
concurrently, so the request waits about as long as the slowest member takes
rather than the sum. A node with no member set — clustering off, or
membership off — answers as before, with no `confirmation` at all. A drop that
finds nothing here mints no entry and confirms nothing.

**Amended 2026-09-06 — the push carries a window, not an entry.** Pushing the
entry alone through `apply_batch` raised the member's witnessed vector past
every earlier entry from this origin it had not yet pulled, and nothing
re-served them; ADR-143 makes a push a pull the sender starts, so `Push` now
carries what `Versions` and `Entries` would have, and the receiver applies it
through `apply_peer_batch`. The decision here — confirm on every live member
before answering — stands unchanged. (ADR-141 has since made a drop mint its
entry wherever it lands, so a drop always has an entry to confirm.)

**Why.** The window ADR-139's finding came through. A client created an index
on one member and, seconds later, wrote through a front that spread requests
across members; the write landed on a member that had not yet received the
definition, which validated against the indexes *it* held, found nothing to
refuse, and committed. Measured on a quiet cluster the gap is about 3.5 s —
a definition is listed on a peer in 1.7–3.4 s and plannable in ~3.5 s — and
it is exactly the moment a client, having been told `200`, reasonably
believes the index exists. ADR-139 makes what slips through the window
harmless; this makes the window not open in the ordinary case. The member
that answers `createIndex` is the one that knows the definition exists and
which peers have confirmed it, so the wait belongs there — not on the writing
member, which cannot tell "no index" from "an index in flight" and could only
hold every write for a sync interval, and still miss a partitioned member.

**Why a push, and why through `apply_batch`.** Anti-entropy is pull-only and
paced by the sync interval; a confirmation needs the member's answer now.
Pushing the entry and applying it by the ordinary path costs nothing new in
correctness: every rule a pulled entry meets, a pushed one meets — the
witnessed vector, the tombstones, the creation-stamp arbitration, the refusal
class — and the pull that follows finds it already witnessed. The alternative
of *asking* the member whether it has seen the stamp yet would have polled
the sync loop's schedule; the alternative of nudging the member to pull now
would have needed a second round trip to learn the answer.

**What the response means.** `200` with an empty `pending` list means every
member the cluster considers alive holds or has refused the definition. A
member under `refused` has counted the refusal on its own metrics and logged
the reason; that is ADR-123's state, now told to the client that caused it. A
member under `pending` did not answer in time — down, partitioned, or slow —
and receives the change through anti-entropy as before; the response says so
rather than pretending, and the request is not failed for it, because the
index exists and will propagate. A partitioned member is the residual this
cannot cover: it is why ADR-139, not this record, is what makes the cluster
safe.

**Alternatives.** *Return 202 or 503 when a member is pending.* The index
exists and is in use here; a status that reads as failure would send a
client to retry a create that succeeded. *Confirm every schema change.*
Collection and vector DDL share the mechanism and could adopt it; index DDL
is where the finding was, and the rest is left for a reason to appear.
*Confirm on a quorum.* A leaderless store has no quorum to name (ADR-037);
"every live member" is the set SWIM already maintains.

**Cost.** Two protocol messages; a `Push` is capped at the batch limit and
refused above it. `serve` gains a `serve_with` form taking the hook.
`createIndex` and `dropIndex` on a clustered node wait for their peers, up to
the deadline; a member that is slow to answer makes the request slow, which
is the point. `drop_index_inner` returns the stamp it minted. `Members` gains
an accessor for address-and-id pairs. One config key. The response schemas
gain an optional `confirmation`.

## ADR-142 — The engine's `/metrics` block renders with the process counters, so the bridge and its guard see the whole page

**Decision.** The nine series the `/metrics` handler read from the engine and
the state and rendered ahead of the process counters — `kimmy_databases`,
`kimmy_collections`, `kimmy_unique_violations`, `kimmy_commits`,
`kimmy_fsyncs`, `kimmy_commits_grouped_total`, `kimmy_storage_bytes`,
`kimmy_vector_index_cache_bytes`, `kimmy_up` — are now inputs to the same
render. `AppState::storage_readings` takes them as a `StorageReadings`
value; `Metrics::render_with` renders the whole page from it, byte for byte
what the route produced, and `Metrics::snapshot_with` carries the same nine
fields, which the OTLP bridge exports as `kimmy.databases`,
`kimmy.collections`, `kimmy.unique_violations`, `kimmy.commits`,
`kimmy.fsyncs`, `kimmy.commits.grouped`, `kimmy.storage.bytes`,
`kimmy.vector.index_cache.bytes` and `kimmy.up`. The bridge's export callback
takes a fresh reading per export, as the handler takes one per scrape, and
observes nothing that export if the reading fails. The guard test that
compares the two surfaces renders the whole page. The mirror ADR-139 added
for `kimmy_index_unkeyed_total` — an atomic each reader refreshed from the
engine before reading — is gone: that count is one of the readings.

**Why.** ADR-070 bridged "the same counters `/metrics` renders" and pinned
the route's engine block "structurally", by the ordered list of series names
a scrape sees. The bridge read `Metrics::snapshot`, which never held the
engine's numbers, and the guard written later — because the bridge had
drifted twelve series behind `/metrics` "without anyone deciding that it
should" — rendered `Metrics::default()`, which never held them either. So a
deployment that reads telemetry only through a collector, the deployment the
bridge exists for, could not see unique violations, commit and fsync cost,
storage size or the vector cache: the series `docs/benchmarks.md` and
ADR-088's durability story are read through, and the one ADR-020 says makes
a merged collision visible without a subscriber watching. Found placing
ADR-139's counter, which had to go in the guarded block by way of a mirror
to reach the bridge at all.

**Why readings handed in, rather than a database handle on `Metrics`.**
ADR-070's reason stands: this type prints numbers and should not own a
database to print two of them. A value read by the caller keeps that, and
gives both readers the same rule — take a reading, render or export it — so
neither can report a window the other measured. Every field is a level or a
monotonic count, so a reading taken at read time is exact, which is what let
the mirror be correct and what makes it unnecessary.

**Why the bridge observes nothing on a failed reading.** A counter exported
as 0 and then as its true value is a reset the collector will believe. An
export skipped is a gap the collector reports as one.

**Cost.** Two metadata scans per export (`kimmy.databases`,
`kimmy.collections`), as the scrape already paid per scrape. `Metrics::render`
and `Metrics::snapshot` survive as the reading-free forms, for callers with no
engine in hand, which are tests. Nine more lines in the golden render, nine
more instruments, and the guard now refuses any engine series added to the
page without an instrument.

## ADR-141 — A drop mints its entry wherever it lands, and a declined drop is counted

**Decision.** `DELETE /v1/db/{db}/coll/{coll}/indexes/{name}` on a member that
holds the collection but not the index **mints the `DropIndex` entry and
records the tombstone all the same**, under a fresh local stamp, in one
transaction, and answers `dropped: false` — this member removed nothing —
while the drop replicates to every member that does hold the index, and, on a
clustered node, is pushed to every live member before the response (ADR-140).
A name a create would have refused is refused here too, before anything is
minted. A collection that does not exist stays `404`. In `apply_ddl`, the
branch that turns away a replicated drop older than the index standing under
its name now logs at info with both stamps and counts in
`kimmy_sync_ddl_declined_total` (bridged as `kimmy.sync.ddl_declined`), on the
round report and on the push reply, where a confirmation reads a declined
drop as a member that did not apply it. Amends ADR-123's cost paragraph.

**Why.** Observed on a three-member cluster running 0.23.2. A test case's own
cleanup dropped the index at the centre of the wedge ADR-139 records, through
a front that spreads requests across members. The request landed on a member
that did not hold the index, answered `200 {"dropped": false}`, minted no
entry, and did nothing cluster-wide; the index survived on the member that
held it for 42 minutes, until an operator dropped it there directly. A client
checking the status saw success. A client reading the body could not tell
"no such index anywhere" from "not here, but a peer holds it", which have
opposite consequences. The product deliberately produces states in which an
index stands on some members and not others — ADR-123's refusal class, and
the seconds after any `createIndex` — and then offered a drop that silently
did nothing against exactly those states.

**Why ADR-123's reason no longer holds.** ADR-123 had a local drop of an
absent index record nothing, because "a tombstone would be a decision this
node's peers never hear of, and it would make this node refuse a definition
every other member accepts." Both halves fall away once the entry is minted:
the peers hear of it and drop theirs, and the definition this member will
read as history is one no member keeps. ADR-132 supplies the arbitration the
paragraph lacked. The drop's fresh stamp is ahead of every creation this
member has witnessed, so on each holder the replicated drop removes the
index, and a create arriving later at the dropper — the one in flight during
the seconds after a `createIndex` — reads as older than the tombstone. The
end state is the same everywhere: no index. A later re-creation, stamped
after the drop, wins everywhere, as it already did.

**Why a drop is an instruction, not a report.** A member a front happened to
route the request to is no less entitled to issue it than the member that
holds the index. `dropped` still answers the local question — did this
member remove one — because that is a fact the member knows; what it no
longer implies is that nothing happened. On a clustered node the response's
`confirmation` says which members applied the drop, which is the cluster-wide
answer the old body could not give.

**Why the declined branch is counted, and at info.** The residual this
change cannot reach is a holder whose creation stamp is *ahead* of the drop's
— a member whose clock ran far ahead when it created the index, the case
ADR-132 documents — which declines the replicated drop while the dropper's
tombstone makes the create history there: a split nothing reported. A
re-served window carries a drop past the recreation it preceded as a matter
of course, and that is the rule doing its job, so the line is info rather
than warn; the witnessed vector keeps re-serves rare, so a count that keeps
rising while nothing is being recreated under the name is the clock case,
and the escape hatch is the one `indexes.md` already gives: a local drop on
that member, which mints a stamp ahead of the creation.

**Alternatives.** *Answer differently on a non-holder* — a status or a field
saying "a peer holds it" (the finding's first proposal). The receiving member
can see the definition is replicated state, but cannot say which peers hold
it now, and a client told so would still have to find the holder; once the
drop reaches the holder there is nothing to distinguish. *Document it and
send drops to the holder.* Correct, insufficient, and what ADR-140's first
text said; withdrawn here. *Mint a tombstone but no entry.* That is the
decision this record amends: a tombstone no peer hears of.

**Cost.** A drop of an absent name costs one transaction and one entry where
it cost nothing. `drop_index_inner` returns `Dropped { stamp, removed }`
rather than a stamp. `SyncOutcome`, `RoundReport`, `Pushed` and the metrics
snapshot gain `ddl_declined`; one new `/metrics` series, pinned by the golden
tests and the bridge guard. A name that fails `validate_name` is a `400` on
drop as on create, where before it was a silent `dropped: false`.

## ADR-143 — A push is a pull the sender starts: a member's witnessed vector is raised only over a window that begins where its own history ends

**Decision.** The push ADR-140 introduced carries the *window* a member
lacks from the pushing node, ending in the change, not the change alone.
`Push` now carries what `Versions` and `Entries` would have carried had the
member pulled — the pusher's servable vector, the entries, `scanned_to` and
`exhausted` — and the receiver applies it through `apply_peer_batch`, the
same coverage rule a pulled window goes through. To derive the window the
pusher asks the member what it has processed (`AskWitnessed` / `Witnessed`:
the member's witnessed vector) and computes the threshold from it exactly as
the member would for itself, then reads the window under the same batch and
frame limits a served pull has, horizon check included. A member that has
already processed the change is confirmed without anything being sent. A
member the window cannot reach — more than a batch behind, or below the
pusher's retention horizon — is sent nothing and named `pending` with that
reason: the sync loop is already doing that work at its own pace, and a
window that stops short of the change would only repeat it on a request's
clock.

**Why.** The first form of the push handed the member one entry through
`apply_batch`, which observes every entry it takes into the witnessed vector
(ADR-054). That is right for a pulled window, whose first entry sits at the
member's own position, and wrong for an entry that arrives out of order: the
member's witnessed position for the pushing origin jumped to the change's
stamp, and every earlier entry from that origin it had not yet pulled — the
collection created a few milliseconds before the index, the documents written
into it — fell behind a position anti-entropy never asks about again. The
member skipped the pushed index as an unknown collection, was never sent the
collection, and stayed that way for the life of the cluster: a hole, of the
kind ADR-054, ADR-082 and ADR-127 each closed one shape of. It showed as two
cluster-harness TTL tests failing whenever the collection's expiry owner was
not the node that created the index — about two runs in three — before the
change was released.

**The invariant, stated.** Nothing raises a node's witnessed vector for an
origin except a window that starts at that node's own position for it, or a
snapshot that hands over coverage wholesale (ADR-082). Every path that moves
entries between members — pull, snapshot, and now push — goes through the
coverage rule; no caller applies a foreign entry through `apply_batch`
directly, which remains for a batch this node already accounts for.

**Alternatives.** *Apply the pushed entry without witnessing it.* Appending
an entry raises both vectors by construction (ADR-054: appending is the
strongest form of having seen it), so the entry would have to be applied
without being appended, leaving an index whose creation this member cannot
serve onward and a snapshot that disagrees with the oplog. *Ask the member to
pull now.* Needs the member to dial back, and a second round trip to learn
the answer; deriving the window on the pusher's side is the same computation
with the roles kept. *Push the window even when it stops short of the
change.* A sync round's work on a request's clock, for a `pending` either
way.

**Cost.** Two protocol messages (`AskWitnessed`, `Witnessed`); `Push` gains
three fields. `push_entries` becomes `push_entry`, which answers with the
member, its outcome and, when the window could not reach the change, why. A
confirmation can now name a member `pending` for being too far behind as
well as for not answering. The receiver's cap on a pushed batch stays. One
harness test and five transport tests.

---

## ADR-144 — A log line's event name and its message are two fields, never one key twice

**Decision.** `impl IntoResponse for ApiError` logs a failure as
`error!(event = EVENT, code, message)` — and the same shape at `warn!` and
`info!` — with no format string. The line's fields are exactly `event`
(`request failed`), `code` (the error code) and `message` (the client-facing
text), plus whatever the subscriber adds: `timestamp`, `level`, `target`. No
`tracing` macro call in the tree passes a field named `message` beside a
format string, and `docs/operations.md`'s log-line contract now names the
three fields as they are. A test drives a failure at each of the three levels
through the same JSON layer `kimmyd` runs, and holds the raw bytes to the
parsed object: every key written once, and the three fields carrying the
values the document promises.

**Why.** A `tracing` macro's format string is not separate from its fields;
it is one of them, named `message`. So `error!(code, message, "{EVENT}")`
— the shape ADR-136 chose — recorded two fields called `message`: the
explicit one carrying the client-facing text, and the implicit one carrying
`request failed`. The pretty formatter renders each field it is handed, and
showed the two as two, which is why nothing looked wrong on a terminal and
why the existing test, which reads the pretty formatter's output, could not
see it. The JSON layer serialises the fields in the order they arrive and
does not deduplicate, so every `request failed` line in the format an
operator's pipeline reads carried `"message"` twice inside one object. That
is legal to emit and every parser accepts it, keeping one of the two without
saying which — first or last is a property of the parser, not of the line.
`operations.md` promised that the client-facing text is in `message` and that
every line says `request failed`; whichever value a parser kept, the line
broke one of the two promises, and the operator learned which from their
tooling rather than from the document.

**Why `event` and not a different format string.** The name has to live
somewhere, and the format string is the wrong place for it *because* it is a
field with a fixed name: anything else called `message` collides with it.
Renaming the explicit field to `detail` or `text` instead would have kept the
format string and broken the promise the other way round — the document
would say `message` and mean the event name, and the client-facing text would
be under a name nothing else in the tree uses. Making the event name an
ordinary field, named for what it is, keeps the contract ADR-136 wrote and
gives the pipeline the key it was already told to match on. The name stays
constant across the three levels for the reason ADR-136 gave: severity is the
level's job alone.

**Alternatives.** *Set `flatten_event` or another layer option.* Flattening
moves the fields up a level; it does not make two keys one. *Deduplicate in
the subscriber.* That is a fix to the wrong layer — the line is wrong before
any subscriber sees it, and every subscriber would need the same fix again.
*Leave it and document the double key.* A contract that says "one of these
two, depending on your parser" is not a contract.

**Cost.** Nothing on the wire moves: the status, the `error` code, the
`retry` class and the response body are what they were. What changes is the
log line, in both formats. A JSON pipeline that matched on
`fields.message == "request failed"` was matching on a parser accident and
now matches on `fields.event`; one that read `fields.message` for the text
now gets it every time. The pretty format shows `event="request failed"`
where the bare text was. `tests/docs.rs` pins the level table and the silent
list, not the field names, so this ADR adds the pin the field names lacked:
`a_failed_request_line_carries_event_code_and_message_once_each` and the
key walk it rests on.

**Held by** `crates/kimmy-api/src/error.rs`'s
`a_failed_request_line_carries_event_code_and_message_once_each` (a 500, a
503 and a 501 through `fmt().json()`, the parsed object checked for the three
fields and the raw line walked for a key written twice), and
`a_json_key_walk_sees_the_repeat_a_parser_would_swallow` (the walker shown a
line with the defect, so the assertion above has a witness that can see it).

---

## ADR-145 — The count half compares against a peer that is behind but standing still, and the divergence gauge says how old its reading is

**Decision.** Two amendments to the cross-member divergence check
(ADR-133, ADR-135), one for each half of what a live cluster showed.

*First*, the guard on the count half — the document-count probe is dropped
whenever the peer has not yet witnessed something this node has (ADR-133,
defect 2) — drops it only while the peer is behind **and advancing**. The
round remembers, per peer, the peer's position on every origin it trails
this node on as of the last checked contact, and how many consecutive
checked contacts that position has come back unchanged on (`PeerStalls`,
in `kimmy-cluster`'s transport beside the gate that reads it). A peer whose
position moved is catching up and its count is stale: deferred, as before.
A peer whose position has come back unchanged on `FROZEN_CONTACTS`
consecutive checked contacts — three, a named constant with its reasoning
on it, not a setting — is standing still: its count is what it holds and
will keep holding, and it is compared. A peer that is not behind is compared
at once, as before. Every checked contact is counted under a new series,
`kimmy_sync_divergence_count_probes_total{outcome="compared"|"deferred"}`,
so a count half that has never compared against anyone is visible rather
than inferred; `compared + deferred ≤ ran` rather than `=`, because a
checked contact increments neither when the rotation named no collection,
when this node's own count of the named one failed, or when the peer's
answer carried no count because it does not hold the collection, which the
existence half reports.

*Second*, a round that fails is counted as `skipped` on
`kimmy_sync_divergence_checks_total` as well as in
`kimmy_sync_failures_total`, and `/metrics` gains
`kimmy_sync_divergence_check_age_seconds`: seconds since the last contact,
with any peer, whose round ran the check, as of the last sync tick, `0`
before the first. The check does **not** run on a failed round. Both new
series reach the OTLP bridge in the same change, as
`kimmy.sync.divergence_count_probes.compared`, `.deferred` and
`kimmy.sync.divergence_check_age`.

**Why.** Observed on a three-member cluster running 0.23.2. One member's
inbound replication wedged: every sync round it ran failed and was retried,
for half an hour. Four collections diverged in document count on it, under
the same fifty collection names every member held.
`kimmy_sync_divergent_collections` read 0 on all three members throughout.
The two healthy members each ran the check some 250 times and reported
clean every time; on the wedged member `{outcome="ran"}` stopped moving and
the gauge went on serving the value its last completed round had left.
Two defects, independent of each other.

*The count half was blind to the wedged member, by the rule that protects
it from ordinary lag.* Defect 2's argument is right and stands: a peer that
has not yet pulled this node's recent writes answers a probe with a stale,
lower count, the two-contact confirmation does not filter that out because
a lagging peer reproduces the same mismatch on every contact, and an alert
that cries wolf on lag is the one an operator disables. But the argument is
about a peer that is *catching up*, and the gate it produced reads every
peer that is behind as one. A member whose inbound replication has stopped
is behind for good, so under that gate its count is never compared for as
long as it stays stopped, while the existence half — which the gate does
not touch — keeps `ran` climbing on every contact. That is the reading the
healthy members produced: 250 checks, clean, and not one of them had looked
at a document count on the member that mattered. Nothing counted a
gate-dropped probe, so "compared and equal" and "never compared" were the
same 0.

What tells the two cases apart is not how far behind the peer is but
whether it is *moving*. A backlog draining advances the peer's position on
some origin it trails this node on, on every round it completes; a member
that has stopped leaves every such position exactly where it was. So the
memo keeps, per peer, the peer's position on each origin it trails this
node on — the peer's own origin is never among them, since nothing holds
more of a node's writes than the node, so a wedged member still taking
local writes reads as still, which it is on every origin that matters; and
this node's own progress, on its own origin or on one it pulled from a
third member, adds an origin to the map without breaking the run, because
that is this node advancing, not the peer. Only the peer getting closer on
an origin it trails resets the count. Three unchanged sightings, because a
peer with a backlog moves on every round it completes, so even one
unchanged sighting is unusual, and three in a row is a peer that has
stopped: fifteen seconds of standing still at the default interval, on a
cluster whose healthy members converge in five. A larger value only delays
the count half against a peer that is already wedged; a smaller one would
trust a peer that merely missed one round's pull from this node, which the
fanout rotation on a cluster larger than a few members makes routine.
"Consecutive" is consecutive *checked* contacts: a contact ADR-133's
cap-truncation skip applies to says nothing about whether the peer moved,
and the memo is not consulted on it.

The memo lives in the transport, beside the gate, and not in either of the
two places it might seem to belong. `PeerHealth` is failure bookkeeping
keyed by address, and it *forgets* a peer on every successful round — the
exact rounds this memo is built from, since a frozen peer answers every one
of this node's pulls perfectly. `DivergenceTracker` is `kimmy-storage`'s,
transport-free by design, and keyed by what a check *found*; this is keyed
by what the wire said before the check ran, which is transport's own
business and the one place the peer's vector exists. The loop owns it, as
it owns the tracker and the health record, because it is a fact about this
process's contacts and not about the data; the round reads and writes it,
because the round is where the vector is. `sync_once` keeps its signature
and its meaning — a round with no memory of the peer, gated exactly as
ADR-133 first had it — and the loop calls `sync_once_with`, which carries
the memo.

The deferral is counted as its own series rather than as a third outcome on
`kimmy_sync_divergence_checks_total`, and ADR-135 rejected both shapes;
this ADR reopens the second on a case that rejection did not weigh. A third
outcome would double-count: a contact that ran the check *and* deferred the
probe is one contact, already in `ran`, and ADR-135's closed set is a
partition of completed rounds, which this is not. Counting the suppression
at all was rejected because "on any busy healthy cluster that happens on
most contacts, so the series would rise in proportion to write traffic and
mean nothing an operator could act on." That is true of `deferred` alone,
and it is why the series carries both outcomes: `deferred` rising is
ordinary and says so, and `compared` flat beside a rising `ran` is the
reading the wedged cluster needed — a count half that has never looked at
anything — which no combination of the existing series can produce. The
count half's coverage bound ADR-135 pointed to instead, order twice the
collection count in checked contacts, was a bound on latency; a peer
permanently behind is not late, it is never.

*The gauge went stale with nothing to say so.* `divergent_collections` is
`DivergenceTracker::confirmed_count()`, re-read every tick, and the tracker
is told about a contact only on a round that succeeded: an `Err` round
touches neither the tracker nor the two counters, and `PeerHealth::failed`
backs the peer off, up to 300 s between attempts. So on a member whose every
round fails, `ran` stops, the gauge holds its last value, and the guide's
"both counters flat → look at failures and backing off" is the right advice
for anyone who reads the counters — but the gauge itself goes on serving a
number, and a dashboard that shows the number shows nothing beside it to
say that the number is half an hour old.

The obvious fix is wrong, and this ADR records why rather than leaving the
temptation for the next reader: running the check on a failed round, or on
the peer's answer to `AskVersions` before the round failed, is checking on
the strength of a round that did not complete, which is the hole ADR-133's
cap-truncation skip closes one level up. A failed round has not earned the
belief the check depends on. It is counted instead, as `skipped`, because
whatever failed and however far it got, the gauge was not re-examined on
that round, and that is the one thing the counter says. Every failed round
is counted, not only an apply failure: splitting failures by how far they
got before failing would be a distinction the gauge does not care about,
and the dial timeouts already make it elsewhere. ADR-135's accounting —
"`ran + skipped + failures` is every contact" — becomes "`ran + skipped` is
every round attempted, and `failed` is the part of the skips that failed",
and the operations guide's PromQL drops the addition. The cost is one
reading that used to be unambiguous: "`ran` flat, `skipped` rising" was a
backlog and is now a backlog or a wedge, which `kimmy_sync_failures_total`
tells apart, and the guide says so.

*Count a failed round under a third outcome, `failed`, instead.* That would
have kept `skipped`'s published meaning — a completed round whose pull was
truncated — and given the counter an exact partition,
`ran + skipped + failed = attempted`, though not the PromQL migration it
looks to spare: the guide's `sum without (outcome)` takes a third outcome in
with the other two, so the published addition of `kimmy_sync_failures_total`
double-counts a failed round under either route, and what the alternative
spares is only a rule that selects `outcome="skipped"` by name. Not taken,
because the counter's question is whether the gauge was re-examined on
that round, and on a failed round it was not, for the same reason and to
the same effect as on a truncated one. A third outcome would split that
one answer by a cause the counter does not care about and
`kimmy_sync_failures_total` already names, and every rule that reads
`skipped` as "the gauge is unknown" would have to learn a second label to
stay right, where under the chosen route it stays right unchanged.

The age gauge is the "seconds since the last check" series ADR-135 rejected,
added on terms that answer the objection. The objection was that a node
that has never checked has no honest number — `0` reads as just checked,
uptime as a stale success, a large number as a fault on a node with no
peers. That is true of the gauge alone, and ADR-135's own counter is what
makes it false of the gauge beside the counter: `0` with `ran` at `0` is
"never", and `ran` carries that case so the gauge does not have to. What
the counter cannot do, and the gauge does, is keep moving on a member whose
loop has stopped completing rounds: `ran` is flat, `skipped` moves once per
backoff interval, and over an alerting window that pair is
indistinguishable from a member with no peers. A level that rises with the
clock is not. The age is a fact about the loop's contacts, so the loop
owns it — a clock advanced in the same arm that folds a finding into the
tracker, so it can never reset on a contact the tracker was not told
about, and reported once per tick through `RoundReport`, the way every
other divergence fact crosses into `/metrics` (ADR-135). It is therefore up
to one interval stale at a scrape, which is why the guide's rule is "above
*k* × the sync interval" and not "above the interval".

**Consequences.** On a cluster with a wedged member, the healthy members'
gauge now names the count-only divergence after the count half has
compared twice against it — three deferred contacts to establish the
stall, then two compared probes of the affected collection, so on the
order of five checked contacts plus however many the rotation spends on
other collections — where before it never did. The wedged member's own
gauge still holds its last value, and now says how old that value is;
`skipped` rises there once per backoff interval, so the counter pair reads
the "unknown" shape the guide documents rather than the "no peers" one.
The operator rule is one line: age above *k* × `cluster.sync_interval_secs`
means the gauge is unknown; look at `kimmy_sync_failures_total` and
`kimmy_sync_peers_backing_off`.

Residuals, stated. A member wedged on one origin's entries while still
pulling another member's reads as moving until that other origin has
drained; the count half reaches it once it has. A peer that stands still
for a reason other than a wedge — backed off from every peer with nothing
to pull, say — is compared, and if it holds fewer documents than this node
that is reported: honestly, since it has not taken this node's writes and
is not about to, and still subject to the two-probe confirmation. The
existence half is untouched. The cap-truncation skip is untouched. No name
of any kind is added to `/metrics`: three series, all bare counts, per
ADR-133's standing property and ADR-135's no-peer-label rule. What the
check cannot catch is what it could not catch before, less the case this
ADR is about.

Cost: one `BTreeMap` of positions per peer in the loop; three `AtomicU64`s
and three sample lines; one field on `SyncOutcome`
(`count_probe_deferred`), three on `RoundReport`, and `sync_once_with`
beside `sync_once`. The HELP text of `kimmy_sync_divergence_checks_total`
is reworded for the failed-round accounting; its name, labels and position
are unchanged, and the byte-for-byte render test carries the new text.
Nothing on the cluster wire changes: the memo is built from `Versions`,
which every round already exchanges.

Defended by `kimmy-cluster`'s transport tests
`the_count_probe_is_deferred_for_a_moving_peer_and_compared_for_a_still_one`
(the truth table, gate and counter outcome together) and
`a_stall_survives_this_nodes_own_progress_and_breaks_on_the_peers`; by
`peers.rs`'s
`the_check_age_is_absent_then_rises_through_failed_rounds_and_resets_on_a_check`;
by `kimmy-api`'s `the_render_is_byte_for_byte_what_a_scrape_receives`,
`the_snapshot_reads_the_same_atomics_the_render_does`,
`the_pushed_gauges_render_what_was_pushed` and
`an_age_the_loop_has_not_got_renders_as_zero`; by `kimmyd`'s
`every_metrics_series_reaches_the_bridge`; and by
`kimmy-cluster/tests/replication.rs`'s
`a_count_divergence_on_a_frozen_peer_is_found_and_the_frozen_member_reports_its_age`,
which runs two real loops over sockets — one member's inbound frozen after
a single good round through a relay that hands exactly one connection
through and refuses the rest — and asserts both halves: the healthy
member's count half defers for `FROZEN_CONTACTS` contacts, compares, and
confirms the count-only divergence with `failed` at 0 throughout; the
frozen member's `ran` never moves again, every failed round is a skip, and
its age rises while its gauge holds 0.
