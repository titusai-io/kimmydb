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

## Next

- [Roadmap](roadmap.md) — decisions still to be made
- [Testing](testing.md) — how these choices are defended
