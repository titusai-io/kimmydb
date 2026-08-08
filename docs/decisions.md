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

**Why.** `fastembed` pulls native ONNX Runtime **and**, by default, OpenSSL. That
would undo the pure-Rust property that motivated redb over RocksDB (ADR-001) and
`rust_crypto` over `aws_lc_rs` (ADR-016), and roughly triples the image. A
zero-config default that quietly costs cross-compilation, a native toolchain and
hundreds of megabytes is not zero-cost.

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

## Next

- [Roadmap](roadmap.md) — decisions still to be made
- [Testing](testing.md) — how these choices are defended
