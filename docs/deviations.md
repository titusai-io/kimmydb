# Deviations Register

[← Documentation index](README.md)

Where the implementation differs from what was planned or requested, why, and
what it would take to close.

Individual notes live near the code they affect, but scattered notes are not
reviewable — this is the single place to see the whole picture. **Every entry
here is a debt, not a decision that has been quietly retired.**

Status meanings:

- 🔴 **Open drift** — differs from an explicit request, not yet agreed
- 🟡 **Agreed deferral** — differs from the plan, explicitly accepted
- 🟢 **Superseded** — the plan changed for a recorded reason

---

## 🟢 HNSW implemented (was an open drift)

**Requested.** During planning the choice was *"HNSW from the start"*. It was
then deferred three times across sessions, each time to land something else
end to end — no single decision, which is exactly how this kind of drift
happens.

**Now built.** `HnswIndex` over `hnsw_rs`, with recall measured against the
exact path rather than assumed: the tests assert ≥ 90% recall at k=10 and that
the nearest neighbour agrees with an exact scan exactly.

**Two findings from building it:**

- The crate the roadmap originally named, `hnswlib-rs`, **requires nightly
  Rust** — its `corenn-kernels` dependency uses `#![feature(f16)]`. Naming it
  without checking was my error. `hnsw_rs` builds on stable.
- **The `Dot` metric has no approximate index.** `anndists::DistDot` computes
  `1 - dot` and *asserts the result is non-negative*, which only holds for
  unit-length vectors; a real embedding would abort the process. Dot-product
  collections use the exact scan. Normalizing on the way in would make it work
  but would silently change what a dot-product search means.

---

## 🟢 HNSW is wired into search (was an open drift)

**Was.** The index existed and was tested, but `vector_search` always ran the
exact scan — nothing chose the approximate path. Wiring it needed a
cache-and-invalidate policy that no component owned.

**Now.** `kimmy_vector::IndexCache` owns that decision, held in `AppState` so
one graph is shared across requests. `access()` returns `Approximate` or
`Exact`, and both search endpoints dispatch through it.

The policy, and why each part of it is what it is:

| Question | Answer | Because |
|---|---|---|
| When is a graph worth building? | ≥ 2000 vectors | Below that, scanning beats building *and* walking a graph |
| How is staleness detected? | A per-collection generation counter, bumped on every vector write and delete | Counting is O(n); a count also cannot see a delete-then-add that leaves the total unchanged |
| When does a stale graph rebuild? | After 30s | Rebuilding per write would rebuild continuously under load, and each rebuild is O(n log n) |
| What happens if a build fails? | Fall back to the exact scan | An optimisation that cannot be built must not fail the query |

**Why bounded staleness is safe here, when a stale secondary index is not.**
The graph only supplies *candidates*. Scores are recomputed from the currently
stored vector, and a candidate whose record no longer exists is skipped. So a
deleted document cannot surface and an updated one scores by its new vector —
the only effect of staleness is that a document written in the last 30 seconds
may not be found yet. That is bounded recall loss on new data, never incorrect
data. `the_approximate_path_agrees_with_the_exact_one` asserts the two paths
return the same nearest neighbour with a byte-identical score.

**Still open:** no on-disk snapshot persistence — see below.

---

## 🟡 Vector indexes are rebuilt from scratch after a restart

The plan called for snapshotting the graph to `hnsw_snapshots` and replaying
newer vector-oplog entries on startup. Not built: the cache is in-memory only.

**Consequence.** The first search of a large collection after a restart pays a
full O(n log n) build, and until then queries are served by the exact scan —
slower, never wrong. Correctness does not depend on it, which is why it was
deferred rather than blocking the wiring.

---

## 🟢 `byo` now has an ingest route (was an open drift)

**Was.** `byo` — "the client supplies the vectors" — is the default provider,
and there was **no endpoint for supplying them**. Search on a `byo` collection
returned nothing, always. The only working path was writing raw records into
the shadow collection using the internal `VectorRecord` serde shape: the `#`
chunk separator in `_id`, the externally-tagged `DocId`, the internal HLC
shape. An implementation detail acting as a public contract.

**Now.** `PUT /v1/db/{db}/coll/{coll}/docs/{id}/vectors` taking
`[{chunk, vector, text}]`, with `GET` and `DELETE` alongside it. Replace-all
per document, matching what the embedding worker does — the only semantics that
stops a shortened document from leaving orphan chunks. The server supplies
`source` and `source_hlc` from the document it already holds, so no internal
shape crosses the boundary and staleness detection keeps working.

Requires `write` on the collection, and the document must exist: without it
there is no HLC for staleness to compare against.

**And the failure is no longer silent.** Searching a collection with no vectors
stored at all now returns `409 no_vectors` naming the remedy, rather than an
empty result that reads as "nothing matched". That was the more damaging half:
M3 exposed `vector_search` to agents, which would retry a query forever against
a collection that could never answer it.

**Shape chosen by the maintainer** rather than unilaterally, since it is public API.

---

## 🟢 A malformed shadow document no longer breaks search (was a live bug)

**Was.** A shadow collection is an ordinary collection, so anyone with write
access could insert a document into one. Any document that did not decode as a
`VectorRecord` made `for_each_vector` fail — turning **every subsequent search
on that collection into a 500**. One insert could brick search.

**Now.** Undecodable documents are skipped and logged at `warn`. Search stays
available; the operator still sees the problem. Same principle as the index
skipping records that no longer exist.

**Found by** the same manual verification as the entry above.

---

## 🔴 Index ranges use only one bound

**Cause.** A real bug, found by property testing: `{a: [2, 0]}` matches
`{$gte: 1, $lte: 1}` because *different array elements* satisfy each bound.
Intersecting both into one key range excluded the document.

**Fix applied.** Use one bound only, keeping the range a superset.

**Consequence.** `{qty: {$gte: 5, $lte: 9}}` scans the index from 5 upward
rather than stopping at 9. Correctness is intact; selectivity is not.

**To close.** Track multikey-ness per index, as MongoDB does, and use both
bounds for fields that never hold arrays. Touches the write path.

**Detail.** [Indexes](indexes.md).

---

## 🟡 `$in` does not use an index

Needs a *union* of point lookups rather than a single range. A query using
`$in` falls back to a collection scan. Common enough to be worth doing.

---

## 🟡 Ranges on descending index fields are ignored

The planner falls back to the equality prefix. Inverted encoding swaps which
end each bound belongs to, and getting it backwards produces a range that is
too **narrow** — silently wrong. Deliberately slow rather than possibly wrong.

**To close.** Implement the swap with its own property test.

---

## 🔴 The "no C toolchain" property has not held since M2

**Claimed.** ADR-001 chose redb over RocksDB and ADR-016 chose `rust_crypto`
over `aws_lc_rs` to keep the build free of a native toolchain. This register
repeated that as a live property, and the `local-embeddings` gating below cites
it as a reason.

**Actually.** `kimmy-vector` depends on `reqwest` with `rustls-tls`,
**non-optionally**, for the remote embedding providers — so
`reqwest → hyper-rustls → rustls → ring` has been in every default build since
M2. `ring` ships C and assembly and builds with `cc`.

**How it was found.** Planning TLS, by running `cargo tree -i ring` instead of
trusting this document. Two milestones of a stated property being false is
exactly the drift this register exists to catch, and it did not catch it — a
claim was recorded once and never re-checked against the build.

**Decided.** Accept the cost and correct the record, rather than gating
`reqwest` behind a feature. The individual choices in ADR-001 and ADR-016 were
still right; only the claim about the whole build was wrong. Recorded 🔴 rather
than 🟢 because what closes it is not code — it is *checking*, and there is no
mechanism that would catch the next such drift.

**The rule that replaces it.** Do not add a *second* native crypto stack.
`ring` is already paid for; `aws-lc-rs` would add CMake for the same primitives.
That is what selected the TLS provider in [ADR-039](decisions.md).

**To close.** A CI check that fails when the dependency graph gains a crate with
a `build.rs` invoking a C compiler. Then the property is enforced instead of
asserted.

---

## 🟡 Local embeddings are feature-gated, not the default

**Planned.** `fastembed` local ONNX as the zero-config default provider.

**Built.** Behind a `local-embeddings` cargo feature, off by default.

**Why.** Its dependencies pull native ONNX Runtime *and* OpenSSL, and roughly
triple the image. Raised and agreed before building.

**One of the original reasons no longer holds.** This was justified partly by
preserving a pure-Rust build, which the entry above shows was already untrue
when it was written. The decision still stands on the other reason: ONNX Runtime
is hundreds of megabytes of native binaries and a separate runtime, which is a
different order of cost from a crate that builds some C with `cc`. Downgraded
from 🟢 to 🟡 because the stated rationale was partly wrong, not the outcome.

**Consequence.** Out of the box, embedding needs a remote provider or
client-supplied vectors. `--features local-embeddings` restores it.

---

## 🟢 Unique indexes are single-node only by default

Uniqueness is a global invariant and provably not maintainable without
coordination. `local` enforcement is the default; `coordinated` is reserved and
refused until M4. Raised and agreed. See [ADR-020](decisions.md).

---

## 🟡 Not yet implemented, and known

| Gap | Consequence | Milestone |
|---|---|---|
| SRV discovery | `dns-srv:` parses but does not resolve: SRV records need a DNS resolver that can read record types the standard library does not expose. `dns:` and `k8s:` work | M4 |
| TLS between nodes | Replication frames are plaintext. `cluster_secret` authenticates peers but does not hide what they exchange | M5 |
| Client certificates (mTLS) | Server TLS authenticates the *server* to clients; clients still authenticate with a bearer token only | not planned |
| Certificate reload | A renewed certificate needs a restart to take effect | M5 |
| Rate limiting beyond login | Only `/v1/auth/login` is limited. Every other route is unbounded — see the entry below | M5 |
| Token revocation | Deleting a user does not invalidate issued tokens | not planned |
| Aggregation pipeline | `$group`, `$unwind`, etc. absent — including the `$vectorSearch` stage, so search is endpoint-only, **and the planned MCP `aggregate` tool, which has nothing to expose** | M5 |
| Backup / restore | Cold file copy only | M5 |
| Multi-document atomicity | A batch update can be partially applied | by design |
| Benchmarks | Partial. The vector-index constants and the write path are measured ([Benchmarks](benchmarks.md)); the planner, `MAX_LIMIT` and concurrent writers are not, and there is no regression baseline | M5 |
| Vector reindex operation | Changing model or dimension needs a disable-with-`drop_vectors` and re-enable, which backfills from the oplog | M5 |

---

## 🟡 Rate limiting covers login only

**Built.** A token bucket per key, on `/v1/auth/login`, keyed on the client
address. Closes the 🔴 that made a password guessable at network speed.

**Deliberately not more than that.** On login a limit is a *security* control:
the route is unauthenticated by necessity, and every attempt runs a full
Argon2id verification — including for a user that does not exist, since
equalising that is what stops timing from revealing whether one does. At the
configured work factor an unthrottled endpoint hands an anonymous caller ~19 MB
and milliseconds of CPU per request.

Everywhere else a limit would be a *capacity* control, and a capacity number
picked without a measurement is a guess of exactly the kind M5's benchmarks
exist to remove. Agreed with the maintainer: build the mechanism route-agnostic, apply it
where it is a security property, and let the benchmark work decide the rest.

**To close.** `kimmy_api::Limiter` takes an arbitrary key and knows nothing
about login, so another route is a field on `RateLimits`, a config knob, and a
`check_at` call — in the handler when the key depends on the body, or in a
`tower` layer when it is just the caller.

---

## 🟡 Per-username login limiting is off by default

`login_per_user` is implemented and defaults to `0`, which disables it.

**Why it exists.** It is the only defence against a brute force spread across
many source addresses, which per-address limiting cannot see.

**Why it is off.** It introduces a lockout: anyone who can reach the endpoint
can spend a *named* user's budget and keep the legitimate holder out for the
rest of the window. Enabling it trades a remote-guessing risk for a
denial-of-service one, and which of those matters more is a property of a
deployment, not something a default should assume. Turning it on is one config
value, and the behaviour is tested either way.

---

## 🟡 A shared egress address shares a login budget

Per-address limiting keys on the peer address, so callers behind one NAT or one
egress gateway draw on one budget. An address that is over its budget is refused
**even with correct credentials** — the check has to precede authentication or
it would not prevent the Argon2 work it exists to prevent.

**Consequence.** On a shared egress, one client guessing passwords can lock out
its neighbours for the rest of the window. Raise `login_per_ip`, or set
`trusted_proxy_header` so the limiter sees the real client.

**Not closed by** trusting a forwarded header by default — that is client-
supplied data, so trusting it unasked would let anyone defeat the limit by
varying a header, which is worse than having no limiter, because it would look
like one was working.

---

## 🟢 Closed

**Collection ids above `i64::MAX` broke replication.** A live bug, not a
deferral, and the most serious thing found since the collection-id fix itself.
Ids are derived by hashing, so they use the whole `u64` range; BSON has no
unsigned 64-bit type; and `CollectionId` used a derived `Serialize`. Any id in
the upper half — **about 48% of collection names** — could not be encoded, so
every oplog entry naming that collection was unsendable and the collection
never replicated. The write succeeded locally; the peer logged one
`malformed frame` warning per round.

Found by running three containers, not by the suite, which used a single
collection name that happens to hash low. Fixed by giving `CollectionId` one
fixed representation (bit-cast to `i64`), matching what `NodeId` already needed
for the same class of bug. On-disk format untouched — ids are persisted by the
hand-rolled codec, not serde. [ADR-031](decisions.md).

**SWIM was silently degraded under the shipped container defaults.** Not a code
bug: `cluster.bind` defaults to a wildcard, and the node correctly refuses to
advertise one, falling back to loopback with a warning ([ADR-037](decisions.md)).
But `docker-compose.yml` — the documented way to run a cluster — never set a
per-node bind, so all three nodes advertised `127.0.0.1` and gossip never
formed. Replication still converged via discovery, which is what made it look
fine. Compose now pins a subnet and a per-node address; Kubernetes uses the
downward API. Verified: both survivors now declare a killed node down within
17 ms of each other, where before nothing was ever declared down.

**TLS for clients.** Was 📋 M5 and the reason "terminate at a proxy" appeared in
every deployment note: tokens and passwords crossed the wire in plaintext
otherwise, including the bootstrap login that sets the first password. The
listener now terminates TLS itself — `axum-server` over `rustls`, on the `ring`
provider already in the build. Set `server.tls.cert_file` and
`server.tls.key_file`. [ADR-039](decisions.md). A proxy is still a fine
deployment; it is no longer the only one. **Node-to-node replication is still
plaintext** — that is a separate piece with its own trust question, and it stays
📋 in the table above.

**Login rate limiting.** Was a 🔴 in [Security](security.md) and a 📋 in this
register: `/v1/auth/login` had no limit, so a password was guessable as fast as
the network allowed, and each guess cost a full Argon2id hash. Now a token
bucket per client address, refusing with `429` and a `Retry-After` **before**
authentication runs. Only failed attempts are recorded, so a client with correct
credentials is never throttled for succeeding. What remains is scope, not the
mechanism — see the three 🟡 entries above.

**Oplog and tombstone GC.** Was the most serious 🟡 in this register —
retention was configured but not enforced, so both tables grew without bound.
Enforced now by a background pass every `storage.gc_interval_secs`. The design
constraint that mattered was not the collection itself but that **the newest
oplog entry must never be collected**: the logical clock resumes from the oplog
tail, so an aged-out tail would reset the clock on restart and make every later
write lose to its own older version, silently. [ADR-028](decisions.md).

---

## 🟢 Deliberate departures in M3

**`kimmy-mcp` depends on `kimmy-api`, not the reverse.** The crate graph
carried a placeholder arrow from M0 pointing the other way. Inverted so both
edges share one executor with the authorization check inside it; the
alternative was duplicating Extended JSON conversion, the query planner path,
and vector search dispatch into a second crate. [ADR-024](decisions.md).

**Tools are not filtered by grant.** A read-only token sees every write tool and
is refused when it calls one. Hiding is not an enforcement boundary, and a
filtered list makes refusals unexplainable. [ADR-025](decisions.md).

**`rmcp`'s `Host` allow-list is off by default.** It is DNS-rebinding
protection for unauthenticated local servers; `/mcp` verifies a bearer token
before the transport runs. The SDK default would have rejected every client
connecting by a real hostname. Operators can re-enable it via
`server.mcp_allowed_hosts`. [ADR-026](decisions.md).

**MCP resources exclude `__kimmy` and `.__vectors`.** A resource is material an
agent attaches to its context, and the user store is a column of password
hashes. Tools still reach them under the ordinary access check, so this is a
default rather than a control. [ADR-027](decisions.md).

**Sessions are disabled.** Stateless, so a token that expires mid-conversation
stops working rather than riding an already-open session. The cost is that a
long-running agent must re-authenticate.

---

## 🟡 Simplifications inside working features

**Keyword search is term overlap, not BM25.** It exists to give hybrid search a
lexical signal, and RRF only uses the *ordering*, so absolute scores need not
be principled. A real BM25 would rank better on its own.

**Chunking counts characters, not tokens.** A token count depends on the
model's tokenizer, which the storage layer has no business knowing. The default
(2000 chars ≈ 512 tokens) is conservative and can overshoot for dense text.

**No minimum score threshold on search.** k-NN returns the `k` nearest even
when nothing is genuinely similar, so a query against unrelated content still
returns results with near-zero scores. Callers must threshold themselves.

**`skip` is O(n)** even with an index. Deep paging stays expensive.

**Result order without an explicit `sort` is unspecified**, and differs between
an index-backed query and a scan. Matches MongoDB, still a footgun.

---

## 🟢 Replicated writes now reach change streams (was the 🔴 M4 blocker)

**Was.** An applied remote entry keeps its originating stamp, so it entered the
oplog *behind* the local tail and a subscriber past that point never saw it.
Single-node streams were unaffected, which is why it had not bitten.

**Now.** A second ordering — `oplog_arrival` — over local arrival sequence, with
the oplog still keyed by origin stamp for conflict resolution and anti-entropy.
The maintainer chose this over restamping on arrival or documenting the limitation.
Resume tokens are unchanged; they are translated to an arrival position at watch
time, because tokens live in clients where no migration can reach them. Detail
in [Oplog](oplog.md).

**Two bugs closed on the way.** Streams de-duplicated by comparing stamps, which
discarded exactly the replicated entries this was meant to deliver; and they
trusted publication order, which can differ from commit order under concurrency.
Both dissolved once the broadcast became a wake-up rather than a data path —
[ADR-030](decisions.md).

---

## How to use this document

When something here is closed, move it to 🟢 with a note on what changed —
don't delete it. The record of *why* a thing was once wrong is worth more than
a clean list.

When a new deferral is made, add it **at the time**, not later. Every 🔴 above
became one because it was recorded somewhere local and never surfaced.
