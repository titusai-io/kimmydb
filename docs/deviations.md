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
| When is a graph worth building? | ≥ 500 vectors | **Measured.** Originally 2000 on the assumption that a scan wins below some size; it never does. 500 is where one build repays itself in ~12 queries ([Benchmarks](benchmarks.md)) |
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

## 🟢 Index ranges use both bounds on scalar-only indexes (was the register's last 🔴)

**Was.** A real bug, found by property testing: `{a: [2, 0]}` matches
`{$gte: 1, $lte: 1}` because *different array elements* satisfy each bound.
Intersecting both into one key range excluded the document. The fix used one
bound only — correct, but `{qty: {$gte: 5, $lte: 9}}` scanned the index from 5
upward rather than stopping at 9, and this register carried it as its only 🔴
from M1 to M7.

**Now.** Multikey-ness is tracked per index, as MongoDB does: a `multikey`
flag on the definition, set in the same transaction as the index entries the
first time any document contributes more than one key — by write, by backfill,
or by applying a peer's write. Scalar-only indexes get both bounds; multikey
ones keep the one-bound superset. One-way, because clearing it safely is a
full scan for the sake of a planner hint. [Indexes](indexes.md).

**Two things the closing surfaced, both fixed in the same change:**

- A plan that intersected both bounds is only sound for the snapshot whose
  flag approved it — so the scan **re-validates the flag in its own snapshot**
  and falls back to a collection scan if a write flipped it in between. A
  stale `false` proving nothing about the present is the same lesson the
  register's native-toolchain entry teaches about claims generally.
- **Index maintenance trusted the caller's index list**, so a write through a
  `CollectionMeta` handle fetched before an index existed silently skipped
  that index — no entries, no unique check. Found by this branch's own tests
  tripping over it. Maintenance now re-reads the definitions inside the
  write's transaction.

---

## 🟡 `$in` does not use an index

Needs a *union* of point lookups rather than a single range. A query using
`$in` falls back to a collection scan. Common enough to be worth doing.

---

## 🟢 Ranges on descending index fields are planned (was a 🟡 deferral)

**Was.** The planner fell back to the equality prefix. Inverted encoding swaps
which end each bound belongs to, and getting it backwards produces a range that
is too **narrow** — silently wrong. Deliberately slow rather than possibly
wrong, until the swap had its own tests.

**Now.** The swap is implemented: a descending component's inverted bytes
reverse order, so the value-space lower bound caps the key-space *top*. Held
down three ways — encoded-key assertions on the planner (the key for a value
inside the range falls inside the plan's bounds, outside falls outside),
equivalence and selectivity tests against a real engine including the compound
ascending-prefix-descending-range shape, and the equivalence property test,
which already generated descending indexes and two-sided ranges and began
exercising this path the moment the planner stopped refusing it. Multikey
descending indexes use one bound, exactly as ascending ones do — the multikey
rule is about the data, not the direction.

---

## 🟡 The "no C toolchain" property has not held since M2 — now enforced

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

**Closed as far as it can be.** `scripts/check-native-deps.sh` runs in CI and
fails when the default build gains a package matching a native-toolchain
indicator — `cc`, `cmake`, `bindgen`, `pkg-config`, `*-sys`, `*-src` — that is
not on `scripts/allowed-native-deps.txt`. The allowlist currently holds one
entry, `cc`, with the reason beside it.

Downgraded from 🔴 to 🟡 rather than 🟢 because the *claim* was wrong for two
milestones and nothing brought it back to true; what changed is that the next
such drift now fails a build instead of sitting in prose. Adding native code is
still allowed — it just has to be a deliberate line in a diff.

**Worth noting about the check itself:** its first version exited 1 for the
wrong reason. With `set -e`, an allowlist holding only comments made `grep`
return non-zero and killed the script *before it printed anything* — a passing
failure, which is precisely the class of bug this register exists to catch. It
was found by running the failure path rather than only the happy one.

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
| Client certificates (mTLS) | Server TLS authenticates the *server* to clients; clients still authenticate with a bearer token only | not planned |
| Certificate reload | A renewed certificate needs a restart to take effect | M5 |
| Rate limiting beyond login | Only `/v1/auth/login` is limited. Every other route is unbounded — see the entry below | M5 |
| Token revocation | Deleting a user does not invalidate issued tokens | not planned |
| `$vectorSearch` as a pipeline stage | The pipeline is built, but vector search stays its own endpoint | M5 |
| Computed expressions in the pipeline | `$add`, `$concat`, `$cond` and friends. Accumulator arguments are a field path or a literal | not planned |
| Multi-document atomicity | A batch update can be partially applied | by design |
| Benchmarks | Partial. The vector index, the write path and the planner are measured ([Benchmarks](benchmarks.md)); concurrent writers and batched writes are not, and there is no regression baseline | M5 |
| Vector reindex operation | Changing model or dimension needs a disable-with-`drop_vectors` and re-enable, which backfills from the oplog | M5 |
| Webhook ownership hashes addresses | Rendezvous hashing takes the `SocketAddr` SWIM publishes, so re-addressing a node reshuffles its subscriptions — the same disruption as that node leaving and another joining. Hashing node ids would be stabler and needs a member → node-id mapping | M6 |

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

**The three findings from the M6 code review.** Recorded 2026-08-10, closed in
the M7 warm-up branch:

- **Webhook events carried empty `database` and `collection` fields.**
  `render_without_document` emitted both as `""`, always — for the whole of M6,
  with every test passing, because every test asserted on fields that were
  right and none looked at these two. They are now filled from the
  subscription, the delivery test asserts them **on the wire** rather than on
  `render`'s output, and the docs show them. The lesson is the fixture one
  again, in a new shape: a payload assertion that lists what must be present
  says nothing about what else is being sent.
- **The egress check and the dial resolved DNS separately.**
  `EgressPolicy::check` resolved and checked every address — and then `reqwest`
  resolved *again* to connect, so a rebinding attacker with a zero TTL could
  pass the check and flip the record before the dial. The documented rule
  ("checked before each delivery") was true; it just checked addresses nobody
  was obliged to dial. Closed by `CheckedResolver`: the delivery client's own
  DNS resolver runs `permits_addrs` on what it resolves, so the approved
  addresses are, by construction, the dialled ones. The pre-delivery `check`
  stays — it is what refuses literal addresses, which never reach a resolver.
  Found on the way: the client was built with `unwrap_or_default()`, whose
  fallback is a default client that follows redirects and resolves unchecked —
  a build failure would have silently shed both egress protections. Now the
  dispatcher refuses to start without its client, loudly.
- **Backoff state outlived its subscription.** Failure state was only cleared
  by a successful delivery, which a removed subscription can never have. The
  map is now pruned against the registry each pass.

**The webhook "saturation" risk did not exist, and the real one was its
opposite.** M6 task notes and the roadmap's open questions both asked for a
"per-node delivery cap, so a webhook on a hot collection cannot saturate a
node's outbound connections". That risk was never reachable. `dispatch_once`
delivered inside a `for` loop with one `.await` per subscription, so the node
had **at most one HTTP request in flight at any moment**, no matter how many
subscriptions existed or how hot the collection was. A bound of one cannot be
exceeded, so a cap would have capped nothing.

The premise came from reasoning about the design as described — "the dispatcher
is an oplog consumer that dials out for each owned subscription" — rather than
from the loop as written. Nothing in the code contradicted it out loud, and the
serial `.await` is one character of syntax, so it survived three branches and a
plan review.

What the serial loop *did* cause is the inverse failure, and a worse one: one
endpoint that stopped answering held the entire pass for the full ten-second
`DELIVERY_TIMEOUT`, delaying **every subscription queued behind it in the loop**.
That is exactly the cross-subscription interference the per-subscription
`Backoff` was built to prevent, reappearing one layer up — and it means a
webhook the operator does not control decides when the ones they do control
fire. Backoff hid how bad it was, because a repeatedly-failing endpoint is
skipped after its first failure; the lasting damage is from an endpoint that is
*slow* rather than dead, which never backs off and pays the timeout every pass.

Both are now addressed by the same change: deliveries run concurrently under a
semaphore of `webhooks.max_concurrent_deliveries` (default 8). Concurrency
removes the head-of-line blocking; the bound makes the original request true
rather than vacuous, since there is now something that could saturate. See
[ADR-045](decisions.md) and `a_slow_endpoint_does_not_hold_up_another_subscription`.

**The lesson worth keeping:** a risk asserted in a plan is a hypothesis about
the code, not a fact about it. This one was written down three times and
reviewed each time without anyone reading the loop. Before building a mitigation,
confirm the failure it mitigates can actually occur — the check costs one
reading and would have reframed the whole task.

**Backup and restore.** The only answer was a cold file copy, which meant
stopping the node — and copying `kimmy.redb` from a running one captures a torn
file. Now `GET /v1/admin/backup` takes a consistent snapshot inside a read
transaction while the node serves, and `kimmyd restore --from` writes it into a
fresh data directory. [ADR-041](decisions.md). The residual sharp edge is
inherent: a backup carries the node's identity, so restoring one onto two nodes
gives them the same id.

**TLS between nodes.** Replication frames were plaintext; `cluster_secret`
authenticated peers without hiding what they exchanged. Now TLS, always on,
with the handshake bound to the secret through the TLS exporter rather than to
a certificate — so there is no PKI to run and a man-in-the-middle cannot relay
the handshake. [ADR-040](decisions.md). The residual limit is unchanged and
inherent: a node's identity is still only "holds the cluster secret".

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
