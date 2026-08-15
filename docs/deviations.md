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

## 🟢 Vector indexes survive a restart (was a 🟡 deferral since M2)

**Was.** The cache was in-memory only: the first search of a large collection
after a restart paid a full O(n log n) build, served by the exact scan until
then. Slower, never wrong — which is why it waited from M2 to M8.

**Now.** Every successful build is persisted beside the database file
(`<data_dir>/hnsw/<collection-id>/`, staged and renamed so a crash mid-save
leaves the previous snapshot rather than a torn one), and a process's first
look at a collection loads the snapshot before paying the build. Validity
across a restart cannot use the generation counter — it is in-memory and
resets — so the check is the vector *count* the snapshot covered: equal
counts adopt it as fresh; unequal counts serve it once (bounded recall loss,
instantly) and rebuild on the next access. The corner accepted, on purpose: a
delete-and-add while the node was down leaves the count equal, and that
snapshot serves as fresh until the next vector write — the same class of
bound as the 30-second staleness window, on a longer clock. A restore does
not carry snapshots and rebuilds, which is always correct.

**Found on the way, by testing the failure path:** `hnsw_rs` *panics* on a
corrupt graph file — an `unwrap` on its magic check — so the load runs under
`catch_unwind`, and a torn file on disk costs a rebuild rather than the
process. The same species as `DistDot`'s assert in M2: the library's failure
behaviour was checked rather than trusted, and it needed containing.

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

**Shape chosen by the maintainer** rather than unilaterally, since it is
public API.

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

## 🟢 `$in` uses the index (was a 🟡 deferral)

**Was.** `$in` needs a *union* of point lookups rather than a single range,
so it fell back to a collection scan — the most common operator to do so.

**Now.** An `IndexPlan` carries a list of `(lower, upper)` ranges rather than
one: a single entry for a contiguous range, one per distinct value for `$in`.
The executor scans each and unions the candidates, deduplicating by document
key — one document can appear under two probes when an array holds two listed
values. Values are deduplicated on the *encoded* key, so `[5, 5.0]` probes
once, exactly as the index collapses them. Probes are equalities, so they are
sound on multikey indexes with no flag interaction, and an empty `$in` plans
an empty union — zero probes for a filter nothing can match. Visible in
`explain` as `"strategy": "indexUnion"` with a `"probes"` count.

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

## 🟢 Bulk insert exists (was a deferral the register never wrote down)

**Was.** `POST /docs` took one document per request, so loading N documents
cost N durable commits. Carried as known debt since M1 and named in the M8
plan — but, found while closing it, **the register never held an entry for
it**. The handoff's debt table pointed at a 🟡 in this file that was not here.
Recorded now on the way out, because the gap in the register is the more
useful lesson: a debt tracked only in a document that gets rewritten each
branch is a debt that can quietly stop being tracked.

**Now.** `POST /v1/db/{db}/coll/{coll}/bulk` takes an array and writes it in
one transaction, capped at 1000 documents ([ADR-048](decisions.md)). A document
inside a batch of 1000 costs about 1/175th of one inserted alone — 291
documents/sec becoming ~51,300 — because the durable commit was very nearly the
whole cost, which is what M8 task 3's flat concurrent-writer curve predicted.

**Consequence.** Bulk insert is atomic and *says* it is atomic, which is a
stronger promise than `update` and `delete` make. That asymmetry is now a
documented part of the surface rather than an accident: see the row below.

---

## 🟢 A renewed certificate no longer needs a restart (was a 🟡 deferral since M5)

**Was.** TLS certificates were read once, before the socket was bound
([ADR-039](decisions.md)), so rotating one meant restarting the node.

**Now.** SIGHUP, or a change to either file noticed within 60 seconds, swaps
the certificate on a running node ([ADR-049](decisions.md)). Both triggers
exist because neither covers both deployments: there is no convenient way to
signal PID 1 of a Kubernetes pod, and a poll alone leaves an operator who has
just replaced a file waiting out the interval.

**Almost none of this was new machinery.** `axum-server` already held the
certificate behind a handle the acceptor reads per handshake and already
exposed a reload; what was missing was something to call it. The branch is
therefore about triggers, and the ADR is mostly about why there are two.

**The startup rule and the reload rule now differ, on purpose.** A bad
certificate at startup is still fatal — there is nothing to fall back to. A bad
certificate at reload is refused and the one already serving stays, because
there is. Verified on a live node: a truncated certificate and a
certificate-without-its-key each left the node serving and healthy, counted a
failure, and the following trigger completed the rotation.

**To close, still open:** a failed reload is visible as
`kimmy_tls_reloads_total{outcome="failed"}`, but a certificate **nobody ever
tried to rotate** reports nothing, because no reload was attempted. That wants
`kimmy_tls_cert_expiry_seconds` parsed from `notAfter` — the metric actually
worth alerting on. Left out here because it needs `x509-parser` as a new
runtime dependency, and a branch about triggers should not also be a branch
about dependencies. See the row below.

---

## 🟢 SRV discovery resolves (was a 🟡 deferral since M4)

**Was.** `dns-srv:` parsed and was tested, and then returned an error when
asked to resolve. The standard library resolves *names*, not record types, and
SRV's whole purpose is the port inside the record — so this needed a resolver
crate, and the open question was never difficulty but which dependency.

**Now.** `hickory-resolver`, with `default-features = false` and exactly
`system-config` + `tokio` ([ADR-050](decisions.md)). The feature list is the
decision: every transport beyond plain DNS — DoT, DoH, QUIC, h3 — and DNSSEC
each ship a `-ring` and an `-aws-lc-rs` flavour, and the aws-lc-rs ones would
add CMake for primitives `ring` already provides. `check-native-deps.sh` still
reports `cc` alone, and the default build still has no `aws-lc-rs` and no
`openssl` — checked, not asserted, since the last prose claim about this was
false for two milestones before anyone noticed.

**Verified on two live nodes on ports 7911 and 7922**, neither the 7900
default, seeded only by `dns-srv:` against a real DNS server. That arrangement
is the point: with both on the default port, an implementation that resolved
the names and ignored the ports would have looked identical.

**Found while building it:** the resolver reports an empty answer as an
**error**, and the discovery loop warns on every error once per tick. A name
with no SRV records yet is what a cluster looks like before its first node
registers — so the obvious mapping would have logged a warning every few
seconds forever while nothing was wrong. `NoRecordsFound` is translated to an
empty set, with a test on it.

---

## 🟢 Webhook ownership follows the node, not the address (was a 🟡 deferral since M6)

**Was.** Rendezvous hashing took the `SocketAddr` SWIM publishes, so
re-addressing a node reshuffled its subscriptions. The register accepted that
as "the same disruption as that node leaving and another joining."

**That justification was wrong, and measuring it is what showed it.** Over
100,000 subscriptions across three members: a node genuinely leaving moves
**25.0%**, while re-addressing one node moves **50.8%** — twice as much,
because it is a departure and an arrival at once. The old address's share
scatters, then the new address claims a fresh share from everyone. For a node
that never went anywhere, which in Kubernetes is what a rescheduled pod is.

**Now.** The node id travels inside the SWIM identity foca already gossips, and
ownership hashes that ([ADR-051](decisions.md)). The id lives in the database
file, so it survives restarts and moves with a restore. Driven live: a node
moved from port 8203 to 8303 with the same data directory, and **all twelve
subscriptions kept their owner** while the cluster reformed and replication
continued.

**Found while building it — the M8 plan's expectation was wrong.** The plan
said a mixed-version cluster "produces duplicates, which at-least-once
tolerates". foca encodes identities with postcard, which is not
self-describing, so the added field is a wire break: a new node **rejects** an
old identity outright. A mixed-version cluster does not double-deliver, it
fails to form membership. That makes this a stop-the-cluster upgrade, like
ADR-040's, and it is recorded in [Operations](operations.md) beside it.

---

## 🟢 Tokens can be revoked (was 🟡 "not planned")

**Was.** Deleting a user, disabling one, or narrowing a role did nothing until
the token expired — up to an hour. The register listed rotating
`KIMMY_JWT_SECRET` as the only remedy, which logs out the entire cluster.

**Now.** The user record carries a token version and the token carries the one
it was issued under; a mismatch is a 401 ([ADR-052](decisions.md)). Deleting or
disabling a user needs no version at all — the absent or disabled record is the
refusal. Changing a password or grants bumps it.

**The grants half is the one that mattered.** Grants ride inside the token, so
a permission that was *taken away* kept working for the rest of the token's
hour. That was the actual exposure, rather than the untidiness of a deleted
account lingering.

**Verification is still a pure function.** `TokenIssuer::verify` does no I/O:
the version check lives in the `Auth` extractor, which already holds state, and
reads through a per-node cache that an oplog consumer keeps honest. Reading the
user record per request would have put a storage read on a path that had none.

**Found while building it — the consumer alone fails quietly.** The integration
tests build a router without spawning background tasks, and happily kept
honouring revoked tokens until the admin routes were changed to evict
synchronously as well. Correctness that depends on remembering to spawn a task
is the same shape as the M6 webhook bug. Now a single node is correct with no
background task, and a missing consumer delays *cluster-wide* revocation rather
than disabling revocation.

**Measured on two live nodes:** the node taking the change refuses immediately,
the other within about two seconds.

---

## 🟢 M8 closed with a mutation pass over the whole milestone diff

**227 mutants, 47 escapes, 32 closed.** `cargo-mutants --in-diff` over
everything M8 changed, run per crate so each mutant scores against a fast
relevant suite. Seventeen new tests killed 31; one restructuring removed
another by construction. The full account, including why the remaining 15 are
left alive, is in [Testing](testing.md).

**The M7 lesson held again: escapes cluster in new callers, not new logic.**
`backfill_from_entry` — task 5's addition — inherited the retry classification
written for the streaming path, and nothing exercised it. Forced one way a
permanent failure retries forever and the whole scan stalls on one document;
forced the other, a transient blip silently drops one. The fake provider could
only fail *retryably*, so neither direction was reachable.

**Worst coverage was `kimmy-core`: 8 escapes out of 9.** All of them in the
provider config added by task 6. The dialects were verified against documented
shapes with fixtures over in `kimmy-vector`, and that left the type beside them
— wire tags, default key variables, validation for Cohere and Gemini — with no
tests at all. It now scores 9 of 9.

**Two escapes were closed by changing the code rather than by adding a test.**
`should_reload` was extracted out of the certificate-reload loop so the
decision could be tested at all, and the `!` at its call site was removed by
inverting the condition — a mutant that cannot be written is better than one a
test has to chase.

**Four were proven equivalent rather than chased**, per the standing rule.
`hlc > held` → `>=` in `lag_behind_ms` contributes `wall_ms − wall_ms` = 0 when
the two are equal, which cannot change a `max()` that already defaults to 0;
and the node-id tiebreak in `win_addr_conflict` is *deliberately* arbitrary, so
flipping its direction preserves the only property that matters — that every
node computes the same answer.

---

## 🟢 SWIM membership is authenticated (was an open drift, found by driving a cluster)

**Was.** Only the TCP replication handshake verified `cluster_secret`;
`membership.rs` never referenced it. A node holding a *different* secret joined
a real cluster's SWIM member set. Replication rejected it correctly and it
could read nothing — so it looked contained.

**It was not contained.** Webhook ownership is rendezvous-hashed over the live
member set, so the impostor became an ownership candidate, won roughly
`1/(N+1)` of subscriptions, and delivered none of them. Measured on a
three-node cluster with twelve subscriptions: **eight delivered, four delivered
nothing**, with every real node believing it had correctly stood down. The same
failure shape as the M8 task-1 bug, reached from the other side.

**The realistic trigger is a rolling `cluster_secret` rotation**, not an
attacker — which is what makes it likely rather than theoretical.

**Now.** Every membership datagram carries `HMAC-SHA256(cluster_secret,
payload)` as a 32-byte prefix, verified in constant time before foca sees it
([ADR-053](decisions.md)). Rejections are counted and logged at powers of two,
so a misconfigured peer says so once rather than every probe interval.

**Found by driving a five-node cluster, not by the suite.** Every automated
test builds its cluster with one shared secret, so nothing ever presented a
wrong one. There is now an integration test that does.

**A wire break, and the third of its kind** — nodes must be upgraded together,
like ADR-040 and ADR-051. Recorded in [Operations](operations.md).

**Not fixed here: the datagrams are still readable.** Authentication was the
missing property and the one the failure needed; encryption needs a key
schedule, nonces and replay handling, and would not have changed the outcome.
Membership topology remains visible to anyone on the path — see the row in
[Security](security.md).

---

## 🟢 The HTTP reference lists every route again

**Was.** `docs/http-api.md` presents a table titled **Endpoints** that reads as
complete. It was missing **six of twenty-eight routes**: `aggregate`, `vector`
(configure), `vector_search`, `hybrid_search`, and both `webhooks` routes. Each
was documented on its own page, so nothing looked wrong — a reader of the API
reference simply could not learn they existed.

**Found the same way**, by reaching for `/vector_search` while driving a
cluster and not finding it in the reference.

**Now.** All twenty-eight are listed, each linking to its own page for detail.
And the claim is **checked rather than asserted**:
`every_route_is_in_the_http_reference` parses the router's own source and fails
naming any route absent from the document — the lesson M8 spent twelve tasks
relearning.

**Corrected while there:** the same document's coverage table said node-to-node
replication was "still plaintext", twenty lines above a section explaining that
replication runs over TLS always. It had been wrong since ADR-040.

---

## 🟢 A converged cluster stops re-syncing, and the lag gauge stops lying

**Was.** The version vector advances only when an oplog entry is appended, but
`behind` and `lag_behind_ms` both read it as "what I have seen". Two things a
node processes correctly append nothing: a document that **loses
last-writer-wins**, and an entry the sender holds but never ships — a
`UniqueViolation`, which is logged locally so it reaches change streams but is
excluded from `entries_for_peer` by design ([ADR-029](decisions.md)).

Either leaves a permanent hole, and `behind` then re-requests everything from
that point on every round, forever.

**The universal trigger is the bootstrap user.** Every node creates its own
`__users` collection and inserts `root` locally. Each peer's `root` insert
loses last-writer-wins on arrival — so **every cluster ever run** has had a
permanent hole from the moment it formed.

**Measured on an idle, fully converged three-node cluster:** 60 merge rounds in
20 seconds with no user data at all, each re-fetching and re-applying the same
entries. The re-sent range grows with everything written after the hole, so the
cost grows with the database. After the fix: **zero**.

**The visible symptom was the gauge.** After concurrent updates to one
document, `kimmy_replication_lag_seconds` pinned at **1204 seconds** on all
five nodes of a converged idle cluster and stayed there — clearing only when a
winning write from each origin finally advanced past the discarded stamps.
Operators are told to alert on this metric and that zero is the caught-up
steady state, so it was both a permanent false alarm and a real signal nobody
could distinguish from one.

The gauge stayed silent in the universal case only because `lag_behind_ms`
ignores an origin it has never seen — the "fifty-year lie" rule. So the loop
ran everywhere while the metric read zero.

**Now.** A second durable vector records what has been **processed**, and
`behind`/`lag` read that; the oplog-derived vector keeps its meaning — what
this node can *serve* — and is still what a peer receives
([ADR-054](decisions.md)).

**Found by driving a cluster, and only after fixing my own test.** The earlier
evaluation reported this area clean because its conflict test used `PUT`
without `?upsert=true` and therefore wrote nothing at all. A test that asserts
five nodes agree can pass because they agree on an error.

**Corrected during the work:** the first draft of ADR-054 claimed replicated
DDL was one of the unlogged paths. It is not — `apply_ddl` appends the
originating entry deliberately, to advance the vector. Reading the code rather
than trusting the draft is what found the real universal trigger.

---

## 🟢 A collection can be paged without `skip` (M9 task 5)

**Two decisions settled before code.** The token is **opaque**, following the
`ResumeToken` convention next door — a client treats it as a blob, so the
encoding can change without breaking anyone. And a cursor pages in **`_id`
order only**, because that is the order both access paths already produce.

**The implementation is smaller than the feature**, which is the point. A
cursor is the encoded document key of the page's last row, and nothing else.
`keyenc` is order-preserving, so byte order *is* `_id` order — "the next page"
becomes a range bound the storage engine already knows how to take. No new
comparison logic, no sorting, no server state, and portability between nodes
falls out because the token is a pure function of the `_id`.

The honest numbers, paging 1,000,000 documents at 100 per page: `skip` visits
~5×10⁹ documents, a cursor ~10⁶.

**Arbitrary sort keys were deliberately excluded**, and the reason is worth
keeping. A token *can* bound the start of a sorted scan — but index candidates
come back in document-key order (`scan_range_in` ends with `out.sort()`), so a
sorted page still materialises and sorts whatever remains. That is roughly 2×
better than `skip` rather than a fix, and shipping it would have promised
efficient sorted paging while delivering a constant factor. Making sorted
paging genuinely cheap needs index-ordered scans, which would also make every
sorted `find` cheaper — it is the obvious next thing, and it is not in M9.

**A design fault was found by writing the test.** The first draft returned
`nextCursor` only when a cursor had been *sent*, so the test had to invent a
magic first-page constant to start a walk — something no client could have
discovered. `nextCursor` is now offered whenever the page filled and the query
is one a cursor can continue, so a client's first request needs no cursor and
no flag. The test that needed a magic constant was the design review.

---

## 🟢 Partial indexes exist, and sparse falls out of them (M9 task 4)

**Two decisions settled before code.** Partial only — a sparse index is
`{field: {$exists: true}}`, which is where MongoDB has been steering for years,
so there is one mechanism rather than two overlapping ones. And the partial
filter language is **deliberately bounded**: `$exists: true`, equality, the four
comparisons against a literal, conjunction. Nothing else.

**The bound is the safety property, not a shortcut.** A partial index may answer
only a query provably contained by its filter, and general implication between
filters is not decidable — so a general language forces a best-effort
containment check whose mistakes return a *subset* with nothing to indicate it.
That is the multikey failure again, and it is the one this codebase is least
able to notice. Restricting the language makes containment a decision. The
refusal lands at index creation, where an operator can act on it, rather than at
query time where the symptom would be a plan that quietly stopped applying.

**Two things were verified rather than assumed**, both of which would have been
silent bugs:

- **`bson::Document` round-trips losslessly through the collection metadata's
  JSON**, as canonical Extended JSON — checked with a date and an integer above
  2^53 before choosing to store the filter as a document. The standing rule is
  that a type crossing a format boundary needs a *chosen* representation, and
  `NodeId` has cost a replication outage by inheriting one.
- **A comparison never matches an absent field**, because `condition_matches`
  evaluates over resolved values that are empty when the path is missing. That
  is what makes `$gt`/`$lt` safe to treat as proving existence. The exception is
  **`{a: null}`, which matches an explicit null *and* a missing field** — so it
  contributes nothing to containment. Answering it from a presence-filtered
  index would drop exactly the documents it is meant to find.

**`impl Eq for Bson`** exists in `bson` despite the `f64`, which is what lets
`IndexMeta` keep its derives with a `Document` inside. Checked, because the
obvious assumption was the opposite.

---

## 🟢 `update` and `delete` use the planner (was a 🟡 raised one branch earlier)

**Raised while building `find_and_modify`, fixed here.** `exec::update` and
`exec::delete` collected their targets with `for_each_doc` — a straight
collection scan — so **an index never sped up a filtered update or delete**,
however selective the filter was. Both now go through `collect_matching`, the
same helper `find` uses, which brings the index plan, the `$in` union with its
per-document deduplication, and the multikey both-bounds re-check with them.

Measured on 200 documents, ten of which match:

| | Documents examined |
|---|---|
| Before | 200 |
| After, index applies | 10 |
| After, no index | 200 (unchanged, and it must be) |

**`explain` came with it**, on both routes, mirroring `find`. That is what
turns "an index applies here now" from a sentence into something a test
asserts and an operator can check — the standing lesson from ADR-016, where a
claim nothing verified stayed false for two milestones.

**A second, quieter bug went with it.** `update` counted `modified` by
incrementing per target rather than reading the write's own answer, so a
document deleted between the read pass and the write was still reported as
modified. It now counts `WriteOutcome::modified`. `delete` already did the
equivalent, which is why only one of the two was wrong.

**What deliberately did not change:** both still apply document by document and
can stop partway. That is the atomicity limitation recorded below, and it is a
separate question from which access path finds the targets.

---

## 🟢 `findAndModify` exists (M9 task 3)

**Three decisions settled before code**, and the second is the substance.

**One `/find_and_modify` route** carrying Mongo's flags, rather than three
`find_one_and_*` routes or an option on `/update`: every existing route is named
for a server operation, and `findAndModify` is the server command all three
driver methods map onto — which matters for the first-party clients M10 builds.

**The match happens inside the write transaction.** `update` collects targets in
a read pass and writes them one at a time, so two callers claiming the same job
both succeed. redb has a single writer, so a match found *inside* the write
cannot be taken between the match and the commit: atomic by construction, with
no retry loop and no way to report "nothing matched" while something did. The
cost is that the writer is held for the match; an unindexed filter over a large
collection blocks every other write for the length of the scan.

**Sort is supported**, so FIFO job queues work — which is most of why the
operation exists. It materialises every match inside the transaction to do it.

**`MAX_CANDIDATES` bounds the damage those two combine into.** Ten thousand,
matching `find`'s `MAX_LIMIT` and the ~8 ms scan it was chosen from — as a
**refusal**, not a truncation, because choosing from a prefix would return a
document the sort did not pick and no caller could tell.

**The crate boundary held.** `kimmy-query` is a dev-only dependency of
`kimmy-storage` on purpose, so filtering, ordering and update operators could
not move into the engine. They arrive as `ModifySpec` — pure functions over
documents that the engine calls inside its transaction — which is the same shape
as the guard `delete_guarded` took for TTL, one step further.

---

## 🟢 Documents can expire (M9 task 2)

**Three decisions were settled before any code**, and each is recorded where it
is enforced rather than only here.

**The trigger is a TTL index**, not a collection setting or a magic document
field, because the pass has to *find* expired documents every tick: a collection
scan is ~8 s per pass at ten million documents whether or not anything expired,
against ~1.66 µs per candidate through an index range scan. The index is both
the policy and the mechanism.

**One node expires a given collection**, by rendezvous hashing over live
members — the webhook ownership machinery, reused. Every node expiring
independently is convergent but produces N deletes per document, of which N-1
are superseded entries that still cost oplog, replication and change-stream
bandwidth. The cost is that expiry stalls for a collection whose owner is
partitioned; TTL is best-effort by nature and that was judged the better trade.

**An expiry is an ordinary `OpKind::Delete`.** A dedicated op kind would be a
**stop-the-cluster upgrade**: `op_kind_from_tag` (`codec.rs`) rejects an unknown
tag as *corruption*, so an old node would treat a new node's expiry as a corrupt
oplog rather than tolerating it — the same class as ADR-040 and ADR-051. The
audit value did not justify it, and MongoDB does not distinguish either.

**Two things came out of building it.**

**The obvious middle path on the third decision was a data-loss bug.** Marking
an expiry by putting a payload in the delete's body looks harmless, and
`apply_remote` branches on exactly that: a delete carrying a body is decoded as
a **live document**. Every "marked" expiry would have resurrected itself on
every node. Found by reading the apply path before offering the option, not
after taking it.

**The scan and the delete cannot share a transaction**, so a document refreshed
in between — a session heartbeat, which is the main reason to have a TTL at
all — would be deleted while live. `delete_guarded` re-reads inside the write
transaction and declines. The ordinary delete shares that body rather than
having a second one beside it, for the same reason `insert` and `insert_many`
share `insert_in_txn`.

**`kimmy_ttl_expired_total` was added** so "one document, one delete" is
measured rather than asserted: the cluster harness sums it across three nodes
and requires exactly 1. Correctness alone cannot distinguish the two designs,
because N deletes converge.

---

## 🟢 The pipeline can derive (was the largest gap against MongoDB)

**M9 task 1.** Expressions were a field path or a literal, so a pipeline could
filter, group and join but could not *compute*. `kimmy-query/src/expr.rs` is now
a recursive tree over the agreed operator list — arithmetic, strings,
conditionals, comparison, boolean and date parts — plugged into `$project`,
`$group` keys, accumulator arguments and the new `$addFields`/`$set` and
`$replaceRoot` stages.

**Three things came out of building it that were not in the plan.**

**A latent `$sum` bug, found by reading the code the decision touched.**
`finish()` carried an `all_int` flag and a comment citing ADR-002 about not
losing precision above 2^53 — while accumulating in `f64` and casting back, which
loses precision above 2^53. A sum of `2^53 + 1` and `1` returned
`9007199254740992`. Fixed by accumulating in `i64` and widening only on a double
operand or an overflow; both the unit test and a pipeline-level test pin it.
**The comment was right and the code was wrong, and nothing failed when they
disagreed** — the same shape as the "no C toolchain" claim (ADR-016).

**A deliberate behaviour change.** A document-valued expression now *computes*
its values, so `{$group: {_id: {c: "$city"}}}` groups by city. Previously that
was a constant document and **every input landed in one bucket** — a wrong answer
rather than a refusal. `$literal` is the escape hatch and exists because the rule
requires one: without it there is no way to produce the string `"$city"`.

**`$literal` was not on the agreed list** and was added anyway, because the list
is incomplete without it. Noted rather than slipped in.

**Deliberately excluded**, and recorded in the table below: arrays and sets,
variable binding (`$$ROOT`, `$map`, `$filter`, `$reduce`, `$let`), and type
conversion. `$$`-prefixed strings are **refused** rather than parsed as a field
named `$ROOT`, which would silently evaluate to null.

---

## 🟢 The client story — settled on 2026-08-12 by ADR-055

**Raised and postponed on 2026-08-11, decided on 2026-08-12.** The protocol is
the HTTP/JSON and WebSocket API KimmyDB already serves, promoted to a
specified and versioned contract with first-party clients for Rust, Python and
Go. The MongoDB wire protocol, gRPC and GraphQL are all rejected as
client-facing protocols. M10 is the milestone that carries it out.

**The framing in the original entry was wrong, and worth preserving as the
lesson.** It read "there is no wire protocol", which conceded a premise that
was never true — HTTP framing, Extended JSON v2 encoding, bearer-token
authentication, a typed error envelope and WebSocket streaming are a complete
protocol, and `kimmy-cli` is 464 lines of working client against it. What was
actually missing was that **nothing specified, versioned or tested it**. That
is a documentation gap wearing an architecture gap's clothes, and describing it
correctly is what made the decision small. Full reasoning, including why each
alternative was rejected, is in [ADR-055](decisions.md).

**Left open on purpose:** a narrow, read-mostly wire shim presenting as a
*standalone* server, purely so Compass and `mongosh` can inspect a database.
Not rejected — a smaller, separate decision, to be made after M10.

---

## 🟢 The protocol is specified, and the specification is checked (M10 task 1)

**Was.** The protocol existed and nothing wrote it down. `docs/http-api.md` said
so itself: "a *reference*, not yet a *specification* — nothing here is
versioned or machine-checked against the routes, so it can drift."

**Now.** `docs/openapi.yaml`, hand-written, with `crates/kimmy-api/tests/openapi.rs`
as the gate. The approach was the task's reserved decision and the maintainer
chose hand-written over generated; the reasoning, including why generation is
weaker *here* specifically, is [ADR-056](decisions.md).

**The test checks two different things**, because a specification can be wrong
two ways. Inventory — every registered route is described and every described
operation is registered — and behaviour: every documented operation is driven
against a real server and its response validated against the declared schema.
It ends by asserting every documented operation was exercised, so an entry
nothing executes cannot be added.

**Three findings, all from the behavioural half:**

- **`PUT /docs/{id}` returned booleans where every sibling route returns
  counts.** `{"matched": true, "modified": true}` against `/update`'s
  `{"matched": 3, "modified": 2}` — one field name carrying two types on one
  protocol, because the route serialized `WriteOutcome`'s three bools straight
  to the wire. Nothing had stated the type, so nothing disagreed, and the route
  had **no integration test at all**. `docs/handoff.md` described it as
  `{"matched": 0}`, which was wrong in a way nothing could contradict.
  Normalized to counts by decision — the cheapest possible moment, since no
  client exists yet and nothing in-tree reads the fields. `upserted` stays a
  boolean because it genuinely is one, and the route now has a test naming both
  behaviours.
- **`GET /v1/users` returns names, not user objects.** The specification's
  first draft said objects, written from the handler's shape rather than the
  store's return type. Caught on the first run.
- **The M8 inventory test had a hole.** `every_route_is_in_the_http_reference`
  matched `.route("` at the start of a line, which skips the three
  registrations rustfmt breaks across lines — `/docs/{id}`, `/docs/{id}/vectors`
  and `/vector`. It had been green while never checking the busiest route on
  the API. There is now one scanner, in the new test, and it checks the prose
  reference as well.

**The pattern is the milestone's own thesis, one task in:** every claim that
mattered and had no mechanism behind it was false. The two documents describing
this protocol both described it wrongly, and it took driving it to notice.

---

## 🟢 The error codes are a closed set, and say what to do about them (M10 task 2)

**Was.** Codes were `&'static str` literals written at call sites across five
modules. Nothing enumerated them, so both documents that tried to list them
were wrong, and nothing said what a client should *do* about any of them.

**Now.** `ErrorCode` is an enum; the wire string and the retry class both come
from exhaustive matches on it, so a new code does not compile until both are
answered. The specification carries the full set and the class of each, checked
against the enum by the contract test. [ADR-057](decisions.md) has the
reasoning, including why the retry class is three-valued.

**`no` / `wait` / `elsewhere`, not a boolean**, because every node accepts
writes and holds a full copy — so "ask a different node" is a real answer, and
the right one for `internal`, `misconfigured` and `snapshot`, which are
conditions of the node rather than of the request. A boolean would tell a
client that `internal` is retryable and have it retry the machine that just
failed.

**The class travels in the envelope**, so a client that acts on `retry` handles
a code released after it was written. That is what has to be true for "adding a
code" to be additive under task 3's compatibility policy.

**Two omissions found while enumerating**, both of the kind that only a list
compared against the code can find:

- **`no_vectors` was in neither document.** A 409 returned when searching a
  collection whose vectors were never ingested — deliberately a refusal rather
  than an empty result, because an empty result is indistinguishable from
  "nothing matched", which is how a `byo` collection can look like it works
  while returning nothing forever. It is in `vectors.rs`, and both lists had
  been written by reading `error.rs`.
- **422 was documented in the prose reference and specified nowhere.** A body
  that is valid JSON of the wrong shape is a different failure from a body that
  is not JSON, and seventeen operations can return it. All seventeen now
  declare it.

**And four more, every one of them found by driving a real node** rather than by
the suite — which had passed the whole time:

- **Sixteen routes were outside the taxonomy.** Axum's body rejection is bare
  text with no code, and the M5 mapping that fixes it is only reached by a
  handler taking `Result<Json<T>, JsonRejection>`. **One handler of nineteen
  did.** Every other typed body answered `422 text/plain`. The mapping now
  lives in an extractor — `json::JsonBody<T>` — which cannot be used without
  it. The conformance test had exercised a wrong-shaped body only against
  `/bulk`, the one route that was right.
- **`/watch` refused non-upgrade requests with no envelope at all**, for the
  same reason and with the same fix. It was the single refusal on the API a
  client could not branch on.
- **The specification said the OpenAI provider's tag was `openai`.** It is
  **`open_ai`** — `openai` is the display name from `ProviderConfig::name()`,
  which is what the spec was written from. `docs/vectors.md` had it right all
  along, and the project had already been caught by this exact distinction once
  before.
- **`no` is not a string in YAML 1.1.** `enum: [no, wait, elsewhere]` reads as
  `[False, "wait", "elsewhere"]` in PyYAML while Rust's reader gives the
  string, so **two readers of the specification disagreed about a value on the
  wire**. Quoted now, with the reason beside it. This is the whole argument for
  the live drive being a second, independently written reader: nothing inside
  the Rust test could have seen it.

---

## 🟢 `/v1` has a written promise, and a node says what it can do (M10 task 3)

**Was.** The path said `v1` and nothing said what that meant. No document
stated what could change under a client, and **nothing exposed a version over
HTTP at all** — the build version was in the startup log and in the MCP
handshake, so a client could not ask what it was talking to.

**Now.** [Compatibility](compatibility.md) is the policy: `/v1` does not break,
additive changes ship in it, anything breaking mints `/v2` served alongside for
a stated window. `GET /v1/version` reports the protocol, the build, the node
that answered, and a **capability list**. [ADR-058](decisions.md) has the
reasoning, including why date-versioned requests were rejected.

**Capabilities rather than a version number**, because nodes are upgraded one
at a time and a round-robin client can reach an older node right after a newer
one. A number answers "can I use this feature" only if the client also carries
a table mapping versions to features — the table this replaces, and the table
that goes stale in every client independently. `Capability` is an enum, checked
against the specification like the error codes.

**Four claims became mechanism**, which is what separates this from the last
time a compatibility property lived in prose:

- Every versioned route is under `/v1/`, and the prefix agrees with the
  server's reported protocol *and* the specification's `info.version`.
- The advertised capability set is exactly the documented one, and every
  capability has an explanation rather than only a name.
- **No response schema forbids unknown properties.** Without this, "a new
  response field is additive" is false for any client that validates — it would
  break on the next field added, silently, and only for them.
- The default build must *not* advertise `local-embeddings`, which is what
  proves the list is answered per build rather than asserted.

**Two things stay prose, and are marked as such**: the six-month `/v2` window,
which is a promise about calendar time, and "changing what a route means is
breaking", which nothing that reads shapes can detect.

---

## 🟢 A token can be refreshed without re-sending credentials (M10 task 4)

**Was.** `/v1/auth/login` was the only way to get a token, and tokens expire —
so a client library's only options were to hold the password and re-send it
every hour, or to hand every application an hourly outage.

**Now.** `POST /v1/auth/refresh` exchanges a valid token for a fresh one.
**Sliding re-issue, not a second credential**: nothing new to store, no second
lifetime to reason about, nothing kept server-side.
[ADR-059](decisions.md) has the reasoning, including why a stored, rotating
refresh token fits a leaderless store badly — rotation is a compare-and-set on
a replicated record, and two concurrent rotations resolve by last-writer-wins,
discarding a credential a client is holding.

**The security half is a property of the route's shape.** Refresh takes the
`Auth` extractor, so the token goes through the same ADR-052 check every route
applies and a revoked session is refused *before the handler runs*. That is the
difference between "cannot launder a revocation" and "does not, as long as
nobody deletes a line".

**Three things it deliberately does not do**, each written down rather than
discovered later:

- **It does not recall the old token**, which keeps working until it expires. A
  stateless token cannot be recalled; ending a session early is what the
  version bump is for.
- **It does not survive a grant change.** Grants live in the token, so changing
  them bumps the version and refresh is refused along with everything else —
  the cost of grants being carried rather than looked up.
- **It offers no grace for an expired token**, so `exp` means one thing on
  every route. A client idle past the lifetime logs in again, which is a thing
  a library may ask of an application where doing it hourly is not.

**Login and refresh both report `expiresIn`**, so a client schedules renewal
without decoding a token it is told to treat as opaque. An added response field
is additive under the policy written one task earlier, which is the first time
that promise was spent.

---

## 🟢 A client can discover the cluster (M10 task 5)

**Was.** A client was handed one address and could not learn the others. It
could not fail over, and the one capability a MongoDB driver's SDAM would have
given free was missing — while being *easier* here, because every node accepts
writes and there is no primary to find.

**Now.** `GET /v1/topology`. **Addresses** come from a replicated registry each
node writes itself into; **liveness** comes from SWIM. [ADR-060](decisions.md)
has the reasoning.

**Reading addresses from SWIM was not available.** `Member` carries the
*gossip* address and is postcard-encoded, so adding a client address to it is a
stop-the-cluster upgrade — the fourth in three milestones — and would put
client-facing configuration inside the cluster's internal wire. Deriving a
client address from a gossip address is a guess that breaks on separate
interfaces, TLS termination and container networking.

**Both known traps were inherited deliberately rather than met by accident:**

- `Members` holds **peers only**, so the answering node is added explicitly. A
  list derived from membership alone would tell a client the cluster does not
  include the node it is talking to — the shape of the bug that silently
  undelivered every clustered webhook.
- The member set must contain **only authenticated peers** (ADR-053), and that
  invariant now protects more than ownership: an unauthenticated peer in the
  set would be advertised to clients as a node to send credentials to.

**Verified on real nodes, not only in-process.** A cluster-harness test boots
three nodes and requires every one to list all three as `live` with its real
address — including a node whose seed list never named it, which only
replication can explain — then checks a token from one node works at every
advertised address, then kills a node and requires it to be reported `unknown`
rather than vanishing. The in-process tests prove the assembly; only the
harness proves the assembled thing, which is the distinction M8 task 1 was
built on.

**`status` is `live` or `unknown`, never `down`**, because a node whose gossip
is partitioned while its HTTP works is a real state here and hiding it removes
an option exactly when a client wants one.

---

## 🟡 A decommissioned node stays in the topology

**Raised 2026-08-13, with M10 task 5.** A node's registry record outlives the
node: a machine that is removed from the cluster keeps appearing in
`/v1/topology` as `unknown` forever.

**Not solvable by age.** Records are written at startup and only when their
content changes — deliberately, so an idle cluster does not append to the oplog
every tick — so a record's age measures uptime, and collecting old ones would
delete the longest-running healthy nodes first.

**The workaround needs no new surface**, which is why it is a 🟡 rather than a
task: the registry is an ordinary collection, so removing a decommissioned node
is deleting its document. An explicit route belongs with whatever operational
tooling comes after M10, not inside the client protocol.

---

## 🟢 Cursors are a protocol promise, and the promise is tested (M10 task 6)

**Was.** M9 built cursors and documented them well, but as *engine* behaviour.
The wire contract carried three claims nothing checked — node portability,
never-repeat-never-miss, and what a full page means — and the specification
said nothing at all about page size.

**Now.** The specification carries the contract a client may rely on, and the
three claims are checked.

**Node portability is verified on real nodes**, which is the one that mattered.
It was argued from the design — a token is a pure function of the `_id`, so any
node computes the same bound — and inherited from change-stream resume tokens,
which *had* been verified on a cluster. A cluster-harness test now walks a
collection changing node on every page and requires the walk to see every
document exactly once, in order. The protocol tells clients to round-robin
([ADR-060](decisions.md)); paging that broke when they did would be a data bug
reached by following the protocol's own advice.

**Two silent behaviours were specified nowhere**, and both are the kind a
client author discovers in production:

- **A `find` with no `limit` returns 100 documents, not all of them.** The
  prose reference said so; the machine-readable specification a client is
  generated from did not, so a generated client had no way to know. A caller
  treating an unlimited `find` as "the collection" processes a prefix and is
  told nothing. `count` has no cap, and is the honest source for a total.
- **A `limit` over 10,000 is clamped rather than refused.** The request
  succeeds and returns less than was asked for.

**And one that is a real trap:** a final page that is exactly full still
carries a token, because the server cannot know it is the last without looking
further, and looking further is work the caller did not ask for. A client must
end its walk on a short or empty page, not on a token no longer being offered.
Now stated in both documents and tested.

**One property is documented rather than enforced, deliberately.** A token
encodes a position, not a query: sending it with a different filter resumes
that filter after the same key, and the server does not check that the token
came from the query it is used with. Enforcing it would mean putting the query
in the token, which would make it large, node-specific in spirit, and a place
for a client to depend on structure it is told to treat as opaque.

---

## 🟢 Client-facing throughput is measured (M10 task 7)

**Was.** Every figure in [Benchmarks](benchmarks.md) was taken at the storage
engine, so JSON and Extended JSON conversion, per-request token verification,
HTTP framing, TLS and concurrent clients were all outside the published
numbers. There was **no honest answer to "what throughput can a client
expect"** — and that file's retracted-figure note is what happened the last
time one was quoted anyway.

**Now.** `cargo bench -p kimmyd --bench http` spawns the **shipped binary** and
drives it with concurrent clients over a real socket, plaintext and TLS, reads
and writes. Recorded rather than gated, like every benchmark here.

**Four findings**, all of which needed the socket to be visible:

- **TLS is close to free** — within noise at one client, about 10% at
  thirty-two. Whatever the reason to terminate TLS elsewhere, throughput is not
  it.
- **The protocol costs about 0.1 ms per request.** A point read, end to end
  through HTTP, token verification, storage and Extended JSON, has a p50 of
  0.09 ms. That is the number "what does the API cost" was missing.
- **Reads scale with clients (8,001/s → 70,660/s) and writes do not**
  (143/s → 602/s), with p99 rising from 10 ms to 246 ms. redb has one writer;
  concurrency queues rather than parallelizes. The engine benchmarks said this;
  the socket confirms a client experiences it as tail latency.
- **`count` is a collection scan** — 30 requests a second over 10,000
  documents, and it barely scales. A client polling a count asks the server to
  read everything, every time.

---

## 🟡 A write costs twice as much through the daemon as at the engine

**Raised 2026-08-14 by M10 task 7.** A single insert is **7.0 ms** over HTTP
against **~3.4 ms** for the same insert at the engine on the same machine.

**What it is not.** Not protocol overhead — the read numbers bound that at
~0.1 ms per request. Not per-document encoding — a batch of 100 costs 12.2 ms
against 6.9 ms at the engine, so the gap is roughly fixed per *request* rather
than growing with the documents in it.

**Candidates, none verified**: the background oplog consumers the daemon runs
and a bare `Engine` does not — the embedding worker and the webhook dispatcher
both wake on every write — or a commit's fsync landing on a runtime worker
thread rather than a dedicated one.

**Left as a question on purpose.** A cause that has not been measured is not a
cause, which is the rule the retracted figure produced. It is the first thing
for whoever picks up performance work; halving it would double single-write
throughput for every client that does not batch.

---

## 🟢 There is a Rust client, and the CLI is its first consumer (M10 task 8)

**Was.** KimmyDB had a specified protocol and nobody to speak it. `kimmy-cli`
hand-rolled every HTTP call — building URLs, reading statuses, deciding what an
error meant — which meant every one of those decisions existed once, in a tool,
where no other client could benefit from it being right.

**Now.** `kimmy-client`, and the CLI is a consumer of it: nothing in
`kimmy-cli` builds a URL or reads a status code any more.

**It depends on no `kimmy-*` crate, and a test keeps it that way.** That is the
property that makes it useful as a check rather than only as a convenience: it
sees exactly what the Python and Go clients will see, so it cannot quietly rely
on something the specification never promised.

**What it does, and each of them is a server promise from an earlier task:**
holds a token and refreshes it before expiry using `expiresIn` (task 4); fails
over between nodes discovered from `/v1/topology` (task 5); pages with cursors,
ending the walk on an empty page rather than a missing token (task 6); returns
typed errors carrying the retry class (task 2); and resumes change streams from
the last token seen, which is safe only because tokens are portable (M4 and
task 6).

**Retries are conservative on purpose.** A read moves to another node on
`elsewhere`; **a write does not**, because `elsewhere` means *this node* did not
answer, not that the work did not happen — and no status distinguishes an
insert that failed before the commit from one that failed after it. Callers who
know their request is idempotent say so.

**Three things the conversion found**, which is exactly why the roadmap made
the CLI the first consumer:

- **`Client::request` took a `reqwest::Method`**, so every consumer had to
  depend on `reqwest` to name a verb — the HTTP stack in the public API, where
  changing it would be a breaking change for everyone. The crate has its own
  `Method` now.
- **Login did not fail over.** It only tried the first endpoint, so a client
  handed a list whose first address was dead could not authenticate at all —
  the one failure that makes every other endpoint useless. Found by a test that
  put a dead address in front of a live one.
- **The CLI could not create a collection.** On a fresh database the first
  `insert` fails with "collection not found" and the tool offered nowhere to go
  but `curl`. It has `create-collection` now, and `watch` and `topology` while
  there.

---

## 🟢 There is a Python client (M10 task 9)

**Now.** `clients/python`, package `kimmydb`, synchronous, on `httpx` and
`websockets` — both chosen because each has a sync *and* an async API behind
nearly the same surface, so "sync first" costs nothing later. The stdlib was
considered and rejected for one measured reason: `urllib` opens a connection
per request, and against a ~0.1 ms request a handshake per call would dominate
everything the client does.

**It shares no code with the Rust client**, which is the point. Two independent
readers of one specification make a disagreement between them mean something.
The test suites are deliberately the same scenario list.

**The surface is Python, not a translation.** Iteration where a Python caller
expects it, `documents()` for the shape most people actually want, exceptions
rather than returned errors, and `.code` as a plain string rather than an enum
— because codes are additive and an enum would make an unfamiliar one an error
in itself.

**One thing had to fight the idiom.** A change stream connects when it is
asked for, not on the first read. Python's natural shape is a lazy generator,
which would open the socket at the first `next()` — so anything written between
`watch()` and that read would be missed, silently. Found by a test that wrote a
document immediately after opening a stream and then waited for it forever.

**CI runs it** against a real `kimmyd`, like every other client test here: a
mocked server would only assert what the client already believes.

---

## 🟢 A dropped collection now ends the streams watching it (was a 🟡, one day old)

**Was.** A change stream whose collection was dropped received no event, no
close and no error: it waited for changes to something that no longer existed.
And because ids are derived from `(database, name)` ([ADR-031](decisions.md)),
a collection recreated under the same name has the *same id* — so the stream
would silently resume delivering for it, bridging two different collections
with nothing in between. The stall was the visible half; the bridge was the
dangerous one.

**Now.** `InvalidateReason::CollectionDropped`, decided in storage rather than
at the HTTP edge, because storage is where the other two reasons live and where
`finished` is set — an in-process consumer of `Engine::watch` would otherwise
keep a stream it believes is live. **Scoped deliberately**: only a stream
watching *that* collection ends. A `Cluster` or `Database` stream keeps going,
and a test says so, because ending those would take the embedding worker down
with the first dropped collection anywhere.

**Fixing it exposed a second defect that had been invisible.** A replicated
schema change was appended to the receiving node's oplog but **never
published**, while a replicated *document* was — so a drop ended its own
node's watchers immediately and left every other node's waiting for an
unrelated write to wake them. Change streams filter schema entries out, so
"delivered late" and "not delivered" had looked the same for as long as the
asymmetry existed. It stopped looking the same the moment a drop became
something a stream cares about.

**The cluster harness is what found it**, and nothing else could have: a single
node applies its own drop directly, so every in-process test passed. This is
the third time that harness has caught a cluster behaviour that every
transport-free test agreed was fine.

**One representation fixed while there.** The invalidate reason was rendered
onto the wire with `{:?}` — a `Debug` derive, so renaming a variant would have
silently renamed a value clients branch on. It has an `as_str` now, with the
two existing names kept exactly as `Debug` produced them, so nothing already on
the wire changed. That is the same invariant `NodeId` and `CollectionId` each
cost a replication outage to learn.

**Both clients assert the new behaviour**, and the specification documents all
three reasons with what a client should do about each.

**And a third defect, older than both.** Because ids are derived from
`(database, name)`, a collection recreated under the same name reuses its id —
so the oplog still held the dead incarnation's entries and streams still
matched them. `from_start` on a healthy recreated collection replayed a dead
collection's documents and then invalidated immediately, never showing the live
data. A stream now never reads across a drop, and a resume token from before
one is **refused** rather than moved forward silently: the gap between that
token and this collection's first event is exactly what a client must not be
left unaware of.

**All three were found by asking a running node rather than reading the code.**
The first came from a client test that hung; the second from the cluster
harness; the third from a probe written to check a claim in a pull request
description, which turned out to describe behaviour the server did not have.

---

## 🟢 There is a Go client (M10 task 10)

**Now.** `clients/go`, package `kimmydb`, import path
`github.com/titusai-io/kimmydb/clients/go/kimmydb`. **One dependency**, and it
is the WebSocket framing: Go's `net/http` pools connections, so the argument
that ruled out Python's standard library does not apply.

**`coder/websocket` rather than `gorilla/websocket`**, for a reason specific to
this design rather than a preference: it handshakes through an ordinary
`*http.Client`, so a change stream inherits the same client, TLS configuration,
proxy and timeouts as every other request. `gorilla` dials with its own
`Dialer`, which is two configurations that can drift — the split this project
has been bitten by before.

**Idioms, not a translation.** Paging and streaming are range-over-function
iterators, so the error is the second loop variable rather than something to
remember to check; everything takes a `context.Context`, including the change
stream, and cancelling it is how a caller stops watching. `Code` is a plain
string rather than a constant set, for the same reason it is in Python — codes
are additive, and an unfamiliar one must not be an error in itself.

**Three clients now pass the same scenario list**, written three times against
one specification, sharing no code. That is the arrangement task 11 was waiting
for: the conformance suite becomes a matter of running one set of scenarios
three ways rather than inventing them.

**It found nothing new**, which the roadmap predicted: Go was placed third
because it is the least likely to surface a protocol gap the other two missed.
That prediction holding is itself worth recording — the specification and the
two earlier clients had already taken the surprises.

---

## 🟢 Three clients are held to one set of scenarios (M10 task 11)

**Was.** Three clients passed matching scenarios because they were *written* to
match. Nothing enforced it. The drift would have been found by a user.

**Now.** `clients/conformance`: one declared scenario list with the
observations a correct client must produce, a small driver per client, and a
runner that starts a fresh node per scenario and compares. Sixteen scenarios,
three clients, forty-eight runs.

**Two checks, not one.** Coverage — every driver must implement every declared
scenario — and behaviour, which is the half a per-language suite cannot do:
three suites can each have a `failover` test and disagree about what failover
means.

**A driver reports; it never judges.** Three clients that each decided whether
they had passed would be three opinions rather than one oracle and three
answers.

**Verified by breaking a client on purpose.** The Python driver made to stop
one page early produced `documents_seen: expected 250, observed 200` while the
other two passed. A suite that has never gone red is a suite nobody has tested.

---

## 🟢 Collection creation is not idempotent, and the specification said it was

**Found by the conformance suite's first full run**, indirectly: the runner
reused its work directory, so a second run started a node on the first run's
data and every client failed to create a collection that already existed. The
runner bug was real and is fixed — but the error message was the finding.

**`POST /v1/db/{db}/collections` returns `409 conflict` when the collection
exists.** It always has: `create_collection_inner` returns `CollectionExists`.
`docs/openapi.yaml` had said "Created, or already present — creation is
idempotent" since M10 task 1, and both the Python and Go clients repeated the
claim in a comment.

**Nothing caught it because nothing ever created a collection twice.** Every
test in the repository — the contract test, three client suites, every drive
script — creates each collection exactly once. The specification's coverage
assertion checks that every *operation* is exercised, not every documented
*outcome*, so a false sentence about a second call sat unchallenged.

**Now**: the specification says what happens, both clients say it correctly,
the contract test creates a collection twice, and it is a conformance scenario.
Four places, because the claim was in four places.

---

## 🟢 One application, written three times, and run (M10 task 12)

**Now.** `examples/shelf` — a small library catalogue in Rust, Python and Go.
It uses what KimmyDB exists for rather than what every database has: bulk
insert in one commit, cursor paging, an aggregation, semantic search over
client-supplied vectors, and a change stream watching the collection while it
is written to.

**They run in CI**, against a real node, all three in sequence — so the second
and third exercise the "already stocked" path and re-runnability is checked
rather than assumed. An example nobody runs rots into a document that used to
be true.

**The embedding is a toy and says so**: a deterministic bag-of-words hash, the
same algorithm in all three languages, so the pipeline is real without an API
key or a model download. All three produce identical scores, which is a
pleasant way to notice that nothing language-specific leaks into the protocol.

**Why an application rather than three snippets.** The deferred decision that
became M10 named *running KimmyDB on something real* as the trigger for judging
whether the client direction was right. A snippet that inserts one document
does not test that; something that has to find, page and react does.

---

## 🟢 The M10 server-side mutation pass is done (was a 🟡)

**Narrowed on 2026-08-14.** M10 task 12 called for a mutation pass over the
milestone diff and covered `kimmy-client` only, leaving 90 mutants in
`kimmy-api`, `kimmy-storage` and `kimmy-auth` unclassified. The original run was
misconfigured — every mutant re-ran all three crates' suites at a parallelism
that pushed each past the timeout, producing 3 caught and 15 timeouts out of 47
before being abandoned.

The scope splits **76 / 12 / 2**, which sums to exactly the 90 recorded.

| Crate | | |
|---|---|---|
| `kimmy-auth` | ✅ 2 caught | Both first read as misses and **both were scoping artefacts** — see below |
| `kimmy-storage` | ✅ 10 caught, 2 unviable | One real gap: `InvalidateReason::as_str` |
| `kimmy-api` | ✅ 76 tested | 36 caught, 32 unviable, **8 missed** on the first clean run; 6 were real and are fixed, 2 are artefacts |

**`kimmy-api`'s eight misses sorted into three different categories**, which is
the result worth keeping — "missed" is not one thing.

**Category 1, a genuine gap: `capabilities()` could return `vec![]`.** The
contract test compared the wire against *the same function that produced it*,
then asserted `!served.contains("local-embeddings")` — and both hold for an
empty list. A node advertising **no capabilities at all** passed every check.
Since [ADR-058](decisions.md) makes capabilities the thing clients branch on
*instead of* a version number, a node that silently claims to support nothing
is the exact failure the mechanism exists to prevent. The unconditionally
present capabilities are now named and required, and the fixture is asserted
non-empty so the test cannot go vacuous again.

**Category 2, covered only by an `#[ignore]`d test: the four `topology.rs`
mutants.** The cluster harness does catch them — three real nodes, `count == 3`,
exactly one `self` — but harness tests are `#[ignore]`, so they are invisible
to `cargo test --workspace` *and* to every mutation pass. **A mutation run sees
only the tests that run by default**, which means anything whose only coverage
is the harness reads as uncovered forever.

**And a real gap was hiding inside that category.** `register`'s docstring
claims it is *"idempotent, and silent when nothing changed — a node that
restarts twice an hour should not append to a log every other node then
replicates."* **Nothing tested it, and the harness structurally cannot**: it
starts each node once and never restarts one on an unchanged address. The
registry is replicated, so a spurious rewrite is an oplog entry every peer then
carries. Two in-process tests now cover it — `register` is entirely local, and
only the *replication* half ever needed real nodes.

**Category 3, a cross-crate artefact: the two `render` mutants.** `render` is
never called in `kimmy-api`'s own suite, because the contract test checks the
`101` handshake and never reads a frame. It is driven by the Rust, Python and
Go client suites and by the conformance runner. Confirmed by re-running with
`-p kimmy-client` in scope, where both die. Classified, not chased.

**The verification was run twice on purpose.** The first widened-scope run was
confounded — the test files were edited while it was in flight, so it could not
separate "the wider scope caught it" from "the new tests caught it". The clean
re-run at the natural `-p kimmy-api` scope reports **25 caught, 5 unviable, 2
missed**, which is what establishes that the six are closed where it matters
rather than only under a scope nobody uses.

**The `kimmy-storage` finding is the one that mattered.**
`InvalidateReason::as_str` could return `""` or `"xyzzy"` with nothing failing.
That method exists *precisely* so a variant rename cannot silently rename a
value clients branch on — and the strings were pinned only downstream, in the
three client suites, the cluster harness and the conformance scenarios, and
only for `CollectionDropped`. `ConsumerLagged` and `ResumeTokenExpired` were
held by prose in `docs/openapi.yaml` and by nothing that fails. They are pinned
now in the crate that chooses them.

**And a methodological finding worth more than either.** Both `kimmy-auth`
misses were caught once `-p kimmy-api` joined the test scope: `ttl_secs` is
asserted in the consumer's suite, not the owner's. **Per-crate scoping — M10's
own documented lesson, and the thing that makes these runs twenty minutes
instead of nine hours — systematically hides cross-crate killers.** A miss in a
crate whose surface is consumed by another may only mean the test lives one
crate up. Verify with a widened scope before believing a gap is real. A local
test was still added, because a crate's public accessor should not depend on a
consumer to pin it.

**Contention had to be learned twice.** Running the `kimmy-api` pass beside an
ordinary `cargo test --workspace` stretched that suite from ~2 minutes past 10.
It does not only ruin the mutation run — it ruins whatever shares the machine.
On an idle box the same 76 mutants finished in 31 minutes with **zero
timeouts**, against the original run's 15 timeouts out of 47. Run these alone.

---

## 🟢 The client's `retry: wait` path is tested — and was broken (was a 🟡)

**Closed on 2026-08-14, and the untested branch turned out to be wrong.** The
🟡 recorded a missing test. Writing it found that `Client::send` did not do
what the comment directly above it said, which is the failure mode this project
keeps meeting: a claim with nothing that fails when it stops being true.

The comment read *"The same node, after the delay it named."* The code slept
and then `break`, and that `break` left the inner `loop` — advancing the outer
`for endpoint`. So `wait` moved to the **next node**, exactly like `elsewhere`
and only slower; with a single endpoint it slept and then returned the error,
so the wait bought a delayed failure and nothing else. That is precisely the
outcome the three-valued taxonomy (ADR-057) exists to prevent: `wait` means
*this* node will serve the request shortly, so failing over abandons the one
node that said how long to wait.

`wait` now retries the same endpoint, **bounded to one wait per endpoint**, and
a node that refuses twice is left for the next one.

**Four tests, and the second endpoint is what makes three of them tests at
all.** With one node, "gave up" and "failed over and found nowhere to go"
produce the same error, so the mutation pass could rewrite the guards either
way unnoticed. Adding a second stub separated them — including the sharpest
case, an **unsafe write** that must not be repeated to a peer that might commit
it a second time. All nine mutants in the changed region are now caught, from
zero.

The harness is `Stalling` in `crates/kimmy-client/tests/client.rs`: a stub that
answers `429` a fixed number of times and counts its hits. It is not
`kimmy_api::router` because a real node's rate limiter bounds *login*, so no
arrangement of one makes an ordinary request answer `rate_limited` on demand —
which is also why this path has no live drive, and the conformance suite's 48
runs stand as the regression instead.

The rest of the residue is classified in [Testing](testing.md): reconnect
backoff arithmetic that needs fault injection to reach, and a handful of
provably equivalent mutants.

---

## 🟡 Sharding is deferred until there is experience

**Raised and explicitly postponed on 2026-08-11**, after the surface was
compared against MongoDB's. Not dropped: a decision the maintainer wants to
make from operational experience rather than from a comparison table. **A
future session should not re-open it unasked.**

Every node holds a full copy, so capacity is bounded by one machine and write
throughput by one redb writer — ~300 documents/sec single-write, ~51,300/sec
batched ([Benchmarks](benchmarks.md)).
**Replicated-not-partitioned is the current position and is considered
correct for now.** Partitioning would be a milestone-sized architectural change
and arguably against the leaderless simplicity that makes the rest of the
design work.

**To close.** Run KimmyDB on something real for a while, then decide. Recorded
here so that the absence of a decision is visible as a decision.

---

## 🟡 Not yet implemented, and known

| Gap | Consequence | Milestone |
|---|---|---|
| Client certificates (mTLS) | Server TLS authenticates the *server* to clients; clients still authenticate with a bearer token only | not planned |
| No certificate expiry metric | A failed reload is counted, but a certificate nobody ever tried to rotate reports nothing. `kimmy_tls_cert_expiry_seconds` needs `x509-parser` as a new runtime dependency (pure Rust — not a second crypto stack, so `check-native-deps.sh` would still pass) | not scheduled |
| Rate limiting beyond login | Only `/v1/auth/login` is limited. Every other route is unbounded — see the entry below | M5 |
| Per-session revocation | Revocation is per user — all of that user's tokens or none. Killing one session while leaving another needs a per-token deny-list, which fails open when an entry has not reached the node handling the request | not planned |
| `$vectorSearch` as a pipeline stage | The pipeline is built, but vector search stays its own endpoint | M5 |
| Array/set expression operators, variable binding (`$$ROOT`, `$map`, `$filter`, `$reduce`, `$let`) and type conversion | Deliberately outside M9 task 1's agreed operator list. Variable binding needs an evaluation *scope*, not another operator | not scheduled |
| Multi-document atomicity | Uneven, on purpose. **Bulk insert is atomic** — one transaction, all or nothing ([ADR-048](decisions.md)). `update` and `delete` still apply document by document and can stop partway, because each match is committed on its own. Note this is about *commits*, not about how the matches are found — that is planned now | by design |
| Benchmarks | The vector index, the write path, batched writes, concurrent writers and the planner are measured ([Benchmarks](benchmarks.md)), against a recorded baseline that is advisory rather than gating | M8 |
| No published protocol specification | The HTTP/WebSocket API is the client contract ([ADR-055](decisions.md)) but nothing specifies or versions it, so every client is hand-written and nothing fails when a route drifts | **M10 task 1** |
| No token refresh | `/v1/auth/login` is the only way to obtain a token and it expires. A long-running client has no option but to re-send credentials, which no driver should ask of an application | **M10 task 4** |
| Clients cannot discover the cluster | SRV discovery serves *nodes finding each other*. A client is given one address and has no way to learn the others or fail over — the one thing a MongoDB driver's SDAM would have provided free | **M10 task 5** |
| Client-facing throughput is unmeasured | Every figure in [Benchmarks](benchmarks.md) is taken at the storage engine in a release build. Nothing measures the socket: JSON and Extended JSON conversion, token verification per request, TLS, and concurrent HTTP clients are all outside the numbers. The single end-to-end figure (500 documents, 0.16 s bulk against 11.6 s individually) came from a **debug** node and is a ratio, not a rate | **M10 task 7** |

---

## 🟡 `find` by `_id` does not use the primary key

Found while measuring latency-histogram buckets (ADR-046): `{_id: 500}`
through `find` is a full collection scan — ~7 ms over 10k documents — because
the planner consults *secondary* indexes only. The primary key is not in its
candidate set. `GET /v1/db/{db}/coll/{coll}/docs/{id}` is the O(1) path and
runs p50 ≈ 250 µs on the same data.

**Consequence.** Correct but slow, the register's favourite shape. Any client
that filters on `_id` through `find` — including via the MCP `find` tool,
where an agent has no reason to know a second route is the fast one — pays a
scan.

**To close.** Teach the planner an `_id` equality (and `$in`) fast path that
resolves through the primary key rather than an index. Self-contained; a
natural M8 stretch or M9 item.

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
exist to remove. Agreed with the maintainer: build the mechanism
route-agnostic, apply it
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

**Reindexing is `POST /vector` again — and the old escape hatch never worked.**
The register carried "changing model or dimension needs a
disable-with-`drop_vectors` and re-enable, which backfills from the oplog."
The second half of that sentence was **false, and had always been false**: the
embedding worker's backfill was its first-ever run starting from the
beginning of the oplog, and a worker whose position has ever advanced is past
old entries forever. Enabling embedding on a collection that already held
documents embedded *nothing* — on any node whose worker had previously run —
and re-enabling after a drop embedded nothing either. A claim in this
register, recorded once and never re-checked against the code: the
native-toolchain lesson, again.

Now the worker treats every `ConfigureVectors` oplog entry as a reindex
trigger: it scans the **collection** — the documents are the durable source;
the oplog may have collected their entries — and re-embeds what the
configuration demands. Idempotency is layered: per document, the HLC
staleness check; per scan, a fingerprint of the configuration written only
after the scan completes, because a configuration change is invisible to
per-document HLCs (configurations do not touch documents). A crash mid-scan
replays the entry; a replay after completion costs a scan and no embedding.
Dimension changes are now legal in place for server-embedded collections —
both search paths already skip width-mismatched records, so old vectors are
invisible while the backfill replaces them — and still refused for `byo`,
whose vectors the server can never regenerate.

**Also found and fixed on the way:** the worker's provider cache had no
eviction, so a collection reconfigured to a new model would have kept
embedding through the *old* provider for the life of the process. The cache
now stores the configuration each provider was built from and rebuilds on
mismatch.

**No webhook was ever delivered in any clustered deployment — found by the
cluster harness on its first run.** The live set SWIM maintains contains
*peers only*: it is populated by foca's `MemberUp` notifications, which never
fire for the node holding it. Rendezvous ownership computed an owner over
that set — so `owner == me` was false on every node, for every subscription,
and every node stood down. Three real nodes, gossip formed, webhook
registered, document inserted: zero deliveries. Single-node worked (an empty
set means "own everything"), which is why the whole M6 suite, every mutation
run, and every live single-node drive passed while ADR-045's central claim —
"when the owner dies, another takes over" — described machinery that had
never once delivered in a cluster.

Fixed in `ownership::owns`: the candidate set is the live peers **plus this
node**, which also dissolves the empty-set special case. The trade, stated:
a node SWIM has declared dead still considers itself a candidate, so a
flapping node can deliver alongside its replacement — a duplicate, which
at-least-once already promises, where the alternative was silence.
`peer_only_views_still_elect_exactly_one_owner` now models what production
actually looks like; the original unit tests all hand-built member sets that
included the owner, which is precisely how the assumption survived.

Two lessons, both already in the register in other clothes: a unit test that
hand-builds its input can encode an assumption the real feed violates — the
fixture lesson again — and a feature verified only in the topology where its
hard path cannot occur has not been verified. The harness exists so the
second one stops recurring.

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
The maintainer chose this over restamping on arrival or documenting the
limitation.
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
