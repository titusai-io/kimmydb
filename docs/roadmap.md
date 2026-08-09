# Roadmap

[← Documentation index](README.md)

Milestone status, and the planned design for what remains. This document exists
so that development can be picked up later without re-deriving decisions.

---

## Status

```mermaid
graph LR
    M0["<b>M0</b> ✅<br/>skeleton · config<br/>container · CI"]
    M1["<b>M1</b> ✅<br/>storage · query<br/>streams · auth · API"]
    IDX["<b>indexes</b> ✅<br/>the last<br/>M1 item"]
    M2["<b>M2</b> ✅<br/>vectors and<br/>auto-embeddings"]
    M3["<b>M3</b> ✅<br/>built-in<br/>MCP server"]
    M4["<b>M4</b> ✅<br/>clustering and<br/>replication"]
    M5["<b>M5</b> ✅<br/>hardening"]
    M6["<b>M6</b> 📋<br/>webhooks"]

    M0 --> M1 --> IDX --> M2 --> M3 --> M4 --> M5 --> M6

    style M0 fill:#2f5d3a,color:#fff
    style M1 fill:#2f5d3a,color:#fff
    style IDX fill:#2f5d3a,color:#fff
    style M2 fill:#2f5d3a,color:#fff
    style M3 fill:#2f5d3a,color:#fff
    style M4 fill:#2f5d3a,color:#fff
    style M5 fill:#2f5d3a,color:#fff
```

| Milestone | Scope | Status |
|---|---|---|
| **M0** | Workspace, core types, HLC, config, Docker, CI | ✅ Complete |
| **M1** | Storage, CRUD, query, indexes, oplog, change streams, auth, HTTP API | ✅ Complete |
| **M2** | Auto-embeddings, HNSW, vector and hybrid search | ✅ Complete |
| **M3** | Built-in MCP server | ✅ Complete |
| **M4** | Discovery, replication transport, anti-entropy, snapshot resync, peer health, SWIM membership | ✅ Complete |
| **M5** | Rate limiting, TLS both fronts, benchmarks, aggregation, backup and point-in-time restore, audit log, metrics, CLI | ✅ Complete |
| **M6** | Webhooks — registration and push delivery of change events | 📋 Planned |

Ordering note: vectors and MCP come **before** clustering, deliberately. The
AI-facing features are the differentiator and are useful on a single node;
clustering is the largest and riskiest piece.

---

## M1 complete

Secondary indexes landed last: compound and descending keys, multikey arrays,
unique constraints, a blocking backfill, a rule-based planner, and `explain`.
See [Indexes](indexes.md).

Two deliberate gaps carried forward:

- **`$in` does not use an index.** It needs a union of point lookups rather than
  a single range. Common enough to be worth doing.
- **A range on a descending field falls back to the equality prefix.** Correct
  but less selective; doing it properly needs its own property test, since the
  failure mode is a range that is too narrow.

Neither affects correctness — an unused index only costs time.

---

## M2 — Vectors and auto-embeddings ✅

**Built.** Full detail in [Vectors](vectors.md); this section records what the
plan said and where the build departed from it.

Enabling embedding on a collection creates a shadow collection
`{coll}.__vectors`, maintained by a worker that is an ordinary change-stream
subscriber:

```mermaid
graph LR
    W["Document write"] --> O[("Oplog")]
    O --> S["Embedding worker<br/><i>an ordinary change-stream subscriber</i>"]
    S --> X["extract configured fields"]
    X --> C["chunk"]
    C --> E["embed"]
    E --> V[("{coll}.__vectors")]
    V --> H["HNSW index"]

    style S fill:#2d3748,color:#fff
```

The worker being a plain change-stream subscriber was the payoff from the oplog
design, and it held: "keep vectors in sync" reduced to "consume the log", which
already worked, including resume-after-restart and backfill of a collection that
predates the configuration.

**Off the write path,** as planned. The API returns as soon as the oplog entry
is durable. Each vector record carries its source document's HLC, so staleness
is a comparison rather than a state machine, and re-embedding is idempotent.

| Piece | Planned | Built |
|---|---|---|
| Providers | `fastembed` local ONNX as the zero-config default | ✅ trait + OpenAI / Ollama / custom HTTP. **`byo` is the default; local is feature-gated** — its native ONNX + OpenSSL dependencies would undo the pure-Rust property ([Deviations](deviations.md)) |
| Index | HNSW via `hnswlib-rs` | ✅ HNSW via **`hnsw_rs`** — `hnswlib-rs` requires nightly Rust (`#![feature(f16)]` in a dependency) |
| Index selection | — | ✅ `IndexCache` chooses approximate above **500** vectors (originally 2000, lowered once measured — [Benchmarks](benchmarks.md)), exact below, with a 30 s rebuild interval |
| Persistence | Snapshot the graph, replay newer entries on startup | ⛔ **Not built.** In-memory only; a restart rebuilds lazily. Correctness does not depend on it |
| Deletes | Tombstone in the graph, rebuild past a ratio threshold | ✅ Handled differently: the graph supplies candidates only, and a candidate whose record is gone is skipped. No tombstoning needed |
| Search | `vector_search` + `hybrid_search` with RRF | ✅ Both, with filter composition against the query language |
| Replication | Vectors ride the oplog as data | 📋 M4 — the records are stored like any document, so this needs no vector-specific work |
| `$vectorSearch` stage | A pipeline stage | ⛔ Not built; search is its own endpoint |

**Resolved open questions.** Filtered k-NN post-filters, widening the graph
search 8× to compensate — a pre-filter would need the graph to know about
document state it deliberately does not track. The slim-image question dissolved
once local embeddings became opt-in: the default image carries no model at all.

**The one thing that surprised.** `anndists::DistDot` asserts `1 - dot >= 0`,
which only holds for unit vectors — a real embedding would have aborted the
process. Dot-product collections take the exact path. Found by testing the
metric rather than trusting it.

---

## M3 — Built-in MCP server ✅

**Built.** Full detail in [MCP](mcp.md); this section records what the plan said
and where the build departed from it.

`rmcp` with `transport-streamable-http-server`, mounted at `/mcp` on the same
axum router, sharing storage handles directly — no separate process, no loopback
hop. That held exactly as planned.

**Every tool call runs as the authenticated principal**, through the same
`Principal::can()` the REST routes use. Write tools exist but simply fail
authorization for a read-only token: capability is controlled by the role, not
by build flags or a second permission system.

| Piece | Planned | Built |
|---|---|---|
| Transport | `rmcp` streamable HTTP at `/mcp` | ✅ **Stateless** — no MCP session, so an expired token stops working immediately rather than riding a session opened while it was valid |
| Read tools | `list_databases`, `list_collections`, `describe_collection`, `find`, `count`, `aggregate` | ✅ all six — `aggregate` landed with the pipeline in M5 |
| Search tools | `vector_search`, `hybrid_search` | ✅ Both, with filter composition |
| Write tools | `insert`, `update`, `delete`, `create_collection`, `create_index` | ✅ All five |
| Resources | Collections with inferred schema and samples | ✅ `kimmy://{db}/{coll}`, grant-filtered, **excluding `__kimmy` and shadow collections** ([ADR-027](decisions.md)) |
| Enforcement | One `Principal::can()`, shared with REST | ✅ Stronger than planned — both edges share `kimmy_api::exec`, so the check is *inside* each operation rather than repeated beside it ([ADR-024](decisions.md)) |

**The dependency arrow was inverted.** The crate graph had `kimmy-api` depending
on `kimmy-mcp`, from a placeholder written at M0. It is now the other way. The
milestone's stated constraint — "there must not be a second, weaker enforcement
point" — is not achieved by co-location alone, only made possible by it; sharing
the executor is what makes the check unskippable. The REST handlers collapsed
into thin adapters in the same change, which is the evidence the extraction was
real rather than shaped around MCP.

**Two things were found by driving a real server, not by tests.**
`describe_collection` reported a `presence` of 2.0 — above 1.0, and meaningless
— because array elements were counted per element rather than per document. And
the first `resources/list` offered `kimmy://__kimmy/__users`, the password-hash
collection, as attachable context. Neither had a test, because neither had
occurred to anyone.

**One deliberate deviation from the SDK's default.** `rmcp` enables a `Host`
allow-list as DNS-rebinding protection; it is off by default here, because the
attack needs an unauthenticated server and `/mcp` verifies a bearer token before
the transport runs. Keeping the default would have refused every client
connecting by a real hostname. [ADR-026](decisions.md).

---

## M4 — Clustering and replication ✅

The largest remaining piece. Membership via [`foca`](https://github.com/caio/foca)
(SWIM + suspicion) over UDP, with TCP for oversized payloads.

```mermaid
sequenceDiagram
    participant A as Node A
    participant B as Node B

    Note over A,B: periodic DNS re-resolution finds peers
    A->>B: SWIM probe + version vector {node → max_hlc}
    Note over B: compares against its own vector
    B->>A: TCP: request oplog range
    A-->>B: entries
    loop each entry
        B->>B: apply_remote() — LWW via merge()
    end
    Note over A,B: converged
```

Already implemented and tested: `apply_remote`, conflict resolution, convergence
of concurrent writes applied in opposite orders on two engines, and discovery
string parsing. **Missing: the transport.**

### Solved: replicated writes reaching change streams ✅

An applied remote entry keeps its *originating* stamp, so it lands in the oplog
**behind** the local tail, and a subscriber past that point would never see it.
Resolved with option 1 — a second index over local arrival order, which streams
follow while replication keeps origin order. See [Oplog](oplog.md).

### Solved: collection identity across nodes ✅

Not in the original plan, and found while starting the transport. Collection
ids came from a **node-local counter**, and every oplog entry names its
collection by id — so two nodes that created the same collection in a different
order disagreed about which collection an entry referred to, and a replicated
write would land in the wrong one. Silently, and only for peers whose creation
order differed.

Ids are now derived from `(database, name)`, so every node computes the same
answer with no coordination. [ADR-031](decisions.md).

### Built: version vectors and anti-entropy ✅

`oplog_versions` tracks `{node → max_hlc}`, maintained on every oplog append and
rebuilt on open if it disagrees with the log. `VersionVector::behind` answers
"where must a peer start sending", `entries_for_peer` produces the range, and
`apply_batch` merges it. Unique-violation entries are excluded, as they must be.

All of it is **transport-free** and tested between engines in one process:
two-node convergence, three-node transitive convergence through a middle peer,
conflicting writes agreeing on a winner, deletes replicating, and a stable
second round that transfers nothing.

### Built: DDL replication ✅

Five entry kinds — `CreateCollection`, `DropCollection`, `CreateIndex`,
`DropIndex`, `ConfigureVectors` — each with its own payload and its own
idempotency rule. Operations rather than a metadata snapshot, so two nodes
adding *different* indexes during a partition both keep theirs.
[ADR-033](decisions.md).

### Built: the replication transport ✅

TCP, length-prefixed BSON, mutually authenticated with `cluster_secret`. Every
node periodically asks every discovered peer what it holds and pulls what it
lacks. Verified between two real daemons: a collection, a unique index and a
document replicated with no configuration beyond a seed address; writes
converged in both directions; a partition healed; and a cross-node unique
violation left both documents in place while both nodes reported it.
[ADR-035](decisions.md).

### Built: snapshot resync ✅

A peer below the sender's retention horizon is told so and sent current state
instead of history. Without it, a node added to a cluster older than
`oplog_retention_secs` received nothing it could apply and retried forever —
which, at the default retention, is any node added to a running cluster.
[ADR-036](decisions.md).

### Built: SWIM membership, over local peer health ✅

Two layers. Local bookkeeping — exponential backoff for peers that fail, and a
fixed fanout per round — keeps this node's costs bounded. SWIM via `foca` over
UDP gives the *cluster* a shared opinion about who is alive, with suspicion and
indirect probing, and learns members that were never configured.
[ADR-037](decisions.md).

Verified on three daemons: the third was told only about the first and learned
the second by gossip; killing one had both survivors agree it was down within
milliseconds of each other.

### Still missing
- **SRV discovery** — `dns-srv:` parses but does not resolve; SRV records need a
  DNS resolver that can read record types the standard library does not expose.
  `dns:` and `k8s:` both work.


### Resolved: dropped collections leave a tombstone ✅

They were protected only by the `DropCollection` oplog entry, so past
`oplog_retention_secs` a rejoining partitioned peer resurrected the collection
and every document in it. Now `collections_dropped` records the drop's stamp
and is collected under `tombstone_retention_secs`, matching how document
deletes already worked. [ADR-034](decisions.md).

**Still inherent:** a peer partitioned for longer than
`tombstone_retention_secs` can still resurrect, for collections and documents
alike. That is the documented trade of tombstone-based deletion in an
eventually-consistent store, not a bug — but it is why the setting must exceed
your worst tolerable partition.

### Uniqueness violation detection — committed for M4

Unique indexes carry an `enforcement` mode. The `local` default enforces on the
accepting node only; two nodes can each accept a conflicting write during a
partition, and last-writer-wins would otherwise discard one **silently**.

M4 must therefore detect violations at merge time and surface them:

- a `uniqueViolation` change-stream event naming the index and the colliding ids — ✅ **built**
- the losing document recorded rather than lost, so it can be reconciled — ✅ **moot, see below**
- a metric, so the condition is visible without watching a stream — ✅ **built** (`kimmy_unique_violations`)

**Correction found while building it: there is no losing document.** The two
colliding documents have different `_id`s, so last-writer-wins never runs on
them — both are applied and both survive. Only the constraint is broken. So
nothing needs recording to avoid being lost; what needed deciding was what to
do when maintaining the index at merge finds the key already held. The answer
is to add the entry anyway and report: skipping it would keep the index
formally unique at the cost of an index-backed query being unable to find a
document that exists, trading a reported problem for a silent wrong answer.

**Still to do in M4:** `OpKind::UniqueViolation` entries are a node's own
observation, not replicated facts — every node detects the same collision when
it merges. Anti-entropy must **exclude them** when shipping oplog ranges to
peers, or the same violation is reported once per node.

This does not *prevent* the violation — that is provably impossible without
coordination (see [ADR-020](decisions.md)) — but it converts silent corruption
into an actionable event, which is most of the value.

`coordinated` enforcement (value-ownership routing, real cluster-wide guarantee,
CP for those writes) stays reserved and is refused at index-creation time until
it exists.

Also open: whether `drop_collection` should replicate as a tombstone (a
partitioned peer could otherwise resurrect a dropped collection), and how to
handle a node whose tombstones were collected while it was partitioned.

---

## M5 — Hardening ✅

| | |
|---|---|
| Backup / restore | ✅ Done — online snapshot over `GET /v1/admin/backup`, offline `kimmyd restore`. [ADR-041](decisions.md). Point-in-time restore rewinds a restored backup with `--until`, refusing what the oplog cannot reconstruct — [ADR-044](decisions.md) |
| TLS | ✅ Done, both fronts. Clients: — `axum-server` over `rustls` on the `ring` provider already in the build, enabled by naming a cert and key. [ADR-039](decisions.md). Node↔node: TLS bound to `cluster_secret` by channel binding rather than to certificates, so there is no PKI to run — [ADR-040](decisions.md) |
| Rate limiting | ✅ Done for `/v1/auth/login` — token bucket per caller, checked *before* the Argon2 verify it exists to bound. [ADR-038](decisions.md). Other routes deliberately deferred to the benchmark work below |
| Oplog & tombstone GC | ✅ Done — background pass, `storage.gc_interval_secs`. [ADR-028](decisions.md) |
| Aggregation pipeline | ✅ Done — those six plus `$skip`, `$count` and `$lookup`, with a hard document ceiling on blocking stages. Also lands the MCP `aggregate` tool. [Aggregation](aggregation.md) |
| `kimmy` CLI | ✅ Done — one-shot subcommands over the HTTP API, JSON on stdout. An interactive shell was considered and deferred: it is the same command surface plus a terminal UI. [CLI](cli.md) |
| Benchmarks | 🚧 Vector index, write path and query planner measured. No crossover existed, so `MIN_VECTORS_FOR_INDEX` dropped 2,000 → 500; and every write costs one durable commit, which makes secondary indexes free on the write path. [Benchmarks](benchmarks.md) |
| Audit log | ✅ Done — emitted from the single authorization point, four modes, `kimmy::audit` target. [ADR-042](decisions.md) |
| Richer metrics | ✅ Mostly — request rates, response classes, storage size, denial and rate-limit counters. **Latency histograms and oplog lag are not built**, and are documented as such rather than guessed. [ADR-043](decisions.md) |

---

## M6 — Webhooks: registration and push 📋

A developer registers a URL and the cluster **pushes** change events to it.
Change streams already carry these events, but the client must open and hold a
WebSocket; this is for consumers that cannot — serverless functions, queues,
other services.

### The dispatcher is another oplog consumer

```mermaid
graph LR
    W["Document write"] --> O[("Oplog")]
    O --> C1["Change streams<br/><i>client holds a socket</i>"]
    O --> C2["Embedding worker"]
    O --> C3["Anti-entropy"]
    O --> C4["<b>Webhook dispatcher</b><br/><i>the node dials out</i>"]
    C4 --> H1["POST https://…"]

    style C4 fill:#2d3748,color:#fff
```

| Need | Already exists |
|---|---|
| Durable resume point | `Engine::consumer_position`, as the embedding worker uses |
| Crash redelivers rather than skips | Position recorded **after** the work |
| A globally unique event id | `Stamp { hlc, node }` on every entry |
| A registry that replicates, backs up, restores | A collection in `__kimmy`, like `__users` |
| Who is alive right now | SWIM — `Members::snapshot()` ([ADR-037](decisions.md)) |
| "What has not been delivered yet?" | `VersionVector::behind` — the same question anti-entropy asks |

### The problem, and the way out

Two obvious designs both fail, in opposite directions:

- **The originating node delivers.** Exactly one delivery — until that node dies
  before dispatching, and then *nobody* delivers, because its peers hold the
  data but consider it not theirs. A client silently never gets an event.
- **Every node delivers.** Nothing is lost, and a five-node cluster fires five
  identical requests for every write.

Both treat *which node delivers* as fixed in advance. The way out is to stop
fixing it and make **delivery progress replicated state**:

> Each node records, per subscription, a `VersionVector` of what it has
> delivered — "for origin node X, delivered through HLC H". A node writes only
> **its own** record, so there are no write conflicts and nothing to merge by
> last-writer-wins. Any node reads the union to know what the cluster as a whole
> has already sent.

Once progress converges like any other data, *any* node can deliver and every
node knows what is already gone. The question shrinks to "who goes first", which
has a cheap answer that survives a death.

### Who goes first: a derived owner

`owner(subscription) = rendezvous_hash(subscription_id, live_members)`.

A pure function of the SWIM member set. **This is not leader election** — there
is no vote, no term, no consensus, and no cluster-wide coordinator. Each node
computes the same answer independently, different subscriptions land on
different nodes, and a disagreement produces a duplicate rather than a split
brain.

When the owner dies, SWIM removes it, every node recomputes, and the new owner
**resumes from the union of progress** rather than from the beginning. Failover
costs roughly what SWIM detection costs — measured in milliseconds during M4.

**So: does a client miss an event when a node dies?** No. The only way an event
is never delivered is if the write never replicated off the node that accepted
it — in which case the data itself is gone from the database too, and webhooks
are not the thing that failed.

### The guarantee, stated plainly

**At-least-once, ordered per subscription.** Exactly-once is not achievable over
a network by any design, and pretending otherwise would set an expectation the
system cannot keep.

What makes duplicates harmless is that every delivery carries a stable,
globally unique id — the originating `Stamp` — in the body and in a header, so
deduplicating is a set-membership test. Duplicates should be rare (a membership
disagreement, or a redelivery after a crash) rather than routine.

Ordering holds per subscription per origin node. There is no total order across
nodes, which is what the leaderless design says everywhere else.

### Decisions taken

| Decision | Chosen | Why |
|---|---|---|
| Who delivers | Derived owner via rendezvous hash over SWIM members, with replicated progress | Leaderless, loses nothing on node death, one delivery in the normal case |
| Egress | Private ranges blocked by default; `webhooks.allowed_hosts` to permit | Closes the SSRF hole while letting public endpoints work with no configuration |
| Registering requires | A new **`webhook`** action | Handing out an egress path should be explicit, not bundled into reading |

The cost, stated: one small progress write per delivered batch, which
replicates. That is the price of not losing events, and it is why progress is a
high-water mark per batch rather than a record per event.

### Tasks

| # | Task | Notes |
|---|---|---|
| 1 | ✅ **`webhook` RBAC action** | Add to `Action`, decide implication: `admin` implies it; `read`/`write`/`watch` do **not**. Independent, like `watch` ([ADR-014](decisions.md)) |
| 2 | ✅ **Subscription registry** | `__kimmy.__webhooks`: url, collection, operation filter, secret, state, created-by. A collection, so it replicates, backs up and restores unchanged |
| 3 | **Progress records** | `__kimmy.__webhook_progress`, `_id = {subscription}:{node}`, holding a `VersionVector`. Written only by its own node, so conflict-free by construction |
| 4 | ✅ **Registration API** | `POST/GET/DELETE /v1/db/{db}/coll/{coll}/webhooks`, gated on `webhook`. Deliberately **no MCP tool** at first: registering an egress path is not a reading act |
| 5 | **Ownership** | Rendezvous hash over `Members::snapshot()`. Must degrade correctly with clustering off — a single node owns everything — and when membership is momentarily empty |
| 6 | **Dispatcher worker** | One per node. Computes owned subscriptions, reads union progress, uses `VersionVector::behind` to find undelivered entries, batches by size and max delay so a bulk load is not one request per document |
| 7 | **Delivery** | HTTP POST, change-stream event shape, timeout, `X-Kimmy-Event-Id` and `X-Kimmy-Signature` headers |
| 8 | **Signing** | HMAC-SHA256 over the body with a per-subscription secret, plus a timestamp against replay. `hmac`/`sha2` are already in the build for the cluster handshake |
| 9 | ✅ **Egress policy** — registration-time checking done; the re-check before each delivery lands with the dispatcher | Resolve the host and check the **resolved** address, refuse redirects, block loopback/link-local/RFC1918 unless allowlisted. A name that is public at registration must not be able to point inward at delivery |
| 10 | **Failure handling** | Exponential backoff; invalidate the subscription when its progress falls past `oplog_retention_secs`, exactly as a lagging change-stream consumer gets `410` |
| 11 | **Observability** | `kimmy_webhook_deliveries_total{outcome}`, `kimmy_webhook_subscriptions`, backlog age; an audit record on register and remove, since registering one grants ongoing egress |
| 12 | **Tests** | A local receiver over a real socket: delivery, batching, signature verification, retry after failure, invalidation past retention, a deleted subscription stopping, ownership failover with a node removed from the member set, and that a private-range URL is refused |
| 13 | **Docs and ADRs** | One ADR for derived ownership plus replicated progress — including why it is not leader election — and one for the egress policy |

### Open questions to settle during

- **Rendezvous input.** `Members` holds `SocketAddr`; hashing node ids would be
  stabler across a re-address. May need a member → node-id mapping.
- **Payload cap.** `fullDocument` on large documents, batched, can produce a
  very large POST. Needs a cap and a decision on what happens to a single event
  that exceeds it.
- **Per-node delivery cap**, so a webhook on a hot collection cannot saturate a
  node's outbound connections.
- **Does the registry replicate?** It will, being a collection — which is what
  lets any node take ownership. Worth confirming that is wanted.

---

## Explicitly not planned

| | Why |
|---|---|
| Multi-document transactions | Requires coordination — the thing a leaderless design forgoes |
| `$where` / JavaScript execution | An obvious injection surface |
| MongoDB wire protocol | Considered and rejected; see [Decisions](decisions.md) |
| Geospatial indexes | Out of scope |
| Field-level encryption | Use an encrypted volume |
| Full-text indexes | Superseded by vector and hybrid search |

---

## Next

- [Decisions](decisions.md) — the choices already settled
- [Testing](testing.md) — the invariants any change must preserve
