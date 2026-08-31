# KimmyDB

A JSON document database in Rust, built around three things that are usually
awkward to get together:

- **Change streams on a single instance.** No replica set, no cluster, no
  ceremony. Start one container and subscribe to changes.
- **Leaderless clustering.** No primary, no elections, no quorum. Nodes gossip
  membership over SWIM and gossip state by anti-entropy — each contacts a few
  peers per round, pulls the oplog entries it is missing, and data reaches the
  whole cluster transitively. Discovery is DNS or a Kubernetes headless Service.
- **AI-native storage.** Embeddings are generated and maintained automatically
  per collection, and an MCP server runs *inside* the database so agents can
  query it directly.

📚 **[Full documentation is in `docs/`](docs/README.md)** — architecture, internals,
API reference, operations, and the decision record.

> **Status: early development, released.** Multi-user document CRUD, Mongo-style
> queries and aggregation, secondary indexes, live change streams over
> WebSocket, signed webhooks, automatic embeddings with vector and hybrid
> search, an in-process MCP server at `/mcp`, OIDC federation, and a
> versioned `/v1` contract with Rust, Python and Go clients all work.
> **Clustering works too** — every node accepts writes, membership is SWIM and
> convergence is anti-entropy over the oplog; `docker-compose.yml` brings up
> three nodes. Pre-1.0: a minor release may break things and the
> [changelog](CHANGELOG.md) says so. See [Roadmap](#roadmap).

## Why it is built this way

The oplog is the spine. Every mutation appends one durable, HLC-ordered entry,
and three subsystems consume that same log:

1. **Change streams** — WebSocket subscribers, resumable by token.
2. **The embedding pipeline** — an internal subscriber, which is why
   auto-embeddings need no separate scheduler.
3. **Cluster anti-entropy** — peers pull ranges they are missing.

Building the log once and reusing it three times is exactly why single-instance
change streams work here. In MongoDB they are a byproduct of replication, so
they require a replica set. Here the log exists whether or not the node has ever
seen a peer.

## Install

Releases are cut by tag. The server ships as a container image, the CLI ships
everywhere.

```bash
# The server — multi-arch (amd64 + arm64) image on GHCR. Pick a root password
# and generate the signing key (32 bytes minimum): a container listens on
# every interface, and off loopback the node refuses an example's value for
# either secret rather than start with one everybody knows.
export KIMMY_ROOT_PASSWORD=$(openssl rand -base64 18)
docker run --rm -p 7878:7878 \
  -e KIMMY_ROOT_PASSWORD \
  -e KIMMY_JWT_SECRET="$(openssl rand -base64 32)" \
  -v kimmy-data:/var/lib/kimmy \
  ghcr.io/titusai-io/kimmydb:latest

# The CLI — Homebrew (macOS, arm64 and x86_64)
brew install titusai-io/tap/kimmy
```

Prebuilt tarballs with SHA256 checksums for both binaries — macOS (arm64,
x86_64) and Linux (arm64, x86_64, statically linked against musl, so they run
on any distribution) — are on the
[releases page](https://github.com/titusai-io/kimmydb/releases). Versioning
policy is in [Compatibility](docs/compatibility.md): pre-1.0, a minor may
break things and the [changelog](CHANGELOG.md) says so; a patch never does.

## Quick start

```bash
# Pick a root password once; both variants below and the login after them
# read it from the environment.
export KIMMY_ROOT_PASSWORD=$(openssl rand -base64 18)

# From source
KIMMY_JWT_SECRET=$(openssl rand -base64 32) \
  cargo run --bin kimmyd -- --bind 127.0.0.1:7878 --data-dir ./data

# Docker
docker build -t kimmydb .
docker run --rm -p 7878:7878 \
  -e KIMMY_ROOT_PASSWORD \
  -e KIMMY_JWT_SECRET="$(openssl rand -base64 32)" \
  -v kimmy-data:/var/lib/kimmy \
  kimmydb
```

Then drive it:

```bash
TOKEN=$(curl -s -XPOST localhost:7878/v1/auth/login \
  -H 'content-type: application/json' \
  -d "{\"user\":\"root\",\"password\":\"$KIMMY_ROOT_PASSWORD\"}" | jq -r .token)
A="Authorization: Bearer $TOKEN"

curl -s -XPOST localhost:7878/v1/db/shop/collections -H "$A" -d '{"name":"orders"}'
curl -s -XPOST localhost:7878/v1/db/shop/coll/orders/docs -H "$A" \
  -d '{"item":"widget","qty":5,"tags":["a","b"]}'

# Mongo-style query, sort, and projection
curl -s -XPOST localhost:7878/v1/db/shop/coll/orders/find -H "$A" \
  -d '{"filter":{"qty":{"$gt":4},"tags":"a"},"sort":{"qty":-1}}'

# Automatic embeddings -- maintained off the write path by an oplog consumer.
# Needs a provider that can embed; the `byo` default means you supply vectors.
curl -s -XPOST localhost:7878/v1/db/shop/coll/orders/vector -H "$A" \
  -d '{"fields":["item"],"dim":768,"metric":"cosine",
       "provider":{"kind":"ollama","model":"nomic-embed-text",
                   "endpoint":"http://localhost:11434"}}'

# ...then search it semantically
curl -s -XPOST localhost:7878/v1/db/shop/coll/orders/vector_search -H "$A" \
  -d '{"query":"small mechanical part","k":5}'

# Live change stream -- no replica set required
websocat "ws://localhost:7878/v1/db/shop/coll/orders/watch?full_document=true" \
  -H "$A"
```

## API

| Method | Path | Notes |
|---|---|---|
| `POST` | `/v1/auth/login` | Returns a JWT |
| `GET` | `/v1/auth/whoami` | Current principal and grants |
| `GET`/`POST` | `/v1/users` | List / create (server admin only) |
| `GET`/`DELETE` | `/v1/users/{name}` | Inspect / remove |
| `POST` | `/v1/users/{name}/password` | Own password, or any with admin |
| `POST` | `/v1/users/{name}/grants` | Replace a user's grants |
| `GET` | `/v1/databases` | Filtered by what you may read |
| `GET`/`POST` | `/v1/db/{db}/collections` | List / create |
| `DELETE` | `/v1/db/{db}/coll/{coll}` | Drop |
| `POST` | `/v1/db/{db}/coll/{coll}/docs` | Insert |
| `GET`/`PUT`/`DELETE` | `/v1/db/{db}/coll/{coll}/docs/{id}` | By `_id` |
| `POST` | `/v1/db/{db}/coll/{coll}/find` | Filter, sort, projection, limit, skip |
| `POST` | `/v1/db/{db}/coll/{coll}/count` | Count matching |
| `POST` | `/v1/db/{db}/coll/{coll}/update` | Update operators, `multi` |
| `POST` | `/v1/db/{db}/coll/{coll}/delete` | Delete matching |
| `GET` | `/v1/db/{db}/coll/{coll}/describe` | Sampled schema: field paths, types, presence |
| `GET`/`POST` | `/v1/db/{db}/coll/{coll}/indexes` | List / create a secondary index |
| `DELETE` | `/v1/db/{db}/coll/{coll}/indexes/{name}` | Drop |
| `GET`/`POST`/`DELETE` | `/v1/db/{db}/coll/{coll}/vector` | Inspect / enable / disable embeddings |
| `GET`/`PUT`/`DELETE` | `/v1/db/{db}/coll/{coll}/docs/{id}/vectors` | Client-supplied vectors, for the `byo` provider |
| `POST` | `/v1/db/{db}/coll/{coll}/vector_search` | k-NN, with an optional filter |
| `POST` | `/v1/db/{db}/coll/{coll}/hybrid_search` | Vector + keyword, fused by RRF |
| `GET` | `/v1/db/{db}/coll/{coll}/watch` | WebSocket change stream |
| `POST` | `/mcp` | MCP for agents — see [MCP](docs/mcp.md) |
| `GET` | `/healthz` `/readyz` `/metrics` | Unauthenticated |

Documents cross the boundary as JSON, using Extended JSON v2 (`{"$oid":…}`,
`{"$date":…}`, `{"$numberLong":…}`) for types JSON cannot express. Whole numbers
stay integers rather than widening to double, so `$type` queries keep working
and values above 2^53 survive exactly.

Check what a given configuration resolves to without starting the server:

```bash
kimmyd --config kimmy.example.toml check-config
```

## Configuration

Three sources, lowest precedence first: **defaults**, then a **TOML file**, then
**CLI flags** (each of which also reads a `KIMMY_*` environment variable).
See [`kimmy.example.toml`](kimmy.example.toml) for every setting with commentary.

The settings worth knowing before you deploy:

| Setting | Env var | Why it matters |
|---|---|---|
| `auth.root_password` | `KIMMY_ROOT_PASSWORD` | Required unless `--insecure-no-auth`. Bootstrap superuser, created on first start only. Off loopback, a value from this repository's examples (`changeme`, `hunter2`, …) is refused. |
| `auth.jwt_secret` | `KIMMY_JWT_SECRET` | **Required whenever auth is on**, single node or cluster — the node refuses to start without one rather than sign tokens with a built-in constant. 32 bytes minimum, and **identical on every node**, or a token issued by one node is rejected by the next. |
| `auth.jwt_previous_secret` | `KIMMY_JWT_PREVIOUS_SECRET` | Optional, during a rotation only: the secret being retired. Tokens it signed stay valid while it is set; new tokens are signed with `jwt_secret`. Held to the same minimum and the same placeholder refusal. Remove it one `token_ttl_secs` after rolling the new secret out. See [Rotating the signing secret](docs/security.md#rotating-the-signing-secret). |
| `auth.oidc.issuer` | `KIMMY_OIDC_ISSUER` | Optional. Federate with an external OIDC provider alongside local users; obliges `auth.oidc.audience`, and `https` only. `admin` cannot be granted through it; `ddl` can. See [Security](docs/security.md). |
| `cluster.seeds` | `KIMMY_SEEDS` | Where to look for peers. `k8s:<headless-svc>`, `dns:<name>`, `dns-srv:<name>`, `static:<host:port,...>`, or a bare `host:port`. |
| `cluster.cluster_secret` | `KIMMY_CLUSTER_SECRET` | Authenticates node-to-node traffic. Required when clustering. |
| `storage.tombstone_retention_secs` | — | Must exceed your worst tolerable partition, or deleted documents resurrect. See below. |
| `storage.gc_interval_secs` | — | How often retention is enforced (default 10 min). `0` disables collection and the oplog grows without bound. |

`--insecure-no-auth` is refused on any non-loopback bind address, and clustering
is refused without seeds and secrets. These are startup errors, not runtime
surprises.

## Data guarantees: ACID where, BASE where

Read this before building on it. The short form: **ACID at the granularity of
one request on one node; BASE across the cluster** — AP by design, not by
accident.

**On the node that accepts a write, it is ACID.** Every write is one redb
transaction holding the document, its index entries and its oplog entry, and
the transaction is fsynced before the response returns — so what a node
acknowledges survives a crash, and a reader never sees half a write.

| Scope | Guarantee |
|---|---|
| One document, one request | Atomic, isolated, durable. Whole-document replacement or operator update; `if_stamp` makes it a compare-and-set against the version a previous response returned (`409 stale` otherwise) |
| `update` / `delete` by filter | Atomic read-modify-write inside the write transaction — concurrent `$inc`s all land |
| `find_and_modify` | Atomic claim-and-return; two callers never claim the same document |
| Bulk insert (`insert_many`) | All or nothing: a duplicate `_id` anywhere inserts nothing |
| `update` / `delete` with `multi: true` | Atomic **per chunk** of `storage.multi_chunk_docs` documents (default 1,000); the writer is released between chunks and the response's `commits` says how many landed |
| Reads | Snapshot-isolated per request |
| Anything across two requests | **No guarantee.** There are no multi-request or multi-document transactions, and none are planned |

**Across the cluster, it is BASE** — basically available, soft state,
eventually consistent:

- **Every node accepts writes**; there is no primary and no quorum.
- **Read-your-writes holds only on the node you wrote to.** There are no
  cross-node read guarantees.
- **Conflicts resolve by last-writer-wins** at whole-document granularity, on a
  hybrid logical clock with the node id as a tiebreak. The losing write is
  discarded, not merged.
- **Unique indexes are enforced per node**; a collision that arrives by
  replication is recorded, surfaced as a `uniqueViolation` event, and queryable
  at `.../violations`, rather than refused.
- **Deletes are tombstones with a retention window.** If a partition outlasts
  `storage.tombstone_retention_secs`, documents deleted during it can resurrect
  when it heals. Set the window longer than any partition you would tolerate;
  a peer that rejoins from further back than that is named in `/v1/topology`.

Per operation — what each route promises, where the engine enforces it, and
the test that defends it — see
["What each operation guarantees"](docs/compatibility.md#what-each-operation-guarantees).
That table is the authority when any other document disagrees with it.

### Durability classes

`storage.durability` chooses how a commit reaches the disk
([ADR-088](docs/decisions.md)). Both classes are durable when the response
returns; there is deliberately no class that is not.

| Class | Mechanism | Use it when |
|---|---|---|
| `durable` (default) | Every commit fsyncs before it returns | Always safe; a single ingest loop should stay here or batch |
| `coalesced` | A commit waits for the next shared fsync, one per `commit_coalesce_ms` window (default 5 ms), so N concurrent writers pay one fsync rather than N | Many concurrent writers each insisting on their own commit |

### Write speed, as measured

Every number here is from [Benchmarks](docs/benchmarks.md), which records
method and machine; they are useful as ratios rather than absolutes.

- **The commit is the cost.** A durable single-document write is **~3.4 ms at
  the engine** (~290/s), and document size and index count disappear
  underneath it — zero, one and two secondary indexes cost the same. Through
  the HTTP API the same insert is **~7.0 ms** at one client, because the
  embedding worker's position record adds a second commit.
- **Concurrency does not raise it.** One to eight concurrent writers under
  `durable` land 296 → 304 docs/s — flat, because redb has one writer and they
  share it cleanly. Under `coalesced`, sixteen writers go from 182 to
  **1,114 docs/s** (6.1×); a lone writer is *slower* under `coalesced`
  (79 vs 170/s), because it waits a window for company that never comes.
- **Batching does.** A bulk insert of 1,000 documents commits once and lands
  **51,320 docs/s** at the engine — the marginal document costs ~13 µs. Over a
  socket, one client gets **143 inserts/s** one at a time and **7,300 docs/s**
  in batches of 100; thirty-two clients get **24,400 docs/s**. On a real
  three-node cluster over a LAN, batches of 250 sustained ~2,000 docs/s.
- **Reads scale; writes do not.** Point reads go from 8,000/s at one client
  to ~70,000/s at thirty-two; a `find` page of 100 in ~0.5 ms; an indexed
  equality lookup in 0.003 ms against an 8 ms scan of 10,000 documents.
- **The tail shows the single writer.** Thirty-two contending single-document
  writers push p99 from 10 ms to 246 ms.

The question to ask of a write workload is not "how many writes per second"
but **"can these writes be batched, or are there enough concurrent writers to
share an fsync?"** — and if either answer is yes, throughput is unlikely to be
the constraint.

These are the normal consequences of choosing leaderless availability over
coordination. They are stated up front because the failure mode of an
eventually-consistent store is a user who assumed otherwise.

## Roadmap

| Milestone | Scope | Status |
|---|---|---|
| **M0** | Workspace, core types, HLC, config, Docker, CI | ✅ Complete |
| **M1** | Storage engine, CRUD, queries, indexes, oplog, change streams, auth, HTTP API | ✅ Complete |
| **M2** | Auto-embeddings, HNSW vector index, vector and hybrid search | ✅ Complete |
| **M3** | Built-in MCP server over streamable HTTP | ✅ Complete |
| **M4** | Gossip membership, DNS/k8s discovery, anti-entropy replication | ✅ Complete |
| **M5** | Rate limiting, TLS, benchmarks, aggregation, backup and point-in-time restore, audit log, metrics, CLI | ✅ Complete |
| **M6** | Webhooks — register a URL, the node pushes change events to it | ✅ Complete |
| **M7** | Query engine completion — the planner's carried gaps | ✅ Complete |
| **M8** | Prove, persist, polish — cluster harness, vector durability, observability | ✅ Complete |
| **M9** | Computed expressions, TTL, `findAndModify`, partial indexes, cursors | ✅ Complete |
| **M10** | The client protocol, formalized — `openapi.yaml`, Rust, Python and Go clients | ✅ Complete |
| **M11** | Index-ordered scans — sorted queries that stop early | 🚧 Task 1 of 5; paused |

Where the build departs from what was planned — and why — is tracked in
[Deviations](docs/deviations.md), in one place rather than scattered.

## Architecture

```
                    ┌──────────────────────────────────────────┐
   HTTP/WS  ──────► │  kimmy-api (axum)   │  kimmy-mcp (rmcp)  │
                    ├──────────────────────────────────────────┤
                    │  kimmy-auth  — JWT, Argon2id, RBAC       │
                    ├──────────────────────────────────────────┤
                    │  kimmy-query — filter / update / pipeline │
                    ├──────────────┬───────────────────────────┤
                    │ kimmy-storage│ kimmy-vector              │
                    │ redb + oplog │ embeddings + HNSW         │
                    └──────┬───────┴─────────┬─────────────────┘
                           │   OPLOG (the spine)
                           ▼                 ▼
                    change streams    kimmy-cluster (SWIM + anti-entropy)
```

| Crate | Responsibility |
|---|---|
| `kimmy-core` | `Hlc`, `Stamp`, `NodeId`, `DocId`, `DocRecord`, `OplogEntry`. No I/O. |
| `kimmy-storage` | redb layout, collections, secondary indexes, oplog, tombstone GC |
| `kimmy-query` | Filter and update operators, projection, sort, aggregation-lite |
| `kimmy-vector` | Embedding providers, oplog-driven worker, HNSW, index selection, search |
| `kimmy-auth` | Users, Argon2id, JWT, RBAC evaluation |
| `kimmy-cluster` | SWIM membership, discovery, the replication protocol, anti-entropy |
| `kimmy-mcp` | MCP tools and resources — calls the same executor the REST routes do, so authorization cannot diverge |
| `kimmy-api` | axum router, REST handlers, change-stream WebSocket, and the executor both edges share |
| `kimmyd` | The server binary |
| `kimmy-cli` | Terminal client |

## Development

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

Some invariants carry disproportionate weight, because breaking them produces
wrong answers rather than crashes. These are property-tested:

- **Key encoding order.** `keyenc::encode(a).cmp(encode(b))` must equal
  `canonical_cmp(a, b)` for every pair of values, because indexes are redb key
  ranges compared with `memcmp`. The encoder and the comparator are written
  independently and cross-checked; numeric ordering is additionally checked
  against an oracle sharing no code with either, since both encode through the
  same decomposition. See `crates/kimmy-core/src/{keyenc,cmp}.rs`.
- **HLC monotonicity.** No sequence of physical timestamps, however adversarial,
  may produce a non-increasing logical clock. See `crates/kimmy-core/src/hlc.rs`.
- **Last-writer-wins convergence.** Merge must be commutative and idempotent, or
  replicas will not converge. See `crates/kimmy-core/src/record.rs`.
- **Change-stream continuity.** A subscriber that disconnects and resumes under
  continuous writes must receive every event exactly once. See
  `crates/kimmy-storage/src/watch.rs`.

## Documentation

| | |
|---|---|
| [Architecture](docs/architecture.md) | Crate layout, layering, the oplog spine |
| [Storage](docs/storage.md) · [Key Encoding](docs/key-encoding.md) | On-disk formats and order-preserving bytes |
| [Time & Conflicts](docs/time-and-conflicts.md) · [Oplog](docs/oplog.md) | HLC, last-writer-wins, the shared log |
| [Change Streams](docs/change-streams.md) | The replay/live splice, resume, lag recovery |
| [Query Language](docs/query-language.md) · [HTTP API](docs/http-api.md) | Using it |
| [Vectors](docs/vectors.md) · [MCP](docs/mcp.md) | Embeddings, search, and the agent surface |
| [Security](docs/security.md) · [Federation](docs/federation.md) · [Operations](docs/operations.md) | Running it |
| [CLI](docs/cli.md) · [Clients](docs/clients.md) · [Compatibility](docs/compatibility.md) | The `kimmy` terminal client, the first-party libraries, and what `/v1` promises |
| [Benchmarks](docs/benchmarks.md) | What has been measured, with method |
| [Roadmap](docs/roadmap.md) · [Decisions](docs/decisions.md) · [Testing](docs/testing.md) | Continuing development |

## License

KimmyDB is developed by [Titus AI LLC](https://titusai.io).

- **Server** (`kimmyd` and the crates it is built from): [GNU AGPL-3.0](LICENSE).
  Run it anywhere, including commercially, under the terms of that license.
- **Client libraries** (`kimmy-client` for Rust, `clients/go`, `clients/python`):
  [Apache-2.0](LICENSE-APACHE). Use them in anything.
- **Commercial license**: for embedding or distributing KimmyDB without the
  AGPL's obligations, write to <licensing@titusai.io>.
