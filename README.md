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

> **Status: early development.** Multi-user document CRUD, Mongo-style queries,
> secondary indexes, live change streams over WebSocket, automatic embeddings
> with vector and hybrid search, and an in-process MCP server at `/mcp` all
> work. **Clustering works too** — every node accepts writes, membership is
> SWIM and convergence is anti-entropy over the oplog; `docker-compose.yml`
> brings up three nodes. See [Roadmap](#roadmap).

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

Releases are cut by tag; every artifact below appears with the first `v*`
release. The server ships as a container image, the CLI ships everywhere.

```bash
# The server — multi-arch (amd64 + arm64) image on GHCR
docker run --rm -p 7878:7878 \
  -e KIMMY_ROOT_PASSWORD=change-me \
  -e KIMMY_JWT_SECRET=a-long-random-secret \
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
# From source
KIMMY_ROOT_PASSWORD=change-me KIMMY_JWT_SECRET=a-long-random-secret \
  cargo run --bin kimmyd -- --bind 127.0.0.1:7878 --data-dir ./data

# Docker
docker build -t kimmydb .
docker run --rm -p 7878:7878 \
  -e KIMMY_ROOT_PASSWORD=change-me \
  -e KIMMY_JWT_SECRET=a-long-random-secret \
  -v kimmy-data:/var/lib/kimmy \
  kimmydb
```

Then drive it:

```bash
TOKEN=$(curl -s -XPOST localhost:7878/v1/auth/login \
  -H 'content-type: application/json' \
  -d '{"user":"root","password":"change-me"}' | jq -r .token)
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
| `auth.root_password` | `KIMMY_ROOT_PASSWORD` | Required unless `--insecure-no-auth`. Bootstrap superuser, created on first start only. |
| `auth.jwt_secret` | `KIMMY_JWT_SECRET` | **Required whenever auth is on**, single node or cluster — the node refuses to start without one rather than sign tokens with a built-in constant. 16 bytes minimum, and **identical on every node**, or a token issued by one node is rejected by the next. |
| `auth.oidc.issuer` | `KIMMY_OIDC_ISSUER` | Optional. Federate with an external OIDC provider alongside local users; obliges `auth.oidc.audience`, and `https` only. `admin` cannot be granted through it. See [Security](docs/security.md). |
| `cluster.seeds` | `KIMMY_SEEDS` | Where to look for peers. `k8s:<headless-svc>`, `dns:<name>`, `dns-srv:<name>`, `static:<host:port,...>`, or a bare `host:port`. |
| `cluster.cluster_secret` | `KIMMY_CLUSTER_SECRET` | Authenticates node-to-node traffic. Required when clustering. |
| `storage.tombstone_retention_secs` | — | Must exceed your worst tolerable partition, or deleted documents resurrect. See below. |
| `storage.gc_interval_secs` | — | How often retention is enforced (default 10 min). `0` disables collection and the oplog grows without bound. |

`--insecure-no-auth` is refused on any non-loopback bind address, and clustering
is refused without seeds and secrets. These are startup errors, not runtime
surprises.

## Consistency model

Read this before building on it.

- **Single-document writes are atomic and durable** on the node that accepts them.
- **Read-your-writes holds only on the node you wrote to.** There are no
  cross-node read guarantees.
- **Conflicts resolve by last-writer-wins** at whole-document granularity, using
  a hybrid logical clock with the node id as a tiebreak. Concurrent writes to the
  same document mean the losing write is discarded, not merged.
- **There is no multi-document atomicity.** No transactions across documents.
- **Deletes are tombstones with a retention window.** If a partition outlasts
  `tombstone_retention_secs`, documents deleted during it can resurrect when the
  partition heals. Set the window longer than any partition you would tolerate.

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
| [Security](docs/security.md) · [Operations](docs/operations.md) | Running it |
| [Roadmap](docs/roadmap.md) · [Decisions](docs/decisions.md) · [Testing](docs/testing.md) | Continuing development |
