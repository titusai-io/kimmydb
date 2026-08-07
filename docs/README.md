# KimmyDB Documentation

KimmyDB is a JSON document database written in Rust. It runs as a single binary
from a terminal or a container, speaks HTTP and WebSocket, and is designed
around three things that are usually awkward to have together:

1. **Change streams that work on one node.** No replica set, no cluster, no
   ceremony.
2. **Leaderless clustering.** No primary, no elections, no quorum — gossip
   membership and eventual convergence.
3. **AI-native storage.** Embeddings maintained automatically per collection,
   and an MCP server running *inside* the database.

---

## Where to start

**New to the project?** Read [Architecture](architecture.md) first — it explains
the one structural idea everything else follows from.

**Trying to use it?** [HTTP API](http-api.md), then [Query Language](query-language.md).

**Running it?** [Operations](operations.md), then [Security](security.md).

**Building on it or continuing development?** [Roadmap](roadmap.md) for what's
planned and why, [Decisions](decisions.md) for what's already settled, and
[Testing](testing.md) for the invariants that must not break.

---

## Document map

```mermaid
graph TD
    IDX[README<br/>you are here]

    IDX --> ARCH["Architecture<br/>crates · layering · the oplog spine"]
    IDX --> USE["Using it"]
    IDX --> INT["Internals"]
    IDX --> OPS["Running it"]
    IDX --> DEV["Developing it"]

    USE --> API["HTTP API<br/>endpoint reference"]
    USE --> QL["Query Language<br/>filters · updates · sort · projection"]
    USE --> CS["Change Streams<br/>subscribing · resuming"]

    INT --> STO["Storage<br/>on-disk layout · codecs"]
    INT --> KE["Key Encoding<br/>order-preserving bytes"]
    INT --> TC["Time and Conflicts<br/>HLC · last-writer-wins"]
    INT --> OPL["Oplog<br/>the shared log"]

    OPS --> OP["Operations<br/>config · deploy · observability"]
    OPS --> SEC["Security<br/>auth · RBAC · threat model"]

    DEV --> RM["Roadmap<br/>milestones and planned design"]
    DEV --> DEC["Decisions<br/>what was chosen and why"]
    DEV --> TST["Testing<br/>invariants and how they are checked"]
```

| Document | What it covers |
|---|---|
| [Architecture](architecture.md) | Crate layout, layering, request lifecycle, the oplog spine |
| [Storage](storage.md) | redb tables, on-disk record formats, tombstones, durability |
| [Key Encoding](key-encoding.md) | Order-preserving byte encoding — the subtlest component |
| [Time & Conflicts](time-and-conflicts.md) | Hybrid logical clocks, last-writer-wins, the consistency model |
| [Oplog](oplog.md) | The shared log, its three consumers, retention |
| [Change Streams](change-streams.md) | The replay/live splice, resume tokens, lag recovery |
| [Query Language](query-language.md) | Filter and update operators, array semantics, Mongo compatibility |
| [HTTP API](http-api.md) | Endpoint reference, request and response shapes, status codes |
| [Security](security.md) | Authentication, RBAC, what is and is not defended against |
| [Operations](operations.md) | Configuration, Docker, Kubernetes, health, metrics, backup |
| [Roadmap](roadmap.md) | Milestone status and the planned design for what remains |
| [Decisions](decisions.md) | Architecture decision record — choices and their rationale |
| [Testing](testing.md) | Testing philosophy and the invariants that carry the weight |

---

## Current capabilities

Honest status. "Working" means exercised by tests and verified by driving the
running server, not merely compiled.

| Capability | Status | Notes |
|---|---|---|
| JSON documents and collections | ✅ Working | BSON on disk, Extended JSON at the edge |
| Multiple databases | ✅ Working | Created implicitly by their first collection |
| Document CRUD | ✅ Working | Insert, get, replace, delete, upsert |
| Mongo-style queries | ✅ Working | 17 filter operators, dot paths, array semantics |
| Update operators | ✅ Working | 12 operators plus whole-document replacement |
| Sort and projection | ✅ Working | Multi-key sort, inclusion/exclusion projection |
| Change streams | ✅ Working | **Single node, no replica set.** Resumable by token |
| Multiple users | ✅ Working | Argon2id, JWT, per-collection RBAC |
| HTTP + WebSocket API | ✅ Working | Also health and Prometheus metrics |
| Docker container | ✅ Working | ~93 MB, graceful SIGTERM shutdown |
| **Secondary indexes** | ⛔ Not implemented | **Every query is a collection scan** |
| Vector search & auto-embeddings | 📋 Planned (M2) | Shadow collections, HNSW |
| Built-in MCP server | 📋 Planned (M3) | Streamable HTTP, RBAC-gated tools |
| Gossip clustering | 📋 Planned (M4) | SWIM membership, oplog anti-entropy |

The replication *primitives* exist and are tested — `apply_remote` resolves
conflicts, and convergence is verified for concurrent writes — but nothing
transports them between nodes yet. See [Roadmap](roadmap.md).

---

## The one idea worth understanding

**The oplog is the spine.** Every mutation appends exactly one durable,
totally-ordered entry, in the same transaction as the change itself. Three
independent subsystems then read that same log:

```mermaid
graph LR
    W[Write] --> T[One redb transaction]
    T --> D[(Document)]
    T --> O[(Oplog entry)]

    O --> CS[Change streams<br/>WebSocket subscribers]
    O --> EM[Embedding pipeline<br/>M2]
    O --> AE[Cluster anti-entropy<br/>M4]

    style O fill:#4a5568,color:#fff
```

Building the log once and reusing it three times is why single-instance change
streams work here at all. In MongoDB they are a byproduct of replication, so
they require a replica set. Here the log exists whether or not the node has
ever seen a peer — clustering is a *consumer* of the log, not its cause.

It is also why auto-embeddings (M2) need no scheduler: the embedding worker is
an ordinary change-stream subscriber.

---

## Consistency, stated plainly

Read this before building on it. These are the normal consequences of choosing
leaderless availability over coordination, and the failure mode of an
eventually-consistent store is a user who assumed otherwise.

- Single-document writes are **atomic and durable** on the accepting node.
- **Read-your-writes holds only on the node you wrote to.**
- Conflicts resolve by **last-writer-wins at whole-document granularity**. The
  losing write is discarded, not merged.
- **No multi-document atomicity.** No transactions across documents.
- Deletes are **tombstones with a retention window**. A partition outlasting
  that window can resurrect deleted documents.

Full detail in [Time & Conflicts](time-and-conflicts.md).

---

## Repository layout

```
kimmydb/
├── crates/
│   ├── kimmy-core/      Hlc, Stamp, DocId, DocRecord, OplogEntry, keyenc, cmp
│   ├── kimmy-storage/   redb engine, documents, oplog, change streams
│   ├── kimmy-query/     filter / update / sort / projection evaluation
│   ├── kimmy-auth/      Argon2id, JWT, RBAC, user store
│   ├── kimmy-api/       axum router, WebSocket, JSON boundary
│   ├── kimmy-cluster/   discovery (membership lands in M4)
│   ├── kimmy-vector/    embeddings and HNSW (M2)
│   ├── kimmy-mcp/       MCP server (M3)
│   ├── kimmyd/          the server binary
│   └── kimmy-cli/       terminal client (M5)
├── docs/                this directory
├── Dockerfile
├── docker-compose.yml
└── kimmy.example.toml
```

---

## Conventions used in these documents

- ✅ Working · 🚧 Partial · 📋 Planned · ⛔ Not implemented
- Code references are given as `crate/src/file.rs` so they survive line drift.
- Where a design has a *sharp edge*, it is called out rather than smoothed over.
  Sharp edges that are only discovered in production are the expensive kind.
