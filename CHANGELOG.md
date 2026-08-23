# Changelog

Notable changes, for people upgrading. Hand-written, deliberately: the commit
log records how the work happened, this records what an operator or client
author needs to know, and the two are different documents (ADR-063 covers the
mechanics — the release workflow lifts the matching section below into the
GitHub Release notes when a `v*` tag is pushed).

Versioning follows the pre-1.0 policy in
[docs/compatibility.md](docs/compatibility.md): a `0.MINOR` bump may carry
breaking changes and says so here; a `0.x.PATCH` bump never does.

## Unreleased

The first tagged release. Everything below already works and is exercised by
tests and by driving real nodes — see the status table in
[docs/README.md](docs/README.md).

### Added

- JSON document storage on redb: multi-database, Mongo-style queries
  (17 filter operators), update operators, sort, projection, cursor paging,
  and an aggregation pipeline with a hard memory ceiling.
- Change streams over WebSocket on a single node — resumable by token, no
  replica set required.
- Leaderless clustering: SWIM membership over UDP, oplog anti-entropy over
  TCP, DNS/Kubernetes discovery, snapshot resync.
- Secondary indexes: compound, descending, multikey, unique (single-node),
  TTL and partial.
- Vector search and automatic embeddings: per-collection providers, HNSW
  above 500 vectors, hybrid search fused by reciprocal rank fusion.
- An MCP server inside the database at `/mcp`, sharing authorization with
  REST.
- Authentication and RBAC: Argon2id, JWT with sliding refresh, per-collection
  grants, login rate limiting, audit log.
- TLS termination for HTTP/WebSocket/MCP, with hot certificate reload.
- Webhooks with signed deliveries and cluster failover.
- Online backup, offline restore, and point-in-time rewind.
- The `kimmy` CLI, first-party Rust/Python/Go clients, and a conformance
  suite that drives all three against a real server.
- Build identity baked into every artifact: `kimmyd` logs version and commit
  at startup, `kimmy --version` and `GET /v1/version` report the same values,
  and a tarball build without `.git` still compiles (commit `unknown`).
- Release engineering: tag-driven releases via cargo-dist — static musl
  Linux binaries (x86_64, arm64), macOS binaries (x86_64, arm64), SHA256
  checksums, a Homebrew formula for `kimmy`, and a multi-arch container
  image at `ghcr.io/titusai-io/kimmydb`.
