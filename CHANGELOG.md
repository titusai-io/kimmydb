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

### Added

- **Every command uses the federated token cache.** After `kimmy login`,
  `kimmy whoami`, `databases`, queries — all of it — just works until the
  token nears expiry: commands with no explicit token fall back to the cache
  login and `kimmy token` write, keyed by issuer, client and resource.
- **The device flow offers to open the browser for you.** Print URL, press
  Enter, default browser opens on the verification page (code pre-filled via
  `verification_uri_complete` when the provider sends one). Skipped entirely
  when stdin or stdout is not a terminal.

### Changed

- **`kimmy login` caches its token by design** (ADR-080): the opt-in from
  ADR-075 lasted exactly as long as nobody read the cache — which was until
  data commands started doing so today, and the documented workflow failed
  with a misleading 401. `--cache-token` is removed along with the opt-in.
- The post-401 hint says `run \`kimmy login\`` first now, instead of pointing
  at flags when the common case has none to set.

## 0.8.0 - 2026-08-26

### Added

- **`kimmy init` discovers instead of interrogating.** It now asks for one
  thing — the node URL — and reads the RFC 8707 resource identifier and the
  issuer from the node itself, the same document `kimmy login` already
  consumes. The client id shows its registered default (`kimmy-cli`) and Enter
  keeps it; explicit prompts return only for nodes too old to publish
  metadata. Re-runs carry existing secret keys forward rather than dropping
  them, secrets are never read interactively, and the output says what was
  written and what was deliberately left to flags and environment.

### Changed

- **Re-running `kimmy init` no longer panics — and neither does anything
  else.** With a settings file present, every subcommand without OIDC flags
  (`databases`, `ping`, `whoami`, …) aborted with `` "issuer" is not an id of
  an argument`` before doing any work: the dotfile loader asked clap for value
  sources that exist only on `login`/`token`. Those commands now skip the
  provider settings entirely.

## 0.7.0 - 2026-08-26

### Added

- **`kimmy init` and a settings file: `~/.config/kimmydb/.kimmy`.** Init
  prompts once per setting — the current value shown as the default, Enter
  keeps it — then writes the file `0600`; running it again re-prompts and
  overwrites. The file is dotenv-style and covers url, token, password,
  issuer, client_id, client_secret, resource, scope, cache_token. Precedence
  per setting is **flag > environment variable > file**, so it fills gaps and
  never overrides something already said. Unknown keys and broken lines are
  errors naming the line.
- **`kimmy roles …` and `kimmy users …`.** The administrative surface the
  federation round made necessary: stored roles (ADR-073) — the things OIDC
  role mappings point at — can now be created, inspected, granted against and
  revoked from the CLI (`roles grant/revoke` are live edits; they apply on the
  callers' next request), and local accounts can be created, granted,
  disabled and deleted without curl. Disable is new server-side too:
  `POST /v1/users/{name}/disabled` ends every session the account holds and
  refuses new logins while keeping the record — the reversible form of
  deletion, guarded like deletion is (not your own account, not the last
  enabled user).

### Changed

- **Bare `kimmy` shows the help screen** — the same long help `--help`
  prints, on stdout, exit 0. Previously it was a two-line usage error telling
  the user to run again with a flag.
- **The system database never matches a wildcard (ADR-079).** A grant of
  `{db:"*"}` no longer reaches `__kimmy` — whose `__users` collection holds
  password hashes and token versions. Wildcard-granted callers stop seeing it
  in listings and are refused on direct access; two doors remain, holding the
  `admin` action anywhere (root and every admin deployment are untouched) or a
  grant naming `__kimmy` exactly, down to its collection pattern. Anyone who
  was deliberately reading system collections through a wildcard must write
  that exact grant now.

## 0.6.0 - 2026-08-25

### Added

- **`kimmy whoami`.** How the node sees the caller: principal name, local or
  federated, and the grants that identity holds — the one-command answer to
  "I logged in fine, why does everything refuse?"
- **A zero-grant note on empty listings.** `kimmy databases` and
  `kimmy collections` filter through authorization server-side, so an identity
  whose token carries no grants sees what an empty cluster looks like. When a
  listing comes back empty and `/v1/auth/whoami` confirms the caller holds no
  grants at all, the CLI says so on stderr; stdout is unchanged.
- **`KIMMY_OIDC_ROLE_MAPPINGS`: role mappings through an environment
  variable.** A container deployment that configures the node with env vars
  could federate but had no way to say what a federated identity was worth —
  `role_mappings` was TOML-only, so such nodes ran with zero mappings and
  every federated caller held zero grants (empty listings, bare 403s). The
  variable takes one JSON array of mapping objects and **replaces** the file's
  list when set; every startup refusal applies unchanged (ADR-078).
- **`kimmy token`.** Prints the caller's access token again: the cached one
  while it stays fresh, otherwise one federated flow whose result is kept, so
  `$(kimmy token)` costs nothing after the first call until the token nears
  expiry. Caching is implied — invoking the command *is* the asking ADR-075
  requires — and what is stored does not change: access token only, `0600`,
  never a refresh token. Federated flows only; `kimmy login <user>` remains
  how local accounts print a token.

### Changed

- **`kimmy login` federates by default.** With no arguments it runs the device
  flow against the node's identity provider; naming a user (`kimmy login ada`)
  logs into that local account as before. Previously bare `kimmy login`
  refused with "name the user to log in as", so nothing scripted depended on
  the old behaviour — but the flag spellings all still work, including
  `--oidc`, which now only spells the default out.

## 0.5.0 - 2026-08-25

### Added

- **Embedding work is owned per collection.** Backfill scans and deferred
  re-checks run on the node the collection's rendezvous hash assigns — the
  same function webhooks and TTL expiry use — instead of on every member.
  Closes the 3x provider-call amplification measured under replication lag
  (ADR-077).
- **The embedding worker can be turned off per node**: `[vector]
  worker_enabled = false` or the one-way `--disable-vector-worker` flag /
  `KIMMY_DISABLE_VECTOR_WORKER`. A disabled node consumes replicated vectors;
  vector search is unaffected.
- **Embedding observability**: `kimmy_embed_documents_total`,
  `kimmy_embed_chunks_total`, `kimmy_embed_deferred_total`,
  `kimmy_embed_skipped_not_owned_total` and `kimmy_embed_failures_total` on
  `/metrics`.

### Changed

- `/metrics` gained five series; scrapers asserting an exact series set need
  the additions.

## 0.4.0 - 2026-08-24

### Added

- **Roles are first-class stored objects.** A role is one named set of grants
  that several principals point at, managed at `/v1/roles` and assigned with
  `POST /v1/users/{name}/roles`. Both halves of the system can now reach the
  same definition: a local user names a role on its record, and an
  `[[auth.oidc.role_mappings]]` entry can name one with `role = "analyst"`
  instead of repeating its grants inline. Requires `admin` over `*`, the same
  bar as managing users.

  **Role grants are added to a principal's direct grants, never a replacement** —
  effective permission is the union of the two, so a user holding no roles is
  completely unaffected. No storage migration and no on-disk schema bump: roles
  live in an ordinary system collection created on demand, and user records
  written before this decode as holding no roles. See ADR-073.

- **`auth.oidc.allow_federated_admin`**, default `false`. With it off — the
  behaviour that shipped in 0.2.0 — a federated principal can never hold
  `admin`. It exists because that absolute refusal makes a large deployment
  impossible rather than awkward: auditors flag the privileged local accounts
  outside the IdP that the rule requires. Turning it on is announced in the
  startup summary every time. See ADR-074.

### Changed

- **Editing or deleting a role revokes the live tokens of every local user
  holding it**, and the response reports how many accounts that was. Without it
  a *narrowing* edit would take effect only as each token expired. Federated
  principals need no such revocation and are not counted: their grants resolve
  from the role store on every request, so an edit reaches them on their next
  call. Their role *membership* is a different matter — it is frozen in the
  provider's access token until that token expires.

- **The audit record carries the roles a principal held.** The roles held, not
  "the role that decided": grants are a union and more than one role can supply
  the same permission. It matters most for a federated caller, where there is no
  local record to recover the association from afterwards.

- **A role mapping naming neither `role` nor `grants` now stops the node at
  startup.** It could never have granted anything, so it was a typo — and the
  failure it produced instead was a caller who authenticated and was then
  authorized for nothing, with no indication that the configuration was at
  fault.

## 0.3.0 - 2026-08-23

### Added

- **KimmyDB names itself as an OAuth 2.0 protected resource.** When
  `auth.oidc.audience` is written as an `https` URL, a node publishes RFC 9728
  metadata at `/.well-known/oauth-protected-resource` naming that identifier
  and its authorization server. `kimmy login --oidc --url <node>` now needs
  nothing else configured — it reads both values off the node — and a
  conformant MCP client can discover where to authenticate the same way.
- **`kimmy login` sends an RFC 8707 `resource` parameter**, on the device flow
  and the client-credentials flow, via `--resource` / `KIMMY_OIDC_RESOURCE` or
  the node's own metadata. Without it the only audience the CLI could obtain
  was whatever the provider defaulted to, so an audience naming this node
  specifically was unreachable from the tool.
- **`WWW-Authenticate` on every 401 and 403** (RFC 6750 §3), pointing at the
  metadata document when there is one. A request that offered no credentials is
  told how to authenticate and deliberately carries no `error` code; a bad
  token gets `invalid_token`, and a denied request `insufficient_scope`.
  `POST /v1/auth/login` is exempt — it is where a token comes from, not a
  bearer-protected resource.
- `kimmyd check-config` and the startup log now say whether the node publishes
  protected resource metadata, and why not when it does not.
- **`auth.oidc.require_at_jwt`** (`KIMMY_OIDC_REQUIRE_AT_JWT`), default `false`:
  refuse a federated token whose `typ` header is not `at+jwt` (RFC 9068 §4).
  Off by default because providers disagree about stamping it — Entra ID sends
  `typ: JWT` on v2 access tokens — so a strict default would refuse every token
  from a supported provider. Turn it on when yours is known to emit it. If
  `auth.oidc.audience` is an `https` URL you are already covered without it: an
  ID token's audience is a client id and can never be a resource identifier.
- **`kimmy login --cache-token`** (or `KIMMY_TOKEN_CACHE`) reuses the access
  token from a previous login instead of authenticating again. **Off unless
  asked for**, so nothing changes for anyone who does not pass it. The token
  goes in a `0600` file under `$XDG_CACHE_HOME/kimmy` (or `~/.cache/kimmy`),
  keyed by issuer, client and resource, and is reused until it is within a
  minute of expiring. **A refresh token is never requested and never stored**,
  with or without the flag. See [ADR-075](docs/decisions.md).

### Changed

- **`auth.oidc.audience` written as `http://` is now refused at startup**, as
  is one carrying a fragment. Every other audience is accepted exactly as
  before, including opaque strings, Entra ID's `api://<guid>` and `urn:`
  values — those simply publish no metadata. No existing configuration that
  used `https` or an opaque string needs editing. See
  [ADR-071](docs/decisions.md).
- **`kimmy login --client-credentials` now authenticates with HTTP Basic** when
  the provider advertises `client_secret_basic`, falling back to the request
  body only when it advertises the body and not Basic. RFC 6749 §2.3.1 makes
  Basic mandatory for an authorization server and the body optional, so this is
  the method that is always available. The id and secret are form-encoded
  before the header is built, as §2.3.1 requires — which matters whenever a
  secret contains a `:`, a `+`, a space or a non-ASCII character.
- **`kimmy login --client-credentials` no longer requests any scope by
  default.** `--scope` previously defaulted to `openid profile` for both flows,
  but there is no end user in the client-credentials grant, so `openid` asks
  for an ID token that cannot be issued — some providers ignore it, others
  refuse the request. The device flow still defaults to `openid profile`, and
  an explicit `--scope` still wins for either. **If you relied on the old
  default for a service account, pass `--scope` explicitly.**
- **Documentation only, no behaviour change:** [Security](docs/security.md) now
  states where authorization stops and why. The collection is the finest unit
  of protection — no document- or field-level security, no ABAC, no embedded
  policy engine — and **named roles will not change that ceiling** when they
  arrive. It also explains that a trailing `*` on a grant's `db` matches a
  prefix, so `sales*` covers `salesforce` and any database created later with
  that prefix, and records why `/metrics` keeps its unauthenticated place on
  the main listener rather than gaining a second port. See
  [ADR-076](docs/decisions.md).

### Fixed

- **`/mcp` now answers a rejected request with `WWW-Authenticate`, and its
  traffic reaches `/metrics` and tracing.** It was merged onto the router
  *after* the layer that counts, times, traces and adds the challenge, and a
  router merged after a layer keeps its own empty middleware stack — so every
  MCP request skipped all four. The header is the half that matters: an MCP
  client holding no credentials has no other way to discover its authorization
  server, which is the case RFC 9728 exists to serve, so a bare 401 left it
  needing to be configured by hand. Nothing about MCP authorization itself
  changed — a request without a valid token was refused before and is refused
  now — and REST routes were never affected.

### Security

- **`kimmyd check-config` no longer prints `auth.jwt_secret` or
  `auth.root_password`.** It dumped the whole configuration, and those two
  fields with it, to a terminal — and to CI output, and to anything pasted into
  a bug report. `jwt_secret` signs every local token the cluster issues, so
  reading it is enough to mint any principal, `root` included. Both now
  serialize as `<redacted>`, and the documented workflow was the one that leaked:
  the config file keeps both commented out in favour of `KIMMY_JWT_SECRET` and
  `KIMMY_ROOT_PASSWORD`, exactly so the secret lives only in the environment.
  Whether each is *set* is still shown, which is the question `check-config`
  exists to answer. **Rotate `auth.jwt_secret` if its value has been through a
  shared log or a pasted report**; rotating it invalidates every local token in
  issue, which is the intended effect.
- **A federated token's `nbf` is now validated.** `jsonwebtoken` leaves that
  check off by default, so a token stamped as not valid until a future time was
  accepted before it was due (RFC 7519 §4.1.5). The same 60-second leeway `exp`
  gets applies, and a token carrying no `nbf` is unaffected — the claim stays
  optional.
- **A provider's discovery document must now name the issuer it was fetched
  for**, and the `jwks_uri` it names must be `https` (OpenID Connect Discovery
  §4.3, RFC 8414 §3.3). Checked in both places that read one: the node's key
  refresher and `kimmy login`. Without it, anything able to answer for the
  well-known path chose which signing keys a node trusts — or, on the CLI,
  where a client secret is sent. Plain `http` to a loopback address stays
  allowed, so a locally-run provider still works. **An operator whose provider
  publishes an issuer that differs from the configured one — a trailing slash
  is the usual case — will now see a startup failure naming both values.** See
  [ADR-072](docs/decisions.md).

## 0.2.0 - 2026-08-23

### Added

- **Distributed tracing and OTLP metrics.** A node can now export spans and
  counters to an OpenTelemetry collector. Configure it under `[telemetry]` (or
  `KIMMY_OTLP_ENDPOINT`); there is no enable flag, because setting an endpoint
  is what turns it on. Spans cover the HTTP request, the executor operation —
  so REST and MCP produce the same ones — the storage commit that is the fsync,
  anti-entropy rounds, the embedding worker, webhook deliveries and the OIDC
  key refresh. Inbound `traceparent` is honoured and outbound webhook
  deliveries carry one. See [docs/operations.md](docs/operations.md) and
  ADR-068 through ADR-070.
- `--otlp-endpoint`, `--otlp-protocol`, `--otlp-sample-ratio`,
  `--otlp-service-name` and `--telemetry-include-names`, each with a `KIMMY_*`
  environment variable.
- The process counters behind `/metrics` are also reported over OTLP, as
  observable instruments reading the same atomics — bridged, not duplicated, so
  the two surfaces cannot disagree (ADR-070). **`/metrics` itself is
  unchanged**, and is now pinned by a golden test over the whole render plus an
  ordered series-name assertion over the route's body.
- **Enterprise OIDC federation.** A node can now accept tokens from one
  external OpenID Connect provider alongside its own local users. Configure it
  under `[auth.oidc]` (or `KIMMY_OIDC_ISSUER` / `KIMMY_OIDC_AUDIENCE` /
  `KIMMY_OIDC_ROLES_CLAIM`), map claim values to grants with
  `[[auth.oidc.role_mappings]]`, and every route, MCP tool and audit record
  works for a federated caller exactly as it does for a local one.
  RS256/ES256 against the provider's JWKS, with issuer, audience and expiry
  validation and 60 seconds of clock-skew allowance. See
  [docs/security.md](docs/security.md) and ADR-064.
- `kimmy login --oidc` — RFC 8628 device authorization, the flow `gh auth
  login` uses. The code and URL go to stderr and the bare token to stdout, so
  `export KIMMY_TOKEN=$(kimmy login --oidc)` works as it always has. Nothing
  is written to disk, and no refresh token is ever requested or kept.
- `kimmy login --client-credentials` — for a service account. The client
  secret comes from `KIMMY_OIDC_CLIENT_SECRET`; there is deliberately no flag
  for it, for the same reason there is no `--password`.
- `Builder::token_provider` on the Rust client: an async callback that supplies
  a fresh token at connect time, before expiry, and once after a 401. This is
  how a long-lived application plugs in its own OIDC refresh — the client
  library deliberately does not implement OAuth2.
- `kimmy_jwks_refresh_total{outcome}` on `/metrics`. Worth an alert: a node
  that has stopped reaching its provider keeps verifying perfectly until the
  provider rotates its keys, and then refuses every federated caller at once.
- `kimmyd check-config` now performs a live discovery and JWKS fetch when
  `[auth.oidc]` is configured, and fails if the provider cannot be reached.

### Security

- **Telemetry omits names by default.** With `telemetry.include_names = false`
  — the default — a span is named for its route template
  (`/v1/db/{db}/coll/{coll}/docs`) or its operation (`find`, `insert`), neither
  of which is built from anything you stored. Turning it on adds
  `db.namespace`, `db.collection.name` and `url.path`, which publishes your
  schema to whatever holds the traces. **Only spans are exported, never log
  events**, and audit records never reach a collector at any setting. See
  [docs/security.md](docs/security.md) and ADR-068.
- **OTLP over HTTP only, never gRPC**, and `https://` collector endpoints are
  refused at startup rather than failing silently at every export. This keeps
  the build free of a second native dependency stack, so the musl and arm64
  cross-compiles are unchanged (ADR-069). A collector reachable only over gRPC
  or TLS needs an OpenTelemetry Collector in front of it.
- **`admin` cannot be granted through an IdP claim.** A role mapping naming the
  `admin` action stops the node at startup. Administration stays reachable only
  through a local account, so a misconfigured or compromised identity provider
  cannot mint a superuser over the database (ADR-067).
- Federation is refused in combination with `--insecure-no-auth`, where the
  role mappings would enforce nothing while appearing to.
- A non-`https` `auth.oidc.issuer` is refused: the signing keys are fetched
  from that URL, and over plaintext they can be substituted.

### Changed

- `GET /v1/auth/whoami` gained a `federated` boolean. A name cannot answer the
  question — an identity provider is free to assert a subject matching a local
  account — and a federated principal has no local record, so it cannot change
  a password or be revoked from here.
- Audit records carry `federated` alongside `unauthenticated`, so the three
  origins of a principal stay distinguishable in the log.
- The `kimmy` CLI's 401 hint is now issuer-aware: with `KIMMY_OIDC_ISSUER` set
  it points at `kimmy login --oidc` rather than at a local password login the
  caller may not have.
- `POST /v1/auth/refresh` refuses a federated principal with a 400 explaining
  that the identity provider renews that token. Previously it would have
  failed with a misleading "this token is no longer valid".
- The container image is tagged `latest` only for a final release. Previously
  a prerelease tag would have taken over `:latest` if prerelease publishing
  were ever enabled.

### Notes for operators

- **Federated sessions cannot be revoked from KimmyDB.** There is no local
  record to carry a token version, so the session ends when the provider's
  token expires. Keep federated token lifetimes short (ADR-065).
- Role mappings are configuration: changing one is an edit and a restart, and
  in a cluster a rolling one (ADR-066).
- Startup does **not** wait for the identity provider. A briefly unreachable
  provider must not stop a database from restarting, so the key fetch retries
  in the background and local users keep working meanwhile.

## 0.1.0 - 2026-08-23

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
