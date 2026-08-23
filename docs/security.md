# Security

[← Documentation index](README.md)

Authentication, authorization, and an honest account of what is and is not
defended against. Implemented in `kimmy-auth`.

---

## Model

```mermaid
graph LR
    C["Client"] -->|user + password| L["POST /v1/auth/login"]
    L -->|Argon2id verify| U[("__kimmy.__users")]
    L -->|"JWT signed with<br/>cluster-wide secret"| C
    C -->|"Authorization: Bearer"| R["Any route"]
    R --> V["verify signature + expiry"]
    V --> P["Principal { user, grants }"]
    P --> D{"Principal::can(action, db, collection)"}
    D -->|yes| H["handler"]
    D -->|no| F["403 forbidden"]

    style D fill:#2d3748,color:#fff
```

**One authorization decision point.** `Principal::can()` in
`kimmy-auth/src/rbac.rs`. Both the HTTP API and the MCP server call it.
A second enforcement path is exactly how an MCP tool ends up quietly more
permissive than the REST route beside it.

**Authorization is an extractor, not middleware.** A route that needs a
principal takes an `Auth` parameter; a route that does not is visibly public.
Middleware makes "which routes are protected?" a question you answer by reading
a registration list.

---

## Two ways in, one decision

A node can accept **local** tokens it issued itself, **federated** tokens from
an external OpenID Connect provider, or both at once. The difference stops at
the extractor: everything after it — `Principal::can`, RBAC, MCP, the audit
log, the search-without-read grant — treats the two identically.

```mermaid
graph LR
    T["Authorization: Bearer"] --> I{"unverified<br/>iss claim"}
    I -->|"matches auth.oidc.issuer"| O["OidcVerifier<br/>RS256/ES256 vs JWKS<br/>+ issuer + audience"]
    I -->|"anything else"| L["TokenIssuer<br/>HS256 vs cluster secret"]
    L --> S["session check<br/>(token version)"]
    O --> P["Principal { user, grants, federated }"]
    S --> P
    P --> D{"Principal::can"}

    style D fill:#2d3748,color:#fff
    style I fill:#2d3748,color:#fff
```

**Routing on an unverified claim is safe because of what it decides:** which
verifier gets to say yes, never whether the answer is yes. Each verifier pins
its own algorithm list and its own key, so a forged `iss` sends a token to a
verifier that refuses it, and an omitted one sends it to the local verifier,
which refuses it just as firmly.

**A token is offered to exactly one verifier.** That is what leaves no
algorithm-confusion surface: the classic attack needs one verifier that reads
the algorithm out of the header and picks a key to match, and neither of these
does. An HS256 token claiming the external issuer is refused; an RS256 header on
the local path is refused. Both have tests ([ADR-064](decisions.md)).

### Configuring it

```toml
[auth.oidc]
issuer = "https://auth.example.com"   # https only; discovery starts here
audience = "kimmydb"                  # required — see below
roles_claim = "roles"                 # "groups" for Entra ID

[[auth.oidc.role_mappings]]
claim_value = "kimmydb-analyst"
grants = [{ db = "sales", collection = "orders*", actions = ["read", "search"] }]
```

Also settable as `KIMMY_OIDC_ISSUER`, `KIMMY_OIDC_AUDIENCE`,
`KIMMY_OIDC_ROLES_CLAIM`. The mappings are file-only: a grant is a structure,
and a command line is where structures go to be mistyped.

Refused at startup, each because of what it would otherwise break:

| Refusal | What it prevents |
|---|---|
| An issuer with no audience | A provider signs for every application that trusts it; without an audience a token minted for the company wiki authenticates here |
| An audience with no issuer | Nothing to match `iss` against and nowhere to fetch keys — every federated token would be refused |
| A non-`https` issuer | Discovery and the JWKS are fetched from it; over plaintext anyone on the path substitutes their own signing keys |
| A mapping naming `admin` | See below |
| A mapping naming an unknown action | Caught while the file is parsed — the error names the bad value and lists the valid ones |
| `auth.oidc` together with `--insecure-no-auth` | Every request is already a superuser, so the mappings would enforce nothing while appearing to |

### `admin` is not federatable

**A role mapping that grants the `admin` action stops the node at startup.**
Administration — creating and dropping collections, managing indexes, managing
users, taking a backup — is reachable only through a local account.

This is a break-glass boundary. Federation makes an external system a dependency
of authentication; a compromised provider that can mint a reader is bad, and one
that can mint a superuser over the database is unrecoverable from inside it.
Keeping `admin` local means the answer to "the IdP has been taken over" is still
"log in as root and turn federation off" ([ADR-067](decisions.md)).

Every other action — `read`, `write`, `watch`, `search`, `webhook` — maps freely.

### A role that maps to nothing

...is a principal with **zero grants**, not a refusal. It authenticated; it is
simply not authorized here, and `Principal::can` already answers `false` to
every question such a principal asks. Refusing at the door would report "your
login is broken" for what is really "your administrator has not given you
access to this database" ([ADR-066](decisions.md)).

### Signing keys, and what happens when they rotate

The provider's JWKS is fetched through
`{issuer}/.well-known/openid-configuration` every five minutes and swapped in
whole. A token naming a `kid` the node has not seen triggers **one**
rate-limited refetch — the recovery for a rotation between two ticks — and the
rate limit is not politeness: a `kid` is attacker-controlled, so an unlimited
one would be a way to make this node hammer its own identity provider.

**Boot does not wait for it.** A briefly unreachable provider must not stop a
database from restarting — during an incident, both are being restarted — so the
fetch retries in the background and local users keep working throughout. Until
it lands, federated tokens get a 401.

That trade has a failure mode worth watching: a node that has *stopped* being
able to reach its provider keeps verifying perfectly against the keys it already
holds, until the provider rotates and every federated caller is refused at once.
Nothing about a working request reveals it. Two things do:

```bash
kimmyd check-config          # does a live discovery + JWKS fetch, and fails if it cannot
curl -s localhost:7878/metrics | grep jwks_refresh
# kimmy_jwks_refresh_total{outcome="ok"} 288
# kimmy_jwks_refresh_total{outcome="failed"} 0
```

### Revoking a federated session

Token-version revocation is for local users. A federated identity has **no
record in `__users`**, and the absence of a record is exactly how that check
refuses a deleted account — so the check is skipped for federated principals
outright. Without the skip every federated request would be refused; worse, a
local user who happened to share the asserted name would silently decide whether
the federated caller could connect.

So the session ends where it began: at the provider, and at the moment the
current token expires. **Keep federated token lifetimes short.**
`/v1/auth/refresh` refuses a federated principal rather than issuing a
replacement — minting a local token from a federated identity would shed the
origin flag and outlive the provider's say in it ([ADR-065](decisions.md)).

### Telling them apart

The principal carries `federated: true`, and the audit record carries it beside
`unauthenticated`:

```
user=ada@example.com unauthenticated=false federated=true action=Read db=sales collection=orders decision=allow
```

A name cannot do this job: nothing stops a provider from asserting a subject
called `root`. `/v1/auth/whoami` reports the same flag.

### Getting a token

```bash
export KIMMY_OIDC_ISSUER=https://auth.example.com
export KIMMY_OIDC_CLIENT_ID=kimmy-cli

export KIMMY_TOKEN=$(kimmy login --oidc)                # RFC 8628 device flow
export KIMMY_TOKEN=$(kimmy login --client-credentials)  # a service; secret from
                                                        # KIMMY_OIDC_CLIENT_SECRET
```

The device flow rather than a redirect, for the reason `gh auth login` uses it:
a redirect needs a browser and a loopback listener on the same machine, and a
database CLI is run over SSH and inside containers. The code and URL go to
**stderr** so the bare token on stdout stays capturable.

**Nothing is stored on disk** — not the token, not a refresh token. An
environment variable answers for its permissions, its lifetime and its cleanup
by not existing afterwards. Applications get a fresh token the same way, through
the Rust client's `token_provider` callback ([Clients](clients.md)).

---

## Passwords

Argon2id via the `argon2` crate, with a fresh random salt per password, stored
as a PHC string:

```
$argon2id$v=19$m=19456,t=2,p=1$<salt>$<digest>
```

The PHC format carries the algorithm and parameters alongside the digest, so
work factors can be raised later without stranding existing hashes.

- A **malformed stored hash verifies as `false`** rather than erroring, so a
  corrupt record is indistinguishable from a wrong password.
- The plaintext never appears in the hash, and hashes are never returned by any
  endpoint.
- Minimum 8 characters, enforced in the handler so every path that creates a
  user is held to it.

> `argon2` is pinned to stable **0.5**, not the 0.6 release candidate. Password
> hashing is not the place to run ahead of a stable release.

---

## Tokens

Local tokens: HS256 JWTs signed with a **cluster-wide** secret. (Federated
tokens are the provider's, signed RS256/ES256 — see
[Two ways in](#two-ways-in-one-decision). Everything in this section is about
the local half.)

```rust
struct Claims {
    sub: String,        // user name
    exp: u64, iat: u64, // seconds since the epoch
    grants: Vec<Grant>, // embedded, not looked up per request
}
```

**Why cluster-wide.** In a leaderless cluster a request may land on any node,
not the one that logged the user in. A per-node key would produce intermittent
401s that only appear under load balancing. `KIMMY_JWT_SECRET` must be identical
on every node — startup refuses to enable clustering without it.

**Why grants are embedded.** Signature verification stays a pure function of
the token — no store lookup to check a signature, and no cross-node consistency
requirement for authorization.

### Revoking a token

A signature proves a token was issued by this cluster and has not expired. It
cannot prove the account still exists. So the user record carries a
**token version**, the token carries the value it was issued under, and a
request is refused when they disagree ([ADR-052](decisions.md)).

Four things end a session, and none of them needs you to rotate the cluster
secret:

| | |
|---|---|
| Deleting the user | No record, no version — the absence is the revocation |
| Disabling the user | `disabled` is now checked on every request, not only at login |
| Changing the password | Bumps the version, so sessions the old password opened end |
| Changing grants | Bumps the version, so a **narrowed** permission takes effect at once |

That last row is the one that mattered: grants ride inside the token, so before
this a permission you took away kept working for the rest of the token's hour.

```bash
# End every session this user holds, on every node.
curl -XPOST localhost:7878/v1/users/ada/password -H "$A" -d '{"password":"..."}'
```

**It is a per-user switch, not a per-session one.** Revoking ends *all* of that
user's tokens; there is no way to kill one session and leave another. That is
deliberate — a deny-list of individual tokens fails open when an entry has not
reached the node handling the request, whereas a version mismatch fails closed.

**None of it applies to a federated identity**, which has no record here to
carry a version. See [Revoking a federated
session](#revoking-a-federated-session).

**Cluster-wide, at replication speed.** The edit is an ordinary write, so it
replicates like any other and each node drops its cached view when it arrives.
Measured on two nodes: the node taking the change refuses immediately, the
other within about two seconds. Refusals are **not** distinguished — deleted,
disabled and logged-out all return the same 401, because telling them apart
reports on an account to whoever is holding a stale token for it.

Rotating `KIMMY_JWT_SECRET` still works and is still the bigger hammer: it
invalidates every token for every user at once.

Minimum secret length is 16 bytes, enforced at construction — the whole cluster
shares this value, so a weak one is a cluster-wide weakness.

Attacks covered by tests: `alg=none` unsigned tokens, payload tampering to
escalate grants, wrong-secret signatures, expired tokens, and malformed input.

---

## RBAC

A grant is a set of actions over a set of collections:

```json
{ "db": "sales", "collection": "orders*", "actions": ["read", "watch"] }
```

| Action | Covers |
|---|---|
| `read` | Get, find, count, list |
| `write` | Insert, replace, update, delete |
| `watch` | Open a change stream |
| `search` | Vector and hybrid search. Implied by `read` but grantable alone, so an agent can search without reading raw documents |
| `webhook` | Register an endpoint the node pushes change events to |
| `admin` | Create/drop collections, manage users |

### Implication

```mermaid
graph BT
    A["admin"] --> W["write"]
    A --> WA["watch"]
    A --> R["read"]
    A --> S["search"]
    W --> R
    R --> S

    style A fill:#2d3748,color:#fff
```

- **`write` implies `read`** — an update must read the document it modifies.
  Requiring both separately would make every writer role wrong by default.
- **`read` implies `search`** — vector search is a read.
- **`admin` implies everything.**
- **`watch` is independent.** A subscriber sees every change to a collection
  continuously, which is a materially different exposure from point reads, so it
  must be granted explicitly. `read` does **not** imply `watch`.
- **`webhook` is independent too, and `watch` does not imply it.** They carry
  the same events, but a change stream ends when the client disconnects and dies
  with the token that opened it, while a webhook keeps sending to an address the
  grant never named long after that token expires. Handing out an egress path is
  a different act from being allowed to read, so it is granted separately. Only
  `admin` implies it.

### Patterns

`collection` supports a single trailing `*`, or `*` alone. `db` likewise.

```json
{ "db": "sales", "collection": "orders*" }   // orders, orders_2024, orders_eu
{ "db": "*", "collection": "*", "actions": ["admin"] }   // superuser
```

Deliberately not a full glob. `orders*` and `*` cover the real cases, and richer
syntax invites patterns whose blast radius is hard to eyeball during an audit.

### Database-wide operations

An operation spanning a whole database (`collection: None` internally) is only
satisfied by a grant covering the whole database:

```json
// This does NOT authorize dropping the "sales" database
{ "db": "sales", "collection": "orders", "actions": ["admin"] }
```

### Managing users requires `admin` over `*`

Otherwise a database-scoped administrator could mint a principal with wider
reach than their own.

---

## Properties enforced, and why

| Property | Rationale |
|---|---|
| Login returns identical responses for wrong password and unknown user | Otherwise login is a user-enumeration oracle |
| The unknown-user path still hashes a dummy | Otherwise it is a *timing* oracle |
| 403 is checked before the collection is resolved | A 404 would let a caller probe for collections they cannot access |
| Listing filters through the same check as access | Enumeration must not leak what access denies |
| Storage errors return a generic message | Their text can name on-disk paths and internals |
| Password hashes never leave the crate | — |
| Metrics expose counts, not names | Naming collections leaks the schema to an unauthenticated endpoint |
| The last user cannot be deleted | Otherwise the server becomes unadministrable |
| Backup requires `admin` over `*` | It is every document on the node; a database-scoped admin must not read past their own grants |

Each of these has a test that would fail if the property were lost.

---

## Bootstrap

On first start the server creates a superuser from `KIMMY_ROOT_USER` /
`KIMMY_ROOT_PASSWORD`.

**Only when the user store is empty.** Restarting with a different
`KIMMY_ROOT_PASSWORD` does **not** reset the account — otherwise a stale
environment variable becomes a privilege grant, and anyone who can influence the
environment can take over an existing database.

Change the root password through the API, not by editing the environment.

---

## `--insecure-no-auth`

Disables authentication entirely; every request runs as a superuser.

**Refused on any non-loopback bind address.** The server will not start with
`--insecure-no-auth` and `--bind 0.0.0.0:7878`. This is a startup error, not a
runtime surprise.

The resulting principal is flagged `unauthenticated: true` and named
`insecure-no-auth`, so audit output can distinguish "root did this" from "auth
was off" — and `/v1/auth/whoami` reports `"authenticated": false`.

---

## What is NOT defended against

Stated plainly, because a security model you have to infer is worse than none.

| Gap | Status | Mitigation today |
|---|---|---|
| **Client TLS** | ✅ Built | Native termination — see [TLS](#tls). A reverse proxy still works if you prefer it |
| **Node↔node TLS** | ✅ Built | Bound to `cluster_secret` via channel binding — see below |
| **No client certificates** | Not planned | The server proves itself to clients; clients authenticate with a bearer token |
| **Per-session revocation** | Not planned | Revocation is per user: all of that user's tokens, or none. See above |
| **Enterprise SSO** | ✅ OIDC | One external issuer, RS256/ES256, inline role mappings — see [Two ways in](#two-ways-in-one-decision). SAML and LDAP are not planned |
| **Revoking a federated session from here** | Not possible | There is no local record to revoke. Revoke at the provider and keep token lifetimes short |
| **Federated `admin`** | By design | `admin` is local-only, so a compromised identity provider cannot mint a superuser ([ADR-067](decisions.md)) |
| **Rate limiting covers login only** | ✅ login · 📋 the rest | See [Login rate limiting](#login-rate-limiting). Every other route is unbounded; limit at a proxy if you need it |
| **Audit log** | ✅ Built | Authorization decisions at the `kimmy::audit` target; `audit.mode` selects how much. See [Operations](operations.md#the-audit-log) |
| **No field-level security** | Not planned | Collection is the finest granularity |
| **No encryption at rest** | Not planned | Use an encrypted volume |
| **No inter-node auth yet** | 📋 M4 | `cluster_secret` is validated at config time but nothing transports data yet |
| **Grants are not validated against reality** | By design | A grant may name a database that does not exist |

### Denial of service

Partially addressed. Regex is linear-time by construction (the `regex` crate),
so patterns cannot be pathological. `find` caps at 10,000 documents. Login is
rate-limited, which also closes an amplification vector — see below. But there
is no limit on the authenticated routes, no request size limit beyond axum's
default, and no query timeout — a collection scan over a large collection will
run to completion.

---

## TLS

The HTTP, WebSocket and MCP listener terminates TLS itself. Point it at a
certificate and a key:

```toml
[server.tls]
cert_file = "/etc/kimmy/tls/server.crt"   # PEM chain, leaf first
key_file  = "/etc/kimmy/tls/server.key"   # PKCS#8, PKCS#1 or SEC1
```

or `--tls-cert` / `--tls-key`, or `KIMMY_TLS_CERT` / `KIMMY_TLS_KEY`.

**There is no on/off switch.** TLS is on when both are set. Setting exactly one
is refused at startup, because the alternative is serving plaintext on a port an
operator believes is encrypted. So is a path that does not exist, or a file that
is not a usable certificate — all three stop the node with a message naming the
file, rather than becoming a handshake failure for whoever connects first.

One listener, one port. There is no plaintext half and no HTTP→HTTPS redirect:
a port is either encrypted or it is not.

**In a container, the key must be readable by uid 10001.** The image runs as a
non-root user, so a key at mode `0600` owned by you stops the node at startup
with `Permission denied` naming the file. That is the right failure — but it is
the first thing to check when a TLS container will not start.

**Plaintext on a public bind warns but still starts.** Terminating at a proxy or
a service mesh is a legitimate deployment and refusing to start would break it.
But nothing about a successful request reveals that the token authorising it
crossed the wire in the clear, so the node says so once at startup:

```
WARN serving plaintext HTTP on a non-loopback address; tokens and passwords
     cross the wire unencrypted
```

Loopback binds do not warn.

### Renewing a certificate without a restart

A renewed certificate takes effect on a running node. Two triggers, one
reload ([ADR-049](decisions.md)):

```bash
kill -HUP $(pidof kimmyd)      # or: systemctl reload kimmyd
                               # or: docker kill -s HUP <container>
```

...and, with no signal at all, the node re-reads the pair **within 60 seconds**
of either file changing. That is the trigger that matters where a certificate
is rotated *by* something rather than by someone — cert-manager rewriting a
mounted Secret, where there is no convenient way to signal PID 1 of a pod.

The swap is free: the running listener holds the certificate behind a handle it
reads per handshake, so nothing rebinds and nothing is dropped. Connections
already in flight finish under the certificate they negotiated with; the next
handshake gets the new one.

**A bad new certificate cannot take the node down.** This is the opposite of
the startup rule above, and deliberately so: at startup there is nothing to
fall back to, and on a serving node there is. The pair is parsed *before* it is
adopted, so anything unparseable — a truncated file, a certificate whose key
has not landed yet — leaves the certificate already in use serving:

```
WARN could not reload the TLS certificate; keeping the one currently in use
     error=keys may not be consistent: KeyMismatch
```

That is also what absorbs the window between writing a new certificate and
writing its key: the mismatch is refused, and the next trigger — a second
SIGHUP, or the next poll — completes the rotation once both halves are on disk.

**Watch `kimmy_tls_reloads_total{outcome="failed"}`.** A reload that fails is
silent in every other way: the node keeps serving the old certificate
perfectly, right up until it expires and every client drops at once.

```
kimmy_tls_reloads_total{outcome="ok"} 2
kimmy_tls_reloads_total{outcome="failed"} 1
```

Note what this does *not* catch — a certificate nobody ever tried to rotate
reports nothing, because no reload was attempted. An expiry gauge would cover
that and is recorded as the follow-up in [Deviations](deviations.md).

### What this does and does not cover

| | |
|---|---|
| Clients → this node | ✅ Encrypted, and the node proves its identity |
| Change streams (WebSocket) | ✅ `wss://`, verified against a running node |
| MCP at `/mcp` | ✅ Same listener, same certificate |
| **Node → node replication** | ✅ TLS 1.3 always, with the handshake bound to the session — see [below](#tls-between-nodes). *(This row said "still plaintext" until it was checked against the code; it had been wrong since ADR-040.)* |
| **Node → node membership (SWIM)** | ⚠️ **Authenticated, not encrypted.** Every datagram carries an HMAC over `cluster_secret`, so an unauthenticated node cannot join the member set ([ADR-053](decisions.md)) — but the payload is readable, so membership topology is visible to anyone on the path |
| **Client certificates (mTLS)** | ❌ Not planned. Clients authenticate with a bearer token |
| **Certificate reload** | ✅ SIGHUP, or within 60s of the file changing. A bad new certificate is refused and the old one keeps serving |

### Notes

TLS 1.3 and HTTP/2 are negotiated where the client supports them. WebSocket
still works when a client offers `h2` in ALPN — axum's upgrade is HTTP/1.1-only,
but hyper serves HTTP/1.1 on any connection that does not open with the HTTP/2
preface. Checked against a real node rather than assumed.

`rustls` on the `ring` provider, which was already in the build. See
[ADR-039](decisions.md) for why not the `aws-lc-rs` default.

---

## TLS between nodes

Replication runs over TLS, always. There is no setting: `cluster_secret` must
already match cluster-wide, and a second setting that must also match is another
way to misconfigure a cluster — one whose failure mode is silent plaintext.

**Certificates are generated per node at startup and are not verified.** That
sounds alarming and would be, on its own: unverified TLS stops a passive
eavesdropper, but an active attacker terminates two sessions and relays between
them, reading everything.

What stops that is **channel binding**. The mutual HMAC handshake, which already
proved both sides hold `cluster_secret` without transmitting it, now also signs
the TLS session's exported keying material:

```
proof = HMAC(cluster_secret, len(nonce) || nonce || len(exporter) || exporter)
```

A man-in-the-middle holds two TLS sessions whose exporters differ, so the proof
it relays is computed over the wrong value — and it cannot recompute one without
the secret. The connection dies.

This is tested with a relay that genuinely terminates TLS on both sides and can
read the frames: removing the binding makes that test fail while a control with
nobody in the middle still passes.

| | |
|---|---|
| Confidentiality | ✅ TLS 1.3 |
| Man-in-the-middle | ✅ via channel binding |
| Node identity | ❌ beyond "holds `cluster_secret`" — which is what the secret always meant |
| PKI required | ❌ none |

**Upgrade note.** A node speaking TLS cannot talk to one speaking plaintext, so
a cluster cannot be upgraded to this version one node at a time. See
[Operations](operations.md).

---

## Login rate limiting

`/v1/auth/login` is limited by a token bucket per client address. Over the
limit, it answers `429` with a `Retry-After` in seconds.

**Why this route and not the others.** Everywhere else a limit would be a
capacity control, and a capacity number picked without measurement is a guess.
Here it is a security control, for two reasons:

- the route is unauthenticated by necessity, so nothing else stands between a
  guesser and the password;
- every attempt runs a full Argon2id verification **including for a user that
  does not exist** — that is deliberate, and is what stops timing from revealing
  whether an account exists (`UserStore::authenticate`). At the configured work
  factor it is ~19 MB and milliseconds of CPU that an anonymous caller can spend
  at will.

The second is why the limit is checked *before* authentication rather than
after. Checking afterwards would return the same `429` while still doing all the
work the limit exists to prevent.

**Only failures count.** A caller presenting correct credentials is not the
threat, and a fleet re-authenticating on a short `token_ttl_secs` must not be
throttled for succeeding. A successful login spends nothing.

### Settings

```toml
[server.rate_limit]
login_per_ip = 10              # failed logins per address per window; 0 disables
login_per_ip_window_secs = 60
login_per_user = 0             # per username, across all addresses; 0 disables
login_per_user_window_secs = 300
trusted_proxy_header = "X-Forwarded-For"   # omit to use the socket peer address
max_tracked_keys = 100000
```

### Three things worth knowing before you tune it

**A shared egress shares a budget.** Keying on the peer address means callers
behind one NAT draw on one bucket, and an address over its budget is refused
even with correct credentials. On a shared egress, raise `login_per_ip` or set
`trusted_proxy_header`.

**`trusted_proxy_header` is off by default, and must stay off unless a proxy you
control rewrites it.** A forwarded header is client-supplied data. Trusting one
that nothing rewrites lets any caller mint a fresh budget per request by varying
a header — worse than no limiter, because it would look like one was working.
When it is set, the **last** value is used: a proxy appends the peer it saw, so
the rightmost entry is the only one the client did not choose.

**`login_per_user` defaults to off, and enabling it is a trade.** It is the only
defence against a guess spread across many addresses, which per-address limiting
cannot see. It also lets anyone who can reach the endpoint spend a *named* user's
budget and keep the legitimate holder out for the window. Which risk matters more
depends on your deployment, so it is not something a default should assume.

With `--insecure-no-auth` the limiter is off entirely: there is no login to
protect and every request is already a superuser.

---

## Deployment checklist

```mermaid
graph TB
    A["Generate a strong KIMMY_JWT_SECRET<br/>openssl rand -base64 32"] --> B["Same secret on every node"]
    B --> C["Set KIMMY_ROOT_PASSWORD via secret manager,<br/>not a config file"]
    C --> D["Set server.tls.cert_file and key_file<br/>(or terminate at a proxy)"]
    D --> E["Behind a proxy? set trusted_proxy_header<br/>so the login limiter sees real clients"]
    E --> F["Create scoped users; do not use root for applications"]
    F --> G["Never expose --insecure-no-auth beyond loopback"]
```

---

## Next

- [HTTP API](http-api.md) — the endpoints these rules protect
- [Operations](operations.md) — configuration and deployment
- [Decisions](decisions.md) — why JWT rather than sessions
