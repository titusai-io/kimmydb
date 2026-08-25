# `kimmy` — the terminal client

[← Documentation index](README.md)

One-shot commands that speak the ordinary HTTP API, print JSON on stdout, and
exit non-zero when they fail.

Install with `brew install titusai-io/tap/kimmy` on macOS, or take a prebuilt
tarball from the [releases page](https://github.com/titusai-io/kimmydb/releases)
— the Linux binaries are static, so they run on any distribution. `kimmy
--version` prints the version, commit and build date, which is what to include
in a bug report.

```bash
export KIMMY_URL=http://localhost:7878
export KIMMY_TOKEN=$(echo hunter2 | kimmy login root)

kimmy find shop.orders '{"qty":{"$gte":10}}' --sort '{"qty":-1}'
kimmy count shop.orders '{"city":"London"}'
echo '{"_id":4,"city":"Paris"}' | kimmy insert shop.orders
jq -s . docs/*.json | kimmy bulk-insert shop.orders
```

---

## Why one-shot rather than a shell

Each invocation does one thing and exits, so it composes with pipes, `jq`, shell
loops and CI. An interactive shell is nicer for exploring, but it is this same
command surface *plus* a terminal UI — so the commands come first, and a REPL,
if it is ever wanted, sits on top of them rather than beside them.

## It is a consumer of `kimmy-client`

Every request goes through the Rust client crate rather than through HTTP calls
written here. That is the point rather than an implementation detail: a client
library nobody uses is a library whose rough edges nobody finds, and converting
this tool is what found three of them — a public API that forced every consumer
to depend on `reqwest`, a login that could not fail over to a second node, and
the missing `create-collection` that made a fresh database unusable from here.

What the tool gets for free as a result: token refresh, failover between nodes,
cursor paging, and change streams that reconnect and resume.

## Why it speaks HTTP

Nothing here opens the database file. redb allows one process to hold a
database, so a file-opening client could not be used while a node was running —
which is most of the time anyone wants one — and it would bypass authentication
and RBAC entirely. Going over the API means the CLI is held to exactly the
grants any other client is.

---

## Commands

| | |
|---|---|
| `kimmy login` | Prints a token from the node's OIDC provider, via the device flow — the default |
| `kimmy login <user>` | A local account instead. Password from stdin or `KIMMY_PASSWORD` |
| `kimmy login --client-credentials` | A service account. Secret from `KIMMY_OIDC_CLIENT_SECRET` |
| `kimmy token` | The token again: prints the cached one while it is fresh, else one fresh flow |
| `kimmy ping` | Health, readiness and the node's version and capabilities. Needs no token |
| `kimmy whoami` | How the node sees you: principal, local or federated, and your grants |
| `kimmy roles list` `show` `create` | Stored roles ([Security](security.md)). Create: repeat `--grant 'db:collection:actions'` |
| `kimmy roles grant` `revoke` | Add or remove actions on one of a role's grants — live, no restart |
| `kimmy roles delete <name>` | Principals lose its grants on their next request |
| `kimmy users list` `show` | Local accounts and their state |
| `kimmy users create <user>` | Password from stdin or `KIMMY_PASSWORD`. Repeatable `--grant`, `--role` |
| `kimmy users reset-password <user>` | New password from stdin; existing sessions end |
| `kimmy users set-grants` `set-roles` | Replace direct grants / stored roles wholesale |
| `kimmy users disable` / `enable` | Disable ends sessions and refuses logins but keeps the record — reversible delete |
| `kimmy users delete <user>` | Outright removal |
| `kimmy topology` | The nodes of the cluster, and which are live |
| `kimmy databases` | Databases you can read |
| `kimmy collections <db>` | Collections in a database |
| `kimmy create-collection <db.coll>` | Idempotent: a collection that already exists reports `{"exists": ...}` and succeeds |
| `kimmy find <db.coll> [filter]` | `--sort --projection --limit --skip --explain` |
| `kimmy count <db.coll> [filter]` | |
| `kimmy insert <db.coll> [document]` | Reads stdin when the document is omitted |
| `kimmy bulk-insert <db.coll> [documents]` | A JSON array, in one commit, all or nothing. Reads stdin when omitted; at most 1000 |
| `kimmy update <db.coll> <filter> <update>` | `--multi` |
| `kimmy delete <db.coll> <filter>` | `--multi` |
| `kimmy aggregate <db.coll> [pipeline]` | Reads stdin when the pipeline is omitted |
| `kimmy describe <db.coll>` | Inferred schema. `--sample` |
| `kimmy create-index <db.coll> <name> <fields>` | `item,-qty`. `--unique --expire-after-seconds --partial` |
| `kimmy drop-index <db.coll> <name>` | Dropping one that is not there succeeds |
| `kimmy indexes <db.coll>` | |
| `kimmy vector-search <db.coll> [query]` | Search by meaning. `--vector --k --filter --per-document` |
| `kimmy hybrid-search <db.coll> [query]` | Dense and lexical, fused by rank. Same flags |
| `kimmy watch <db.coll>` | Follow changes until interrupted, one event per line. `--full --resume-after` |
| `kimmy backup --out <file>` | Whole node. Needs `admin` over `*`. `-` for stdout |

Global: `--url` (`KIMMY_URL`), `--token` (`KIMMY_TOKEN`), `--pretty`.

A target is `db.collection`, split at the **first** dot — a collection name may
contain one (`orders.__vectors`), a database name may not.

---

## Searching by meaning

```bash
kimmy vector-search shelf.articles "how do I look after my bread starter" --k 3
```

```json
{"results":[{"_id":"a3","score":0.6276},{"_id":"d2","score":0.5851}]}
```

The query is text by default and **the server embeds it**, using whatever
provider the collection is configured with — so the same model that embedded the
documents embeds the query, which is the only way the scores mean anything.

`--vector` sends an embedding computed elsewhere instead. That is not merely an
optimisation: a collection configured `byo` has no provider to embed text with
and will refuse a text query rather than return an empty result that looks like
"no matches". The array is checked for being numbers here, before the request,
because the server's complaint would be about dimensions and the mistake is a
type.

`--filter` is an ordinary query-language document and runs *first*, restricting
the search to what it matches. `--per-document` caps how many chunks of one
document may fill result slots, so a single long document cannot take every one.

`hybrid-search` takes the same flags and runs a dense and a lexical search,
fusing them with Reciprocal Rank Fusion. **Its scores are fusion scores** — much
smaller numbers, and not comparable with the similarity scores `vector-search`
returns. Compare rankings between them, never scores.

Two refusals are worth expecting rather than reading as bugs:

```
$ kimmy vector-search shelf.notes "anything"
kimmy: 400 bad_request: collection "notes" has no vector configuration

$ kimmy vector-search shelf.articles --vector '[0.1,0.2,0.3]'
kimmy: 400 bad_request: query vector has 3 dimensions, but this collection stores 1024
```

A collection with no vectors stored at all answers `409 no_vectors` rather than
an empty result, because "nothing matched" and "nothing was ever ingested" are
different problems and only one of them is fixed by rewording the query.

---

## There is no `--password` flag

Deliberately. A password on the command line lands in shell history and is
visible in `ps` to every user on the machine — a credential that leaks by being
typed. The password comes from stdin or `KIMMY_PASSWORD`:

```bash
export KIMMY_TOKEN=$(echo hunter2 | kimmy login root)
export KIMMY_TOKEN=$(KIMMY_PASSWORD=hunter2 kimmy login root)
```

A test asserts the flag does not exist, so it cannot be added back as a
convenience without someone deciding to. The same goes for `--client-secret`
below, for the same reason and with its own test.

**The token is not written to disk.** `login` prints it and nothing else, so it
is usable directly in `$(...)`. A CLI that stored a bearer token in a file would
have to answer for its permissions, its lifetime and its cleanup; an environment
variable answers all three by not existing afterwards. Nothing here keeps a
refresh token either — it never asks for one.

---

## Logging in through an identity provider

Bare `kimmy login` federates — the device flow against the node's OIDC
provider is the default, because that is what a federated deployment almost
always wants. Naming a local account takes the password path instead:

```bash
export KIMMY_OIDC_ISSUER=https://auth.example.com
export KIMMY_OIDC_CLIENT_ID=kimmy-cli

# A person, in a browser. RFC 8628 device authorization. `--oidc` spells the
# default out, for scripts and muscle memory written before it was one.
export KIMMY_TOKEN=$(kimmy login)

# A service. The secret comes from KIMMY_OIDC_CLIENT_SECRET and nowhere else.
export KIMMY_TOKEN=$(kimmy login --client-credentials)
```

The device flow prints a code and a URL to **stderr**, waits while you approve
it in a browser, and puts the bare token on **stdout** — so `$(...)` captures
the token and the instructions still reach the terminal:

```
Open https://auth.example.com/device and enter the code: WDJB-MJHT
Waiting for approval...
```

**The device flow rather than a redirect**, for the reason `gh auth login` uses
it: a redirect needs a browser and a loopback listener on the same machine, and
a database CLI is run over SSH and inside containers. It honours whatever
polling interval the provider asks for, including a `slow_down`.

These talk to the **identity provider**, not to a node — the one place this tool
does not go through `kimmy-client`, because an OAuth2 implementation inside a
database client library is one every application linking it would inherit.

### Scopes differ between the two flows

Left unset, the device flow asks for `openid profile` and
`--client-credentials` asks
for **nothing**. That is not an oversight: there is no end user in the
client-credentials grant, so `openid` requests an ID token that cannot be
issued — providers split between ignoring it and refusing the request. A
service client gets whatever scopes it is registered for. `--scope` overrides
either flow.

### Client authentication

`--client-credentials` sends the id and secret as **HTTP Basic** when the
provider advertises `client_secret_basic`, and in the request body only when
the provider advertises the body and not Basic. RFC 6749 §2.3.1 requires every
authorization server to support Basic and leaves the body optional, so Basic is
the one that is always there — and a provider advertising nothing gets it.

### Reusing a token between commands

`kimmy token` is the spelling that reuses by existing: it prints the cached
access token whenever one is still fresh, and otherwise runs one federated
flow, keeps the result, and prints it — so after the first call,
`$(kimmy token)` is instant until the token nears expiry:

```bash
curl -s -H "Authorization: Bearer $(kimmy token)" "$KIMMY_URL/v1/databases"
```

`kimmy login` writes nothing to disk unless you ask:

```bash
export KIMMY_TOKEN=$(kimmy login --cache-token)
```

With `--cache-token` (or `KIMMY_TOKEN_CACHE=1`) — or by using `kimmy token`,
where caching is the point rather than an option (ADR-075's "only when asked";
invoking the command *is* asking) — the access token is kept in a `0600` file
under `$XDG_CACHE_HOME/kimmy` — `~/.cache/kimmy/tokens.json` by default —
keyed by issuer, client and resource, and reused until it is within a minute of
expiring. A later `kimmy login --cache-token` then prints the cached token
instead of making you approve the device flow again.

**A refresh token is never requested and never stored**, flag or no flag. The
access token is short-lived and audience-restricted; a refresh token outlives
the session and can mint more, which makes caching it a different feature with
a different risk ([ADR-075](decisions.md)).

To forget everything cached:

```bash
rm -f "${XDG_CACHE_HOME:-$HOME/.cache}/kimmy/tokens.json"
```

Local accounts still work on a federated node, and `kimmy login <user>` is how
you reach the break-glass administrator: `admin` cannot be granted through an
IdP claim ([ADR-067](decisions.md)).

---

## Output and exit codes

JSON on **stdout**, diagnostics on **stderr**, so `kimmy find ... | jq` works
without flags and a failure never puts something on stdout that a pipeline could
mistake for a result.

Exit is non-zero on any failure — including an HTTP error status, so a script
that checks `$?` does not have to parse the response to know something went
wrong. An empty result set is a *success*.

Errors carry the server's own message rather than the status alone:

```
$ kimmy find shop.nosuch
kimmy: 404 not_found: collection "shop"."nosuch" not found

$ kimmy databases        # with no token
kimmy: 401 unauthorized: missing Authorization header
  set --token, or KIMMY_TOKEN from `kimmy login`
```

The first line is the server's own code and message; the second is the one hint
this tool adds, because that failure is fixed with a flag rather than by
changing the request. Both go to stderr.

The hint follows the deployment. With `KIMMY_OIDC_ISSUER` set it names the
federated route instead, because sending a federated user to `kimmy login
<user>` asks them for a password they do not have:

```
  set --token, or KIMMY_TOKEN from `kimmy login` (issuer
  https://auth.example.com); a local account still works with `kimmy login <user>`
```

## Empty listings and zero grants

`kimmy databases` lists databases you can **read** — authorization filters the
listing server-side. So an identity whose token carries no grants sees exactly
what an empty cluster looks like: `{"databases":[]}`, then a bare 403 on the
first real operation. When a listing comes back empty, the CLI asks
`/v1/auth/whoami` once and, if that identity holds no grants at all, adds one
line on stderr:

```
note: this identity carries no grants, so listings only show what you are allowed to read.
       run `kimmy whoami` to see how the node sees you.
```

stdout is unchanged — scripts piping into `jq` see the same bytes either way.
The note appears only when grants are actually empty; an identity *with* grants
seeing an empty result may simply be looking at an empty namespace.

The usual cause on a federated login is a roles claim no `role_mappings`
turned into grants (see [security](security.md)); on a local login it is a user
record whose direct grants were never set or were revoked.

---

## Indexes

```bash
kimmy create-index shelf.orders item_qty 'item,-qty'
```

Fields are a comma-separated list of paths, `-` for descending — rather than
`[{"path":"item"},{"path":"qty","descending":true}]`. Index fields are the most
tedious JSON this tool would otherwise ask you to type, and a CLI that makes you
hand-write the wire format is not saving you from `curl`. Paths are dotted, so
neither a comma nor a leading `-` appears in a real one.

Everything the route accepts is reachable, so nothing about indexes needs HTTP:

```bash
kimmy create-index shelf.users uniq_email 'email' --unique
kimmy create-index shelf.sessions ttl_seen 'seen' --expire-after-seconds 3600
kimmy create-index shelf.users has_email 'email' --partial '{"email":{"$exists":true}}'
kimmy drop-index shelf.orders item_qty
```

Read [Indexes](indexes.md) before relying on `--unique`: it is enforced **per
node**, not cluster-wide, and that document explains exactly what it does and
does not promise.

Re-creating an index with the **same** definition succeeds. The same name with a
*different* definition is a conflict rather than a silent redefinition:

```
$ kimmy create-index shelf.idx item_idx 'qty'
kimmy: 409 conflict: ... index already exists with different fields
```

Dropping an index that is not there reports `{"dropped": false}` and succeeds —
the collection ends up without that index either way, which is what was asked.

Use `kimmy find ... --explain` to confirm one is actually being used:

```json
{"documentsExamined":1,"documentsMatched":1,"index":"item_idx","strategy":"index"}
```

`"strategy":"collectionScan"` with `documentsExamined` equal to the collection
size means it is not.

---

## Next

- [HTTP API](http-api.md) — the endpoints these commands call
- [Query Language](query-language.md) — what goes in a filter
- [Aggregation](aggregation.md) — what goes in a pipeline
