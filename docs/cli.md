# `kimmy` — the terminal client

[← Documentation index](README.md)

One-shot commands that speak the ordinary HTTP API, print JSON on stdout, and
exit non-zero when they fail.

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
| `kimmy login <user>` | Prints a token. Password from stdin or `KIMMY_PASSWORD` |
| `kimmy ping` | Health, readiness and the node's version and capabilities. Needs no token |
| `kimmy topology` | The nodes of the cluster, and which are live |
| `kimmy databases` | Databases you can read |
| `kimmy collections <db>` | Collections in a database |
| `kimmy create-collection <db.coll>` | Creating one is idempotent |
| `kimmy find <db.coll> [filter]` | `--sort --projection --limit --skip --explain` |
| `kimmy count <db.coll> [filter]` | |
| `kimmy insert <db.coll> [document]` | Reads stdin when the document is omitted |
| `kimmy bulk-insert <db.coll> [documents]` | A JSON array, in one commit, all or nothing. Reads stdin when omitted; at most 1000 |
| `kimmy update <db.coll> <filter> <update>` | `--multi` |
| `kimmy delete <db.coll> <filter>` | `--multi` |
| `kimmy aggregate <db.coll> [pipeline]` | Reads stdin when the pipeline is omitted |
| `kimmy describe <db.coll>` | Inferred schema. `--sample` |
| `kimmy indexes <db.coll>` | |
| `kimmy watch <db.coll>` | Follow changes until interrupted, one event per line. `--full --resume-after` |
| `kimmy backup --out <file>` | Whole node. Needs `admin` over `*`. `-` for stdout |

Global: `--url` (`KIMMY_URL`), `--token` (`KIMMY_TOKEN`), `--pretty`.

A target is `db.collection`, split at the **first** dot — a collection name may
contain one (`orders.__vectors`), a database name may not.

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
convenience without someone deciding to.

**The token is not written to disk.** `login` prints it and nothing else, so it
is usable directly in `$(...)`. A CLI that stored a bearer token in a file would
have to answer for its permissions, its lifetime and its cleanup; an environment
variable answers all three by not existing afterwards.

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
kimmy: 404 Not Found: collection "shop"."nosuch" not found

$ kimmy databases        # with no token
kimmy: 401 Unauthorized: expected an Authorization: Bearer token
       (set --token, or KIMMY_TOKEN from `kimmy login`)
```

---

## Next

- [HTTP API](http-api.md) — the endpoints these commands call
- [Query Language](query-language.md) — what goes in a filter
- [Aggregation](aggregation.md) — what goes in a pipeline
