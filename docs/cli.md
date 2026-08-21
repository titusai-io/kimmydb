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
kimmy: 404 not_found: collection "shop"."nosuch" not found

$ kimmy databases        # with no token
kimmy: 401 unauthorized: missing Authorization header
  set --token, or KIMMY_TOKEN from `kimmy login`
```

The first line is the server's own code and message; the second is the one hint
this tool adds, because that failure is fixed with a flag rather than by
changing the request. Both go to stderr.

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
