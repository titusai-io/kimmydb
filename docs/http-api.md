# HTTP API

[← Documentation index](README.md)

Everything speaks JSON over HTTP; change streams use WebSocket. Implemented in
`kimmy-api`.

Default port **7878**.

**This is KimmyDB's client protocol, not one of several.** HTTP provides
framing, Extended JSON v2 provides the encoding for types JSON cannot express,
a bearer token from `/v1/auth/login` provides authentication,
`{"error": "<code>", "message": "…"}` provides errors, and the WebSocket at
`/watch` provides streaming. The MongoDB wire protocol, gRPC and GraphQL were
all considered and rejected — [ADR-055](decisions.md) has the reasoning.

**The specification is [`openapi.yaml`](openapi.yaml)**, and it is the
authority: an OpenAPI 3.1 document covering every route, checked by a contract
test that drives each operation against a running server and validates the
response against the declared schema ([ADR-056](decisions.md)). Point a client
generator at it.

This page stays as the *reference* — the one to read to learn the API, with the
reasoning a schema cannot carry. The same test requires every registered route
to appear here too, because a table titled **Endpoints** that reads as complete
has been incomplete before.

---

## Endpoints

| Method | Path | Action required |
|---|---|---|
| `GET` | `/healthz` | — public |
| `GET` | `/readyz` | — public |
| `GET` | `/metrics` | — public |
| `GET` | `/.well-known/oauth-protected-resource` | — public — see [Protected resource metadata](#protected-resource-metadata) |
| `GET` | `/.well-known/oauth-protected-resource/{resource_path}` | — public — the same document, for a resource identifier that has a path |
| `POST` | `/v1/auth/login` | — public |
| `POST` | `/v1/auth/refresh` | authenticated — see [Tokens](#tokens) |
| `GET` | `/v1/topology` | authenticated — see [Topology](#topology) |
| `GET` | `/v1/version` | — public — see [Version and capabilities](#version-and-capabilities) |
| `GET` | `/v1/auth/whoami` | authenticated |
| `GET` `POST` | `/v1/users` | server admin |
| `GET` `DELETE` | `/v1/users/{name}` | server admin |
| `POST` | `/v1/users/{name}/password` | own account, or server admin |
| `POST` | `/v1/users/{name}/grants` | server admin |
| `POST` | `/v1/users/{name}/disabled` | server admin |
| `POST` | `/v1/users/{name}/roles` | server admin |
| `GET` `POST` | `/v1/roles` | server admin |
| `GET` `DELETE` | `/v1/roles/{name}` | server admin |
| `POST` | `/v1/roles/{name}/grants` | server admin |
| `GET` | `/v1/databases` | `read` (filtered) |
| `GET` `POST` | `/v1/db/{db}/collections` | `read` (filtered; 404 if the database does not exist) / `ddl` |
| `DELETE` | `/v1/db/{db}/coll/{coll}` | `ddl` |
| `POST` | `/v1/db/{db}/coll/{coll}/docs` | `write` |
| `GET` | `/v1/db/{db}/coll/{coll}/docs` | `read` |
| `POST` | `/v1/db/{db}/coll/{coll}/bulk` | `write` |
| `GET` `PUT` `DELETE` | `/v1/db/{db}/coll/{coll}/docs/{id}` | `read` / `write` / `write` |
| `POST` | `/v1/db/{db}/coll/{coll}/find` | `read` |
| `POST` | `/v1/db/{db}/coll/{coll}/count` | `read` |
| `POST` | `/v1/db/{db}/coll/{coll}/update` | `write` |
| `POST` | `/v1/db/{db}/coll/{coll}/find_and_modify` | `write` |
| `POST` | `/v1/db/{db}/coll/{coll}/delete` | `write` |
| `POST` | `/v1/db/{db}/coll/{coll}/aggregate` | `read` (and `search`, implied by it, for a `$vectorSearch` stage) — see [Aggregation](aggregation.md) |
| `GET` | `/v1/db/{db}/coll/{coll}/describe` | `read` |
| `POST` | `/v1/db/{db}/coll/{coll}/vector` | `ddl` — configure embedding ([Vectors](vectors.md)) |
| `GET` `PUT` `DELETE` | `/v1/db/{db}/coll/{coll}/docs/{id}/vectors` | `read` / `write` / `write` |
| `POST` | `/v1/db/{db}/coll/{coll}/vector_search` | `search` ([Vectors](vectors.md)) |
| `POST` | `/v1/db/{db}/coll/{coll}/hybrid_search` | `search` ([Vectors](vectors.md)) |
| `GET` `POST` | `/v1/db/{db}/coll/{coll}/webhooks` | `webhook` ([Webhooks](webhooks.md)) |
| `DELETE` | `/v1/db/{db}/coll/{coll}/webhooks/{id}` | `webhook` ([Webhooks](webhooks.md)) |
| `GET` `POST` | `/v1/db/{db}/coll/{coll}/indexes` | `read` / `ddl` |
| `DELETE` | `/v1/db/{db}/coll/{coll}/indexes/{name}` | `ddl` |
| `GET` | `/v1/db/{db}/coll/{coll}/violations` | `read` — unique violations still standing ([Indexes](indexes.md#resolving-a-unique-violation)) |
| `GET` | `/v1/db/{db}/coll/{coll}/watch` | `watch` (WebSocket) |
| `GET` | `/v1/admin/backup` | `admin` over `*` — see [Backup](#backup) |
| `POST` | `/mcp` | authenticated; per-tool ([MCP](mcp.md)) |

Every route the server registers is in that table. Several have their own page
for the detail — the link is in the row — but a reader should not have to find
the page to learn the endpoint exists.

---

## Backup

```
GET /v1/admin/backup
```

Streams a consistent backup of the whole node as `application/octet-stream`.
Requires `admin` over `*`. Restore it with `kimmyd restore --from <file>` while
the target node is stopped. See [Operations](operations.md#backup-and-restore).

---

## Authentication

Every non-public endpoint expects a bearer token:

```
Authorization: Bearer <jwt>
```

```bash
# KIMMY_ROOT_PASSWORD is whatever the node was bootstrapped with.
TOKEN=$(curl -s -XPOST localhost:7878/v1/auth/login \
  -H 'content-type: application/json' \
  -d "{\"user\":\"root\",\"password\":\"$KIMMY_ROOT_PASSWORD\"}" | jq -r .token)
```

```json
{ "token": "eyJ0eXAiOiJKV1Q...", "user": "root" }
```

Tokens are signed with a cluster-wide secret, so any node validates any node's
token. Default lifetime one hour (`auth.token_ttl_secs`). See
[Security](security.md).

**Use `https://` when the node is configured with a certificate.** TLS
terminates on the same listener and the same port — there is no plaintext half
and no redirect, so a plaintext request to a TLS port simply fails. Change
streams become `wss://` on that node. See
[Security](security.md#tls).

**Login is rate-limited.** Repeated *failures* from one caller earn a `429` with
a `Retry-After` header; a successful login spends nothing, so a client that
re-authenticates on a short TTL is never throttled for succeeding. Tunable under
`[server.rate_limit]` — see [Security](security.md#login-rate-limiting).

**Authenticated requests carry three limits** ([ADR-099](decisions.md)), each
defaulting to what the server always did: a body ceiling
(`server.max_body_bytes`, 2 MiB; over it `413 payload_too_large`), a deadline
for a request still waiting on its body or on an embedding provider
(`server.request_timeout_secs`, 30 s; past it `503 timeout`), and — off unless
an operator sets it — a per-principal request budget
(`server.rate_limit.per_principal`; over it `429` with `Retry-After`, on any
route that takes a token). A client should treat a `429` as possible on every
authenticated call, not only on login. See
[Security](security.md#limits-on-authenticated-requests).

**Login may be restricted to the host.** Under `auth.local.login =
"loopback_only"`, `/v1/auth/login` and `/v1/auth/refresh` answer `403
forbidden` to any connection whose TCP peer is not loopback; under `"disabled"`
both answer `404`. A token already issued keeps working under either — the
setting restricts minting, not verifying — and federated tokens are unaffected.
See [Security](security.md#local-login-is-a-mode).

---

## Documents

### Insert

```bash
curl -XPOST localhost:7878/v1/db/shop/coll/orders/docs -H "$A" \
  -d '{"item":"widget","qty":5,"tags":["a","b"]}'
```

```json
{ "insertedId": { "$oid": "6745f2a1b3c4d5e6f7081920" } }
```

`_id` is generated if absent: a random ObjectId, or, on a request carrying an
`Idempotency-Key`, a string derived from the key and the request (see
[The derived `_id`](#the-derived-_id)). A duplicate `_id` is **409** with
`"error": "duplicate_key"`.

### Bulk insert

```bash
curl -XPOST localhost:7878/v1/db/shop/coll/orders/bulk -H "$A" \
  -d '[{"item":"widget","qty":5},{"item":"gadget","qty":2}]'
```

```json
{
  "inserted": 2,
  "insertedIds": [
    { "$oid": "6745f2a1b3c4d5e6f7081920" },
    { "$oid": "6745f2a1b3c4d5e6f7081921" }
  ]
}
```

The body is a bare array, and `insertedIds` comes back in submission order.

**All of the batch is written, or none of it.** The whole batch is one durable
commit, which is the point — a document inside a batch of 1000 costs about
1/175th of a document inserted on its own, because the commit dominates
([ADR-048](decisions.md)). Atomicity falls out of that single transaction, and
it is stronger than what `update` and `delete` promise: those still apply
document by document and can stop partway.

So a rejected document rejects the batch, and the message names its position:

```json
{ "error": "duplicate_key",
  "message": "document at index 17: duplicate key: 42" }
```

Nothing is written, no oplog entry is appended, and no change event is
published. Documents in the batch are checked against each other as well as
against stored state, so two documents sharing an `_id` — or colliding on a
unique index — fail the batch even though neither was there when it started.

Two ceilings, whichever binds first: **1000 documents**, and the **request
body limit** (`server.max_body_bytes`, 2 MiB by default; `413`,
`"error": "payload_too_large"`), which is the lower of the two for documents
over about 2 KB. Over the document cap is
**400**; a body that is not an array is **422**. An empty array is a no-op
that inserts nothing and commits nothing.

Measured end to end over loopback, this is what it buys: 500 documents in one
request took **0.16 s**, against **11.6 s** as 500 separate requests.

### Get, replace, delete by id

```bash
curl        localhost:7878/v1/db/shop/coll/orders/docs/42  -H "$A"
curl -XPUT  localhost:7878/v1/db/shop/coll/orders/docs/42  -H "$A" -d '{"item":"gadget"}'
curl -XPUT  'localhost:7878/v1/db/shop/coll/orders/docs/42?upsert=true' -H "$A" -d '{"item":"new"}'
curl -XDELETE localhost:7878/v1/db/shop/coll/orders/docs/42 -H "$A"
```

The `{id}` segment is interpreted by shape: 24 hex characters → ObjectId, an
integer → integer, anything else → string.

**A `Decimal128` `_id` is reachable by these routes only when it is an
integer** ([ADR-238](decisions.md)). An integral one in the 64-bit range is the
integer id of its value: `{"_id": {"$numberDecimal": "5"}}` is the document at
`docs/5`, and its body keeps the decimal as written. A non-integer one
(`{"$numberDecimal": "1.5"}`), or one outside the 64-bit range, has no path
form, since the segment is never read as a decimal: such a document is reached
through a filter on `_id` only (`find`, `update`, `delete`, `find_and_modify`).
Creating a document whose `_id` is a `Decimal128`, by any route, is refused
`400 bad_request` naming `cluster.decimal128` until the operator has set it
and every live member can hold one
([Operations](operations.md#clusterdecimal128-and-the-marker)); so is creating
an index whose `partialFilterExpression` holds a `Decimal128`.

`PUT` returns `{"matched": 0|1, "modified": 0|1, "upserted": true|false}` —
**counts**, as `update` and `find_and_modify` return, even though a replace
touches at most one document. Replacement is **not** a merge — unnamed fields
are dropped — and `_id` always comes from the path, never the body.

**A body identical to the stored document is not written**
([ADR-208](decisions.md)): `{"matched": 1, "modified": 0, "upserted": false}`,
no `stamp`, and the document keeps its version, so no oplog entry, no change
event and nothing for other nodes. Identical means the stored bytes: the same
fields in another order are a change. `_id` keeps the type it was
stored with, so a `PUT` over a document inserted with a 32-bit integer `_id`
leaves it 32-bit and an identical body is no change. A `PUT` with `upsert` that
creates the document keeps the body's `_id` when it names the same id, else
stores a 32-bit integer for a path id that fits one.

**Without `?upsert=true` a missing document is not an error.** The answer is
`200 {"matched": 0}` and nothing is written. A test built on the assumption
that this creates the document writes nothing and passes.

`DELETE` returns `{"deleted": 1, "stamp": "…"}` when it removed the document
and `{"deleted": 0}` when there was none to remove — a count, like `PUT`, and
not an error either way. The `stamp` is the **tombstone's** version, the one
the delete produced, exactly as `POST .../delete` of one document reports it;
it is present when and only when `deleted` is `1`. The tombstone is what keeps
a peer that never saw the delete from replaying its older image of the
document, for as long as it is retained ([ADR-085](decisions.md)); dropping a
*collection* carries the same guarantee, on its own tombstone — see
[Databases and collections](#databases-and-collections).

**Every write reports the version it produced**, as `stamp` — an opaque token
— and a read by id carries the document's version as its `ETag`. Pass one
back as `if_stamp` to make the next write conditional:

```bash
curl -i localhost:7878/v1/db/shop/coll/orders/docs/42 -H "$A"
# ETag: "AAABmR3k9...."
curl -XPUT 'localhost:7878/v1/db/shop/coll/orders/docs/42?if_stamp=AAABmR3k9....' \
  -H "$A" -d '{"item":"gadget","qty":6}'
curl -XDELETE 'localhost:7878/v1/db/shop/coll/orders/docs/42?if_stamp=AAABmR3k9....' -H "$A"
```

The write happens only if the document is still at that version. Otherwise
the answer is **`409 stale`** with `retry: no` and *nothing is written* — not
the document, not an oplog entry, not a change-stream event. A missing
document is stale too, `upsert` or not: the condition says "at this version",
not "or create it". Re-read, decide again, and send a new request with the
current stamp. This is check-then-act on one document, on one node, with no
coordination ([ADR-084](decisions.md)); it says nothing about other nodes,
where last-writer-wins still decides.

The token is opaque, and the only place to get one is a `stamp`, `stamps` or
`ETag` the server sent. An `if_stamp` that is not one — truncated, re-encoded,
invented — is refused before anything is read, `400 bad_request` with the
message `malformed stamp: pass back a stamp exactly as the server returned
it`; it is not treated as "some other version" and answered `409 stale`.

### Find

```bash
curl -XPOST localhost:7878/v1/db/shop/coll/orders/find -H "$A" -d '{
  "filter":     { "qty": { "$gt": 4 }, "tags": "a" },
  "sort":       { "qty": -1 },
  "projection": { "item": 1, "qty": 1, "_id": 0 },
  "limit":      50,
  "skip":       0
}'
```

```json
{ "count": 2, "documents": [ { "item": "gadget", "qty": 12 }, … ] }
```

Every field is optional. `count` is the size of the returned page, not the
total match count — use `/count` for that. Operator reference:
[Query Language](query-language.md).

`"stamps": true` adds a `stamps` array **parallel to `documents`** — each
document's version, for an `if_stamp` write that follows. Parallel rather
than a field inside each document, because the document is your data and
comes back exactly as stored. A `find` on `_id` takes the primary-key path,
so this is the cheap way to read one document *with* its version.

**Default limit 100, maximum 10,000, and both are silent.** Omitting `limit`
returns a page of 100 rather than the collection, and a larger `limit` is
clamped rather than refused. `limit: 0` is legal and is an empty page —
`{"documents": [], "count": 0}`, with no `nextCursor` — on every sort order.
To read everything, walk with a cursor:

```json
{ "filter": {}, "limit": 100 }
// -> { "documents": [...], "count": 100, "nextCursor": "AoAAAAAAAAAq" }
{ "filter": {}, "limit": 100, "cursor": "AoAAAAAAAAAq" }
```

The token is opaque, carries no server state, and is **portable across
nodes** — a page fetched from one node continues on another, which is what
makes the node list from [Topology](#topology) usable for reads as well as
failover. End the walk on a short or empty page, not on a missing token.
[Cursors](query-language.md#cursors) has the full contract.

**A sorted `find` holds `skip + limit` documents, and that window stops at
10,000.** A larger one is refused with `400` rather than clamped — a clamped
`skip` would return a different page and say nothing. An unsorted `find`, or
one sorted by `{"_id": 1}`, holds only its page and has no such ceiling,
though `skip` still visits everything it steps over. To page deeper through
another order, narrow the filter on the sort field to where the last page
ended — `{"score": {"$lt": <last score seen>}}` — which costs a page rather
than everything before it ([ADR-098](decisions.md)).

**A page holds what it returns**, projection included: the projection is
applied as each match is visited, not to the page afterwards, so asking for
`{"_id": 1}` costs ids and not documents ([ADR-150](decisions.md)). Over a
collection of very large documents the projection is as much a capacity
setting as the limit is —
[Vectors](vectors.md#what-a-page-over-the-shadow-collection-costs) has the
arithmetic for a vector shadow collection, where one page of 10,000
unprojected chunks is gigabytes.

### Count, update, delete by filter

```bash
curl -XPOST localhost:7878/v1/db/shop/coll/orders/count -H "$A" \
  -d '{"filter":{"item":"widget"}}'

curl -XPOST localhost:7878/v1/db/shop/coll/orders/update -H "$A" -d '{
  "filter": { "item": "widget" },
  "update": { "$inc": { "qty": 10 } },
  "multi":  true
}'

curl -XPOST localhost:7878/v1/db/shop/coll/orders/delete -H "$A" \
  -d '{"filter":{"qty":{"$lt":1}},"multi":true}'
```

`multi` defaults to `false` — without it, one document is affected. An
omitted `filter`, or `{}`, matches every document — combined with
`multi: true` that is the deliberate way to affect the whole collection. An
explicit `filter: null` is not the same as omitting it: it is refused `422`,
not read as "no filter" ([The JSON boundary](#the-json-boundary)).

**`modified` counts documents changed** ([ADR-208](decisions.md)). A
document the update leaves byte for byte as it was — a `$set` to the value a
field already holds, an `$unset` of a field that is not there — counts in
`matched` and not in `modified`, and is **not written**: no new version, no
oplog entry, no change event, nothing replicated. `{"matched": 2,
"modified": 0, "commits": 0}` is an update that found two documents already
in the state it asked for. A single-document update that changed nothing
reports no `stamp`, and the document keeps the one it had. A value of another
type is a change (`1` over `1.0`), as is another field order; the full table
is in [Query language](query-language.md#modified-counts-changes).

**The operators run inside the write transaction.** An `update` matches and
writes in one transaction, on the image that transaction holds, so two
concurrent `$inc`s on one document both land — the same guarantee
`find_and_modify` makes, through the same engine path (ADR-083). A
`multi: true` request commits in **chunks** of `storage.multi_chunk_docs`
documents (default 1,000): each chunk is one transaction and one fsync, the
writer is released between chunks, and the response's `commits` field says
how many chunks landed ([ADR-086](decisions.md)). A chunk in which nothing
changed is not committed and not counted.

`if_stamp` makes a single-document `update` or `delete` conditional on the
matched document's version, exactly as on the by-id routes above: `409 stale`
and nothing written otherwise. It cannot be combined with `multi` — one stamp
names one document — or with `explain`, below. On these routes it is a
**body** field: `?if_stamp=…` on the URL is refused `400`, as any query
string on a route that takes none is (see
[The JSON boundary](#the-json-boundary)), rather than being read as no
condition at all — and `"if_stamp": null` in the body is refused `422` for
the same reason, rather than making the write unconditional.

An update path may address array elements — `items.$[].qty` for every
element, `items.$[line].qty` for the elements an `arrayFilters` entry
selects — which is how one line item is changed without replacing the order:

```bash
curl -XPOST localhost:7878/v1/db/shop/coll/orders/update -H "$A" -d '{
  "filter": { "_id": 42 },
  "update": { "$set": { "items.$[line].shipped": true } },
  "arrayFilters": [ { "line.sku": "gasket" } ]
}'
```

`find_and_modify` takes the same field. The rules — one identifier per filter
document, every identifier filtered and every filter used, `$` not
implemented — are in [Query language](query-language.md#positional-updates).

> **Sharp edge.** A `multi: true` request is atomic per chunk, not per
> request. Once a chunk has committed, the later chunks wait for the writer
> as long as it takes, so a busy writer does not split the request; but a
> failure after the first chunk leaves the earlier chunks committed, and is
> answered `500 partially_applied` with their counts (below). Nothing is
> visited twice and the oplog reflects exactly what landed. See
> [Storage](storage.md).

#### A request that was partly applied

A request that commits in more than one transaction — `update` or `delete`
with `multi: true`, and `DELETE /v1/db/{db}` — and fails after its first commit
is answered `500 partially_applied`, `retry: verify` ([ADR-192](decisions.md)).
What landed stands, is published and replicates. The envelope carries it, as
fields beside the usual three:

```json
{"error": "partially_applied", "message": "…", "retry": "verify",
 "applied": {"matched": 1000, "modified": 1000, "commits": 1, "in_doubt": 0},
 "cause": {"code": "stopping", "message": "the node reached its shutdown deadline"}}
```

- **`applied`** counts what the request got through before it stopped: the
  committed chunks, and chunks in which nothing changed, which commit nothing
  (`matched` includes their documents, `modified` and `commits` do not).
  **`in_doubt`** is the number of documents written in a chunk whose commit's
  outcome is unknown: they may be there. A database drop
  answers `{"dropped": [names…], "in_doubt": name-or-null}`. Its first commit is
  the database's tombstone (ADR-229), so `dropped` can be **empty** while a commit
  landed: the drop is recorded and replicates, and what this member still holds
  is buried by the next drop attempt or the next start.
- **`cause`** is the code and message the failure would have been answered
  with on its own — `bad_request` for an operator a later document cannot
  take, or for a filter whose `$expr` cannot be evaluated against a later
  document ([ADR-206](decisions.md)), `internal` for a storage failure,
  `outcome_unknown` — or `stopping`,
  the node's shutdown deadline, or `storage_failed`, a storage failure that is
  stopping the node (ADR-188). A request that begins after the deadline still
  commits its first chunk, and then answers this with `stopping`.
- **The counts say how many, not which.** Chunks follow an internal order.

**Sending it again.** A `$set` to constants, or a delete, can be sent again as
it is: it converges. **A `$inc`, `$push` or anything else that is not
idempotent cannot, and without a marker there is no recovery**: nothing
says which documents took it. So build such a request to carry one, a value
unique to the request, and filter it out:

```bash
curl -XPOST localhost:7878/v1/db/shop/coll/orders/update -H "$A" -d '{
  "filter": { "status": "open", "ops": { "$ne": "req-7f3a" } },
  "update": { "$inc": { "reminders": 1 }, "$addToSet": { "ops": "req-7f3a" } },
  "multi": true
}'
```

Sent again after `partially_applied`, it touches exactly the documents the
first attempt did not. `if_stamp` does not help here: it names one document,
and cannot be combined with `multi`. **A client must never resend a
multi-document write automatically**, on this code or any other, unless it
carries a key.

**Sending it again with its key.** A `multi: true` update or delete sent with
an [`Idempotency-Key`](#idempotency-keys) is recovered by sending it again
with the same key, whatever ended it: `partially_applied`, a dropped
connection, a crash, a node that stopped at its drain deadline. It **resumes**:
it continues past the point it reached, and writes only matching documents
that **have not changed since the request's first write**, by stamp order, so
it never writes a document twice, on any member. A match it leaves because it
changed after that write, by this request on another member or by another
write, is counted in **`skipped`**, not in `matched`, and the answer carries
**`resumed: true`**. `skipped` appears whenever it is above 0, `resumed` only
when an attempt resumed: a fresh attempt can leave a match too, since every
chunk after its first applies the rule once its record exists (another attempt
of the request may have written meanwhile):

```json
{"matched": 1800, "modified": 1800, "commits": 2, "skipped": 3, "resumed": true}
```

`matched`, `modified`, `commits` and `skipped` are cumulative across the
request's attempts. If it fails again it answers `partially_applied` as above,
with cumulative counts, and can be sent again. A request whose record says it
is done is answered with its counts (a replay); one still running on this
member is `409 idempotency_key_in_flight`, `Retry-After: 1`. A resume on the
member that started it starts after the point the record names; on another
member it starts from the beginning and relies on the stamp rule alone.

**A keyed resend leaves the matches that changed.** That is the point for a
`$inc`, which must not land twice, and a difference for a `$set` to constants,
which an unkeyed resend would also write to the documents changed meanwhile. A
client that needs every *current* match written sends the request again
**without the key**, for an update that is idempotent, or uses the marker
pattern above, which is also the answer for a client that sends no keys.

### Describe

Sample a collection and report the field paths it actually contains, their BSON
types, and how often each appears — useful against a schemaless store where a
filter on a misremembered field name returns an empty result rather than an
error.

```bash
curl "localhost:7878/v1/db/shop/coll/orders/describe?sample=200&examples=true" -H "$A"
```

| Query | |
|---|---|
| `sample` | Documents to inspect. Default 100. `0` is refused `400` naming the parameter — a sample of nothing describes nothing — and a value above 1000 is clamped to 1000, as `find` clamps `limit`; `sampled` in the answer is how many were actually read |
| `examples` | Include one example value per field |

Array elements are reported under `path[]`, matching how a query on the field
matches an *element*. `presence` is a fraction of the **sample**, counting
documents — it is inference, not a schema, and a field missing from the sample
may still exist.

Nesting is expanded six levels below the top-level fields, so a reported path
has at most seven components; whatever sits deeper is reported as an `object`
or `array` at the last expanded level and not opened, which is what keeps one
pathological document from producing a field list longer than the documents it
describes. With `examples=true` an `example` is always a scalar: a field whose
values are objects, arrays or `null` carries none, because the field list
already says what they hold, and a string over 120 bytes is passed over in
favour of a shorter occurrence. Scalar there means *BSON* scalar, so a date,
an ObjectId or a binary value is an example and arrives in its
[Extended JSON](#the-json-boundary) form — `"example": {"$date":
1754006400000}` is a scalar example, not a container one. A field is left
without an example when no occurrence in the sample qualifies, which for a
field whose every value is a long string means none at all: the key is
absent rather than `null`.

`nodeDurability` is the durability class of the node that answered —
`durable` or `coalesced`, the same value `GET /v1/version` reports as
`durability` ([Storage](storage.md#durability-classes)). It is a fact about
the node, not the collection, and is repeated here so the one call made before
writing already says what an acknowledged write means.

The same information backs the MCP `describe_collection` tool; see
[MCP](mcp.md).

---

## Idempotency keys

A document write may carry an **`Idempotency-Key`**. The member that applies
the write keeps a record of it in the write's own transaction, so a retry of
the same request with the same key gets the **first answer** back, marked
`Idempotency-Replayed: ?1`, and nothing is written again. The record survives
exactly what the write survives: a crash, a failed fsync, a restart, under both
durability classes. This is what makes a write safe to send again after an
answer that never came or said
[`outcome_unknown`](clients.md#retrying-after-an-unknown-outcome).

```bash
curl -XPOST localhost:7878/v1/db/shop/coll/orders/docs -H "$A" \
  -H 'Idempotency-Key: "6f1c0e52-8a1d-4f53-9b3e-2c1d6a7e4b90"' \
  -d '{"item":"widget","qty":5}'
```

Sent again, the same request answers `200` with the same body, `insertedId`
and `stamp` included, and the header `Idempotency-Replayed: ?1`. A member
lists the capability `idempotency-keys` in
[`GET /v1/version`](#version-and-capabilities) while it takes the header.

### The header

The value is a structured-field **String** (RFC 9651), as the
draft-ietf-httpapi-idempotency-key-header-07 requires: **quoted**, and once
unquoted and unescaped, **16 to 255 printable ASCII characters**. `\"` and
`\\` are the only escapes, and each counts as one character. Use a UUID (v4
or v7), a new one for every new request. Refused `400 bad_request`, with the
message naming the header and what is wrong:

- the header on more than one field line;
- an empty or unquoted value (a bare token is not a String);
- a key shorter than 16 or longer than 255 characters, or holding a character
  outside printable ASCII;
- parameters on the value (`"k";a=1`), which are refused rather than ignored,
  or anything else after the string.

### Routes that take it

| Route | |
|---|---|
| `POST …/docs` | insert |
| `POST …/bulk` | one key for the whole batch |
| `PUT …/docs/{id}` | replace. The method is idempotent already; a key turns a retry's `409 stale` (with `if_stamp`) into the first answer |
| `DELETE …/docs/{id}` | and turns a retry's `{"deleted": 0}` into the first answer |
| `POST …/update` | single, or `multi: true` |
| `POST …/find_and_modify` | including an upsert |
| `POST …/delete` | single, or `multi: true` |
| MCP tools `insert`, `insert_many`, `update`, `delete` | as the argument `idempotency_key`, the same key without the quotes: an MCP client sets no header per tool call. A replayed tool result carries `"kimmydb/idempotency-replayed": true` in its `_meta` ([MCP](mcp.md)) |

**Every other route refuses the header with `400 bad_request`** ("this route
does not take an Idempotency-Key"): reads, including the `POST` `find`,
`count` and `aggregate`; schema changes; vectors, webhooks, users, roles and
the backup; the health probes; `/mcp` itself; and `update` and `delete` with
`explain: true`, which write nothing. A client that believes it is protected
and is not is worse off than one told no, so the header is refused where it
would do nothing, never ignored. Schema changes are left out on purpose: their
retries already say what an earlier attempt did (a second create is `409
conflict`, a second drop is `{"dropped": false}`), and a database drop that
answered `partially_applied` is recovered by sending it again. The route's
refusal, and a malformed key's, are made before authentication, as a query
string's is: they read the header and the route and nothing else. Whether this
member takes keys now ([The gate](#the-gate)) is asked after authentication,
since its refusal can name a member of the cluster: a keyed request with no
token is `401`, as without the header.

**This changed in 0.52.0.** Earlier releases ignored the header on every
route, so a client or a proxy that stamps it on every request breaks against
this release. A member of an earlier release still ignores it, and does not
list `idempotency-keys`.

### Same key, same request

A key belongs to a **principal and a collection**: the same key from another
user, or on another collection, is another key. The principal is its name,
`local:<user>` or, for a federated caller, its issuer and subject; users
replicate cluster-wide, so a caller has the same scope on every member. A
local user deleted and created again under the same name within the window
inherits the old one's keys. The key itself is never stored, only a hash over
the principal, the collection and the key.

What makes a retry **the same request** is its fingerprint: a SHA-256 over the
method, the route, the path and query string **as sent**, and the body
**as sent**. A retry must therefore send the same bytes: a body whose fields
come in another order, or a path percent-encoded differently, is another
request. An MCP call is fingerprinted over the tool and its arguments without
`idempotency_key`, with every document's fields in the order sent.

### What each case answers

| Case | Answer |
|---|---|
| The member does not take keys now (see [The gate](#the-gate)) | `400 bad_request` naming the header and the cause. Nothing is looked up or written |
| No record of the key | Processed as without a key. If it wrote anything, the record is written with it. No marker |
| A record, the same request | The first answer: `200` and the body the first attempt returned, with `Idempotency-Replayed: ?1`. Nothing is written. Under `coalesced` it is answered once the record's commit is durable |
| A record, a different request | `422 idempotency_key_reused`, `retry: no`. Nothing is written |
| A `multi: true` update or delete with this key is still running on this member | `409 idempotency_key_in_flight`, `retry: wait`, `Retry-After: 1`. Nothing is written; sent again here once it ends, it is answered from its record |
| A record of a `multi: true` request that stopped and is not running here | It resumes: [a request that was partly applied](#a-request-that-was-partly-applied) |
| Two single-document requests with one key at once | They wait for the single writer as any write does: whichever commits first applies, and the other is answered from its record |
| The request changed nothing: refused, matched nothing, or left every document as it was ([ADR-208](decisions.md)) | No record: a retry is evaluated again, and since the first attempt changed nothing it is still the request's only change |

**Authorization comes first.** A principal whose grant was revoked since gets
`403`, not a replay.

**A replay is the first answer, not the current state.** Its `stamp` names the
version the first attempt produced; an `if_stamp` built from it is `409
stale` if the document has moved on since, which is correct. When the first
attempt was answered `outcome_unknown`, or not at all, the replay is the `200`
that attempt never delivered. Only an answer that wrote is replayed: a
`409 duplicate_key` or a `400` is not remembered.

### The derived `_id`

A keyed insert without `_id` (an insert, each document of a bulk, a
`find_and_modify` upsert's seed) gets an `_id` **derived from the key and the
request** instead of a random ObjectId: a UUID version 8 (RFC 9562, built from
SHA-256 over the collection, the key's scope, the request's fingerprint and the
document's position in the batch), stored as its canonical 36-character
**string**. A retry of the same request derives the same `_id`, so two
applications of one insert on two members are two versions of one document,
and last-writer-wins keeps one. A key reused with a different request derives
a different one, so it never replaces a document another request wrote.

**It is a string beside ObjectIds.** A collection written with and without keys
holds both types of `_id`. Comparisons stay within a type, so an `_id` range
over ObjectIds (paging by time order, `{"_id": {"$gt": {"$oid": …}}}`) silently
excludes the string ids, and in `_id` order the strings sort before every
ObjectId. `$toDate` does not apply to them. A by-id route addresses one as any
string `_id` (`docs/6f1c0e52-…`). Send your own `_id` if the collection needs
one type.

### How long a key is remembered

A key is remembered **`storage.oplog_retention_secs` after the write's last
commit**, 24 hours by default: the window in which the write itself is kept and
replicated. `GET /v1/version` publishes it as `idempotencyWindowSecs`, and it is
**each member's own**: members configured differently remember keys for
different times. After it, the same request is a new one; a keyed insert
without `_id` still lands on its derived `_id`, and answers `409 duplicate_key`
while that document stands.

### A retry on another member

The record travels with the write's entries, so **a member that has received
the write replays it** like the member that took it. A member that has **not
received it yet cannot know the key**, and applies the request a second time,
answering without the marker. That window runs from the first member's commit
until the member the retry reaches has pulled the write, directly or through
another member: at least one `cluster.sync_interval_secs` while the first member
serves, longer in a large cluster or one whose rounds against it fail, and for
as long as the first member is down or cut off, which is the usual state right
after a failed fsync. **There is no wait after which a retry elsewhere is
safe.** What the second application does:

- **By id** (insert, bulk, replace, delete by id) it targets the same documents
  and leaves no duplicate; an insert without `_id` uses the derived `_id`.
- **By filter** (`update`, `delete`, `find_and_modify` and its upsert) it
  targets what the filter selects on the member it reaches, which can be
  different documents, and then both changes stand.
- Where both applications started from the same version of a document,
  last-writer-wins keeps one of them, which is the change made once: a `$inc`
  lands once. Where they did not (a later write the second member had not
  seen), last-writer-wins decides as for any concurrent write, and that later
  write can be lost.

**To stay protected, retry on the member that took the write, once it serves
again.** Every client's member selection is sticky already
([Clients](clients.md)). Retrying elsewhere accepts the above, and is the only
choice if that member does not come back.

### The gate

A member takes the header only while **its `cluster.idempotency_keys` is set
and every live member carries records**. Otherwise every keyed route refuses
it with `400 bad_request`, naming the header and the cause: the switch when it
is off, otherwise the live member that does not carry records (a member of an
earlier release, or one not heard from yet). The refusal comes after
authentication, so a caller with no token is answered `401` and learns nothing
of the cluster. The member then does not list `idempotency-keys`. The switch
defaults to `true` with clustering off, so a
single node takes keys with no setting, and to `false` with clustering on: a
cluster takes them once its operator has set it, after every member runs this
release ([Operations](operations.md#clusteridempotency_keys-and-the-procedure)).
During a roll the members of this release refuse the header; branch on the
capability per node, as for every capability. A client that read
`idempotency-keys` and then meets this `400` has met a roll, a rollback or a
change of the switch in between. A refused retry is not lost: a record written
before the gate closed stays for its window, and the retry replays from it once
the gate opens again.

### What is not guaranteed

- Nothing after the key's window ends.
- Nothing on a member of an earlier release, which ignores the header.
- Nothing across collections or principals: a key is scoped to both.
- Nothing for schema changes, vectors, webhooks or administration, which refuse
  the header.
- Nothing about the change stream and webhooks, which carry each application
  of a request as its own event.
- Once the switch is set, **rolling any member back to an earlier release
  reopens the gap** it closes: that member relays keyed writes without their
  records, and a retry on a member that pulled one through it can apply on top
  of the first application.

---

## Databases and collections

```bash
curl localhost:7878/v1/databases -H "$A"
curl localhost:7878/v1/db/shop/collections -H "$A"
curl -XPOST localhost:7878/v1/db/shop/collections -H "$A" -d '{"name":"orders"}'
curl -XDELETE localhost:7878/v1/db/shop/coll/orders -H "$A"
curl -XDELETE localhost:7878/v1/db/shop -H "$A"           # every collection in it
```

Databases are created implicitly by their first collection and removed
implicitly with their last: dropping the last collection takes the database
out of listings on every member, because the drop replicates and the
decision is made where it is applied. `DELETE /v1/db/{db}` records the
database's drop first, as a tombstone of its own that replicates, and then
drops each collection the member holds in turn (`ddl` over the database);
system databases (`__…`) are refused, and a database this member does not hold
answers `{"dropped": false}` and records nothing.

**Creating a name again soon after dropping it can be refused, for a while.**
A drop answers before what the collection held is removed (below), and a
collection created under the same name would stand over those rows, so
`POST /v1/db/{db}/collections` answers **`503 collection_purging`** with
`retry: wait` and a `Retry-After` in seconds until the removal is done. The
refusal is immediate, writes nothing, and moves that removal to the front, so a
client that waits and retries succeeds; how long it takes depends on how large
the dropped collection was. Enabling vectors on a collection whose previous
vectors were dropped with `drop_vectors=true` is refused the same way until
they are removed ([ADR-189](decisions.md)).

**A database drop is cluster-wide, and it reaches collections the member
answering it does not hold.** The drop is stamped. On every member, every
collection of the database created at or before that stamp is dropped, with the
documents written into it at or before the stamp, **including one created on
another member that this member had not received**, and no collection created at
or before it can be created. A collection created after it stands. **A write
stamped after the drop is kept**, as it is for a collection's drop (below; since
0.50.0, [ADR-236](decisions.md)). (Before 0.47.0 a drop covered only the
collections the member held when it was applied, and a collection it had not
received brought the database back, on every member, with that collection in
it. [ADR-229](decisions.md).)

**Issue a database drop once, on one member, and let it replicate.** Sending
the same `DELETE` to every member is no safer: each is a drop of its own, and a
vector-enabled collection's shadow, which the owning member builds locally and
which replicates a moment behind, can still reach a peer between its drop of the
parent and its drop of the database. Drop on one member and confirm the drop on
the others (a drop is not instant — see [Operations](operations.md)); do not
drop on each.

**After a database drop, recreate it on the same member, or wait until every
member lists it gone** (`GET /v1/databases` on each) before creating it anywhere
else. A creation is judged by the stamp it was made at, and a member that has
not yet received the drop may stamp a creation before it, when its clock is
behind the dropping member's. Such a creation is dropped with the database. The
documents written into it are judged by their own stamps, as for a collection's
drop: those stamped at or before the drop go with it, and those stamped after it
are kept, so the collection, and the database, come back on every member holding
only them. The window is the replication lag, the clock skew between the members
and the time the drop takes to bury its collections; it closes once the creating
member has received the drop. **A write acknowledged into a collection on a
member that has not yet applied a database drop is kept when it is stamped after
the drop, and brings the collection back** (since 0.50.0,
[ADR-236](decisions.md); 0.47.0 to 0.49.x lost it with the collection on the
member that held the collection, and kept it on a member that held a newer one
of the name, so the members could stay different). A member that is behind
longer than `storage.tombstone_retention_secs` can still bring the database back.

**Dropping a collection on one member and creating the same name through
another before the drop has replicated converges since 0.49.0**
([ADR-231](decisions.md)). The two are concurrent, and the member that made the
second life applied the first life's documents before it learned of the drop.
When the drop reaches it, it removes every document stamped at or before the drop
from its own life and keeps the ones the life itself wrote, so all members hold
the same documents. In 0.48.x the members stayed different for good (counts that
differ, `kimmy_sync_divergent_collections` above 0, and a repair that re-ran every
few seconds and changed nothing). The advice for databases above still stands as
the way to avoid the window altogether. The indexes and the vector configuration the first life had made are removed
from the newer life the same way. A member that stayed divergent past
`storage.tombstone_retention_secs` is not healed by the start's sweep, which
finds its work through the tombstones a member holds.

**A write made after a collection's drop is kept, and the collection can come
back (since 0.49.0, [ADR-231](decisions.md)).** A write stamped after the
drop's stamp, made through a member that still holds the collection and has not
yet applied the drop, is kept: when the member applies the drop it removes what
is stamped at or before it and keeps the later writes, and every member ends
with the same documents. In 0.48.x the member lost the write with the
collection while the members whose collections were newer kept it, and the
members stayed different. Two things follow, and both are intended:

- **A collection can reappear after its drop.** If nobody created the name
  again, the member that kept the write holds a collection the others dropped,
  and the others take it back, so it ends on every member holding only the
  writes stamped after the drop. A client that drops a collection and then
  writes to it through another member before that member has applied the drop
  can bring it back. Do not write into a collection through a member that has
  not yet received its drop; confirm the drop on the members first.
- **A delete counts as a write** (since 0.50.0, [ADR-236](decisions.md)). If the
  only thing written after the drop was a delete, the collection comes back holding
  no document, with the delete, so a document the delete removed cannot be written
  back by a peer that had not yet heard it.
- **Clock skew can cause it.** "After" is decided by the stamp, not by real
  time. A write made *earlier* in real time by a member whose clock runs
  *ahead* of the dropping member's is stamped after the drop and counts as
  "after" if it reaches a member that has not applied the drop.

A **database** drop does the same since 0.50.0 ([ADR-236](decisions.md)): a
collection created at or before it keeps what was written into it after the
drop's stamp, and can come back with it, database and all. (0.49.x buried such a
collection with the writes made into it afterwards.) While
a member removes what the drop covers (a thousand records per commit, bounded
by the chunk) a read of the collection still shows documents stamped before the
drop and counts them; the change streams open on the collection end when the
drop reaches it, and a stream opened afterwards without a resume token may
begin at a document's latest version rather than at the insert that created
it, and may not hear a definition (an index created, say) that arrived before the
first document the life kept.

**A drop is not atomic in one transaction, and it is atomic to a reader.**
The collection's definition and its tombstone go in the drop's first commit and
everything it held is removed after that, a thousand rows per commit, so the
member's other writes are not held behind it ([ADR-158](decisions.md), and
[Operations](operations.md) for the cost). From that first commit the
collection is **gone** to every client and every peer: `GET
/v1/db/{db}/collections` does not list it, reads and writes to it answer as
they do for a collection that never existed, and there is no window in which it
is served intact and short of documents. **The `DELETE` answers at that first
commit**, in milliseconds whatever the size, and what the collection held is
removed afterwards by the node's drop purger ([ADR-189](decisions.md)); a
client timeout no longer needs sizing for the drop. Issue it once rather than
retrying, since a retry answers `{"dropped": false}` for the drop that is
already done. Two drops of one collection sent at once to one member are the same
case: one answers `200 {"dropped": true}`, the other `200 {"dropped": false}`,
and only the first replicates. A member restarted part-way through a removal
finishes it once it is serving again; the drop itself was durable and
replicated before the first row went.

**A drop stays dropped.** It is applied where it lands, it replicates as a
change like any other, and it is not undone by the anti-entropy that converges
the cluster: while the tombstone is retained, a member that has applied the
drop neither reports the collection as missing against a peer still holding
it, nor lets a repair from that peer recreate it or write into it
([ADR-155](decisions.md)). So the interval between `200 {"dropped": true}` on
one member and the drop landing on the rest is a wait, not a race — the peers
converge on the drop, not on the copy. One route in a converged cluster is
outside that guarantee and is recorded rather than closed: a member so far
behind that it catches up by copying a peer's *whole database* rather than by
replaying changes is not told about drops that way, and keeps a collection it
already held (ADR-155 states what follows from that). During a rolling upgrade
there is a second and temporary one, for a member not yet on the release that
carries this. Past `storage.tombstone_retention_secs`
the tombstone is collected and the ordinary resurrection case applies, the same
one a deleted document is subject to ([ADR-085](decisions.md)); the retention
setting is the width of that window and [Operations](operations.md) says how to
size it.

Listing responses
are **filtered by what the caller may read**, so they cannot be used to discover
objects you have no access to.

Names may not be empty, exceed 120 bytes, start with `__` (reserved for system
objects), or contain `/`, `\`, `$`, spaces, or NUL.

---

## Users

```bash
curl -XPOST localhost:7878/v1/users -H "$A" -d '{
  "user": "analyst",
  "password": "a-good-password",
  "grants": [ { "db": "shop", "collection": "orders*",
                "actions": ["read", "watch"] } ]
}'
```

```bash
curl localhost:7878/v1/users -H "$A"                       # list
curl localhost:7878/v1/users/analyst -H "$A"               # inspect
curl -XDELETE localhost:7878/v1/users/analyst -H "$A"
curl -XPOST localhost:7878/v1/users/analyst/password -H "$A" -d '{"password":"new-password"}'
curl -XPOST localhost:7878/v1/users/analyst/grants   -H "$A" -d '{"grants":[…]}'
curl -XPOST localhost:7878/v1/users/analyst/roles    -H "$A" -d '{"roles":["reader"]}'
```

### Roles

A role is one named set of grants that both local users and federated
principals can point at, instead of the same permissions being copied onto
every user record. Role grants are **added to** a user's direct grants — the
effective permission is the union of the two, never a replacement.

Editing or deleting a role revokes the live tokens of every local user holding
it, and the response says how many accounts that was. Without it a *narrowing*
edit would take effect only as each token expired. Federated principals need no
such revocation: their grants are resolved from the role store on every
request, so an edit applies to them on the next call. What stays stale for them
is role *membership*, which is frozen in the provider's token until it expires.

Deleting a role leaves its name on holders' records, where it resolves to
nothing.

```bash
curl -XPOST localhost:7878/v1/roles -H "$A" -d '{
  "name": "reader",
  "grants": [{"db":"sales","collection":"orders*","actions":["read","search"]}]
}'

curl localhost:7878/v1/roles -H "$A"                 # list
curl localhost:7878/v1/roles/reader -H "$A"          # inspect
curl -XPOST localhost:7878/v1/roles/reader/grants -H "$A" -d '{"grants":[…]}'
curl -XDELETE localhost:7878/v1/roles/reader -H "$A"
```

Passwords must be at least 8 characters. Password hashes are never returned. A
user may change their own password without admin rights. The **last remaining
user cannot be deleted**, and you cannot delete the account you are signed in
as — either would leave the server unadministrable.

Grant semantics: [Security](security.md).

---

## Indexes

```bash
curl -XPOST localhost:7878/v1/db/shop/coll/orders/indexes -H "$A" -d '{
  "fields": [ { "path": "item" }, { "path": "qty", "descending": true } ],
  "unique": false,
  "name":   "item_qty"
}'

curl          localhost:7878/v1/db/shop/coll/orders/indexes -H "$A"
curl -XDELETE localhost:7878/v1/db/shop/coll/orders/indexes/item_qty -H "$A"
```

`fields` is an **array**, not a `{field: 1}` object: field order decides which
queries a compound index can answer, and JSON object key order is not something
a client can rely on its own encoder to keep. The server keeps whatever order
arrives (see [The JSON boundary](#the-json-boundary)); the array is insurance
against the client side, where a language with unordered maps may not.

A duplicate against a `unique` index returns **409 `unique_violation`**. Setting
`"enforcement": "coordinated"` returns **501** until clustering lands — a
`local` unique index is a single-node guarantee. See [Indexes](indexes.md).

**A document without the indexed field indexes as `null`**, so a unique index
builds without complaint over documents that lack the field, and the *next*
document that lacks it collides on `null` — `409 unique_violation` on an
insert that named no such field. To constrain only the documents that carry
it, create the index partial:
`"partialFilterExpression": {"email": {"$exists": true}}`
([partial indexes](indexes.md#partial-indexes--indexing-only-some-documents)).

Creating an index under a name the collection already has is **idempotent
when the definition is identical** and **409 `conflict`** when any part of it
differs — the field list, `unique`, `expireAfterSeconds` or
`partialFilterExpression` — with the message naming which part moved. Drop
the old one first, or choose another name; the server never silently keeps
the definition it had.

Across nodes a collision is detected when the replicated write is merged, not
prevented, and both documents stay. `GET …/violations` lists what still
stands — counts per index, or with `?index=<name>` the colliding groups with
their documents — so the application can choose; a document deleted or
rewritten out of the collision drops out of its group, and a group with one
member left drops out of the report
([resolving a unique violation](indexes.md#resolving-a-unique-violation)).
`?index=<name>` for an index with nothing standing — or a name no index on
the collection has — is `200 {"index": "<name>", "count": 0, "groups": []}`,
not a 404: the question was what collides there, and the answer is nothing.

Add `"explain": true` to `find`, `count`, `update` or `delete` to see whether
an index was used (and to `aggregate` to see its plan without running it: below):

```json
{ "explain": { "strategy": "index", "index": "qty_1", "indexFieldsUsed": 1,
               "documentsExamined": 10, "documentsMatched": 10 } }
```

`strategy` is `collectionScan`, `index`, `indexUnion` (a `$in` union of
probes) or `idLookup` (the filter pinned `_id`, answered through the primary
key with no index). Treat an unrecognized value as an access path this client
does not know about — new names are additive. `indexEntriesRead` appears when
an index answered a read: how much of the index was touched, as distinct from
how many documents were examined — an equality stopped by `limit` reads as
many entries as it returns, a range put in `_id` order reads the whole range.

`count` visits every match and holds none of them: its cost is the time of
the scan, not the memory of the result.

**On `aggregate`, `explain: true` plans and runs nothing**
([ADR-221](decisions.md), [Aggregation](aggregation.md#performance)). It answers
`{"explain": {"executed": false, "source": ..., "stages": [...]}}` with no
`documents` and no `count`, and without `documentsExamined`, `documentsMatched` or
`indexEntriesRead`, which are what a scan found. It is the one `explain` that does
not execute the read it describes. `source` carries the same `strategy`, `index`,
`indexFieldsUsed` and `probes` as a `find` over the leading `$match`. A
non-boolean `explain`, `null` included, is `422`, and so is any other field the
route does not define; `?explain=true` is `400`, as on any route with no query
string. It needs the access a run needs, `$lookup` collections included.

**On `update` and `delete`, `explain: true` plans the write; it does not
perform it** ([ADR-131](decisions.md)). It answers with the same
`documentsExamined`/`documentsMatched`/`strategy` a real write would use, but
writes nothing and spends no commit: `matched`, `modified`, `deleted` and
`commits` all read `0`, and `stamp` is absent — exactly what those fields
already say for a write that matched nothing. What the write *would* touch
is `explain.documentsMatched`. Drop `explain` to perform the write the plan
described.

**On `update`, that count is the selection, not a guarantee.** `explain`
plans which documents match and how they are found; it never runs the
update operators, so the real write can still refuse a document `explain`
reported as matched — a `$inc` on a field holding a string, for instance,
answers `400` on the write and `200` with that document counted on the
plan.

**`explain` cannot be combined with `if_stamp`, and is refused `400` with
it.** A plan never checks a document's version — that check only happens
inside the write `explain` does not perform — so a plan cannot honestly
answer "would this write happen" for a conditional write: the document it
reports as matched may be exactly the one the real write, checking the same
stamp, refuses `409 stale` on.

---

## Change streams

```
GET /v1/db/{db}/coll/{coll}/watch      (WebSocket upgrade)
    ?resume_after=<token>
    &from_start=true
    &full_document=true
```

Full detail in [Change Streams](change-streams.md).

---

## Health and metrics

```bash
curl localhost:7878/healthz    # {"status":"ok"}
curl localhost:7878/readyz     # {"status":"ready","node":"9d5200f2-…","decimal128_id_present":false}
curl localhost:7878/metrics    # Prometheus text format
```

All three are unauthenticated so a load balancer can probe them without
credentials. `/healthz` is liveness; `/readyz` proves the storage engine
actually responds, so a node with a wedged database is taken out of rotation.
A ready answer carries `decimal128_id_present`: whether a document whose `_id`
is a `Decimal128` has ever been written or applied on this member, the marker
a rollback reads ([Operations](operations.md#clusterdecimal128-and-the-marker)).

**A node answers these from the moment it binds its port, before its store is
open** ([ADR-198](decisions.md)). Until the store is open `/healthz` is `200`
and `/readyz` is `503` in the error envelope (`error: starting`, `retry:
elsewhere`) with `status: opening`, `phase`, `phase_age_seconds`, and `done` and
`total` when the phase counts; every other route, `/v1/version` and `/metrics`
included, answers `503 starting`.

**A member that knows it is behind is not ready** ([ADR-202](decisions.md)). A member
that created its store in a cluster it has seeds for keeps a marker until it has caught
up, and while it is set `/readyz` is `503` in the error envelope (`error: catching_up`,
`retry: elsewhere`, and `reason`: the state) and every route but `/healthz`, `/readyz`, `/metrics` and
`/v1/topology` answers `503 catching_up` too, `/v1/version`, a login, `/mcp` and a
WebSocket upgrade included. Every response of a member whose marker is set carries
`x-kimmy-catching-up: <state>` (`seeded_empty`, `restored`, `snapshot` or `unknown`).
Past `cluster.catch_up_wait_secs` with no peer reached that could say whether it is
behind, the member serves as `unknown`: `/readyz` is `200` and carries
`"catching_up": "unknown"`. It also serves as `unknown`, rather than clearing, while a
member the cluster lists live still owes it the replay of its own origin past that
wait ([ADR-212](decisions.md)). Beside `catching_up`, `/readyz` then carries
`unknown_because`, `"no_counting_peer"` or `"owed_replay"` (the second when both hold),
and `owed_members`, the node ids of the members that owe the replay, sorted (never
empty under `owed_replay`):

```json
{"status":"ready","node":"9d5200f2-…","decimal128_id_present":false,"catching_up":"unknown",
 "unknown_because":"owed_replay","owed_members":["4c1e07a9-…"]}
```

The two differ in what comes next: `no_counting_peer` returns to `503` when a peer that
is ahead is reached, `owed_replay` does not, and either returns to `503` when the replay
proves this member lost writes. While a member is gated and members owe it the replay,
the `503` carries `owed_members` beside `reason`. `GET /v1/topology` marks the member (`catchingUp` with the
state) and a peer that says it is catching up (`catchingUp: true`).

Metrics deliberately expose **counts only** — naming collections there would
leak your schema to anything that can reach the port.

---

## Protected resource metadata

When the node federates with an identity provider **and** `auth.oidc.audience`
is written as an `https` URL, the node publishes what RFC 9728 calls protected
resource metadata — its own name as an OAuth 2.0 resource, and the
authorization server that speaks for it:

```bash
curl localhost:7878/.well-known/oauth-protected-resource
```

```json
{
  "resource": "https://kimmydb.example.com",
  "authorization_servers": ["https://auth.example.com"],
  "bearer_methods_supported": ["header"]
}
```

Unauthenticated, necessarily: a client that has no token is exactly who needs
it. It is what lets `kimmy login --url https://kimmydb.example.com` work
with nothing else configured (the `kimmy` CLI is not currently distributed;
see [cli.md](cli.md)), and it is how a conformant MCP client discovers where
to authenticate — see [mcp.md](mcp.md).

`scopes_supported` is deliberately **absent**. Authorization here is roles
carried in the token, not scopes; advertising a scope vocabulary would describe
an access-control model this database does not implement.

**404 when the node has no such name.** An `auth.oidc.audience` that is an
opaque string (`kimmydb`, or Entra ID's `api://<guid>`) is a supported
configuration, not a broken one — there is simply nothing truthful to publish,
and an identifier no token will ever carry would send every client to ask its
provider for a resource the provider refuses.

RFC 9728 §3 inserts the well-known segment between the host and the path, so a
resource identifier of `https://kimmydb.example.com/nodes/one` publishes at
`/.well-known/oauth-protected-resource/nodes/one`. A request for any other
suffix is a 404 rather than this node's document.

### Refusals carry a challenge

Every 401 and 403 answers with `WWW-Authenticate`, as RFC 6750 §3 requires:

```
401, no credentials offered:  Bearer realm="kimmydb", resource_metadata="…"
401, bad or expired token:    Bearer realm="kimmydb", error="invalid_token", …
403, authenticated but denied: Bearer realm="kimmydb", error="insufficient_scope", …
```

The first two differ on purpose. `invalid_token` tells a client to refresh and
retry, which is the wrong advice for one that has not tried yet — so a request
that offered no credentials is told only *how* to authenticate, never that it
failed.

`resource_metadata` appears only when there is a document to point at. The 403
challenge reveals nothing the body does not: it is byte-identical whether the
target exists or not, which is the same property the uniform 403 has always
had.

The `error_description` on a 401 is deliberately generic — `the access token
is expired, revoked or malformed` — with no exceptions. A more specific
description was carried for one refusal and removed with it
([ADR-112](decisions.md)); every 401 on the token path now says the same
thing.

`POST /v1/auth/login` is exempt. It is where a token comes from, not a
bearer-protected resource, and challenging there would tell a client to come
back with the thing it is asking for.

---

## The JSON boundary

Documents are stored as BSON. JSON cannot express several BSON types, so the
edge uses **Extended JSON v2**:

| BSON type | JSON form |
|---|---|
| ObjectId | `{"$oid": "6745f2a1b3c4d5e6f7081920"}` |
| DateTime | `{"$date": 1700000000000}` or RFC 3339 |
| Int64 | `{"$numberLong": "9007199254740993"}` |
| Decimal128 | `{"$numberDecimal": "1.25"}` |
| Binary | `{"$binary": {"base64": "…", "subType": "00"}}` |
| MinKey / MaxKey | `{"$minKey": 1}` / `{"$maxKey": 1}` |

Plain JSON keeps working for anything expressible in it — you only meet this
when you need a type JSON lacks.

**A Decimal128 is stored and returned as sent, and compared by its exact
value.** `$numberDecimal` is read on input and written on output, and the
digits come back exactly as sent. It is an index key like any number, keyed by
its exact value, so an index answers a range over it and a unique index
enforces on it ([Key encoding](key-encoding.md#numbers-the-hard-part)); a
partial index's filter decides it by value, and a partial unique or TTL index
enforces and expires by that decision
([Indexes](indexes.md#a-decimal128-at-a-filtered-path)). It can be an `_id`
under the gate described [above](#get-replace-delete-by-id). As an operand it is a number like any other: `{"v":
{"$numberDecimal": "1.5"}}`, and a Decimal128 under `$eq`, `$ne`, `$gt`,
`$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$all`, inside a document or array
literal, or as a literal in an expression, `$expr` included, matches the
values equal to it in any numeric type and spelling, and orders among them by
value; `0.1` the decimal is not `0.1` the double. A sort places a stored one
among the numbers, and the update operators that compare their operand —
`$min`, `$max`, `$pull`, `$pullAll` — compare it by value
([Query language](query-language.md#3-comparisons-do-not-cross-type-groups)).
Until 0.51.0 each of these was refused, `400`, because the canonical order
ranked a Decimal128 equal to every other number; for the same reason it could
not be an index key or an `_id` (a document holding one at an indexed path was
filed under the index's unkeyed run, and one at a partial filter's path in an
undecidable run that the listing's `undecidable` and `explain`'s
`undecidableCandidates` counted: both fields are removed in 0.51.0, a breaking
change for a typed client that declared them).
Through 0.24.0 the edge did not read `$numberDecimal` at all: the wrapper
arrived as a nested document, which is why a Decimal128 written to a member
was keyed there and filed unkeyed on its peers ([ADR-139](decisions.md)).

**The `$numberDecimal` text is a number, read exactly, or a `400`.** The
syntax is the General Decimal Arithmetic specification's, and nothing else: an
optional sign, digits with at most one `.` and at least one digit (`1.50`,
`.5` and `1.` are numbers; `.` is not), an optional exponent of `e` or `E`, an
optional sign and digits (`1E+3`, `25e-1`); or, in either case, `Infinity`,
`Inf`, `NaN` or `sNaN`, a NaN with an optional payload of up to 33 digits. No
white space, ASCII digits only. The value is never rounded: a nonzero digit
past the 34th, a value below `1E-6176` or with a digit below it, and one above
`9.999999999999999999999999999999999E+6144` are refused, each with its reason.
An exact value keeps its written exponent where it fits, and otherwise moves
only as far as the format requires (`1.0000000000000000000000000000000000000`
is stored with 34 digits, `1E+6144` as `1000000000000000000000000000000000E+6111`,
`0E+9999` as `0E+6111`). A refusal names the text: `invalid $numberDecimal:
"": not a decimal number: …`. **Until 0.51.0** the edge used the `bson`
crate's parser, which took `""`, `"."` and `"-"` as zero, `"++1"` as `1`,
`"0+5"` as `5` and `"e100"` as `0E+100`; each is now refused.

**A binary `_id` is echoed with its subtype, but keyed without it.** The ids
a write or a report answers with (`insertedId`, `insertedIds`, `upsertedId`,
and the `ids` and `merged` of `GET .../violations`) are the `_id` as stored,
subtype included, so `find {_id: <echo>}` finds the document. Two things
still carry the subtype folded to `00`:
- **A change stream's and a webhook's `documentKey`, and a `uniqueViolation`
  event's `documentKeys` and `merged`.** A delete's entry carries no body, so
  its key cannot carry the subtype, and an insert's `documentKey` that did
  would never match its later delete's. Match a binary `documentKey` on its
  bytes, or read the subtype from `fullDocument`.
- **The document key.** A binary `_id` is keyed by its length and bytes only,
  so `BinData(0, X)`, `BinData(3, X)` and `BinData(4, X)` are one document: a
  second insert of the same bytes under another subtype is refused
  `duplicate_key`, and writes of the two on two members are merged into one
  document, last writer wins. **Do not mix subtypes for one value**: write a
  UUID in one representation everywhere.

**Whole numbers stay integers.** A JSON `42` becomes `Int32`, not `Double`.
Widening would break `$type` queries and lose precision above 2^53; `2^53 + 1`
round-trips exactly, and there is a test pinning it.

**Non-finite doubles** come back as `{"$numberDouble": "NaN"}` rather than
`null`, so a number never silently becomes a missing value.

**Every request body is closed.** A field the route does not define — a
misspelt `limitt`, an `explainn` on `aggregate`, anything the route's section
does not list — is refused `422 bad_request`, and the message names the field
and lists the ones the route takes ([ADR-121](decisions.md)). Nested shapes
are closed too: an entry in an index's `fields`, a search's `weights`, a
vector configuration's `provider`, a grant inside a user or role body — where
a misspelt `collection` used to default to `*` and widen the grant. A document
body is content, not a shape: insert, replace and bulk take any field, because
`limit` and `filter` are perfectly good names for a document's own fields.

**An explicit `null` on a declared *optional* field is refused `422
bad_request`, not read as absent.** An optional field you omit still means
what it always meant; the same field sent as `"field": null` is refused by
name, in the same envelope and message shape as any other wrong-typed value.
`Option<T>`'s ordinary deserialization cannot tell "you never mentioned this"
from "you sent `null`", and the two are not the same request:
`{"if_stamp": null}` on `update`, `delete` or `find_and_modify` is not the
same as leaving `if_stamp` out, and `{"filter": null, "multi": true}` on
`delete` is not the same as `{"filter": {}, "multi": true}` — the latter is
the documented, deliberate way to match every document; the former is
refused rather than silently meaning the same thing ([ADR-128](decisions.md)).
This is about a request shape's own declared fields, not what is inside
them: a filter, an update operator's operand, or a stored document may hold
a genuine `null` value anywhere, and none of that is touched by this rule.
A *required* field sent as `null` was already refused before this rule
existed — it fails its own type, downstream of the field being present at
all — but by whatever status and message that field's own validation
answers with; `update`'s `update` on `POST .../update` is `400 "expected a
JSON object"`, for instance, not this `422`. This rule is what closes the
one gap: an *optional* field's `null`, which nothing refused before.

**One nested shape does not name the field inside it: a tagged `provider`
object.** `{"provider": {"kind": "open_ai", "endpoint": null, …}}` is
refused, but the message reads `"provider: invalid type: null, expected a
non-null value"` — it names `provider`, not `provider.endpoint`. This is not
particular to `null`: `{"provider": {"kind": "open_ai", "dimensions":
"nine"}}` is refused the same truncated way, and always has been. Once the
`kind` tag is matched, the value is re-read through a second, unrelated
deserializer, and the path tracker that otherwise names a nested field —
`chunk.max_tokens` reports its full path correctly, being an ordinary struct
rather than a tagged one — cannot see across that seam. The status and the
refusal are both right; only the name is short.

**An aggregation stage operand is closed too, but at `400`, not `422`.**
`$unwind`'s document form (`path`, `preserveNullAndEmptyArrays`,
`includeArrayIndex`), `$lookup`'s both forms, and `$replaceRoot` refuse a key
they do not define, naming it — the same closure, deliberately at a different
status ([ADR-129](decisions.md)). The `422` above is `JsonBody<T>`'s status
for a *serde-typed* shape the request extractor itself rejects; a pipeline's
`pipeline` array is untyped JSON at that layer — which stage a document
names, and therefore which keys are legal, is not decided until
`kimmy-query` parses it — so its refusal lands where every other
malformed-pipeline error already does: `400 bad_request`, the same status
`$group`'s unknown-accumulator refusal has always used. `$match` filters and
`$project` specifications are **not** closed: every key in either is a
document field name the caller chose, not vocabulary this server defines.

**Query strings are held to the same rule, at `400`.** `?limt=5` on
`GET .../docs`, or `?limit=abc`, is `400 bad_request` with the parameter
named. The status differs from a body's `422` because a query string is part
of the request line rather than an entity the server could not process; the
envelope is the same. A route that takes no query parameters — every route
other than `GET .../docs`, `PUT` and `DELETE .../docs/{id}`,
`GET .../describe`, `GET .../violations`, `DELETE .../vector` and
`GET .../watch` — refuses *any* query string at all, `400 bad_request`
naming the first parameter and saying the route takes none
([ADR-124](decisions.md)). So `if_stamp` on `update`, `delete` and
`find_and_modify` is a body field a query string can never carry:
`POST .../update?if_stamp=…` is refused rather than read as an unconditional
write. A query string that names **nothing** is not refused: a bare `?`, and
one made only of `&` separators — `?&`, `?&&&` — pass on every route,
because there is no parameter in either to reject and a client that appended
an empty one has asked for nothing. `?=` and `?;` do name something — an
empty name, and a parameter whose name is `;` — and both are refused, the
first as a malformed query string and the second as an unknown parameter,
each saying the route takes none ([ADR-124](decisions.md)).

**Object key order is preserved** through the boundary and into the stored
document: `{"zeta": 1, "alpha": 2}` is stored, indexed and read back with
`zeta` first, as BSON keeps it, and the same holds for the keys of a filter,
sort or update document, where the order carries meaning — a sort document's
first key is its primary key, and update operators apply in the order they
arrive, which is the order the fields they add are appended in (ADR-120). Two
that write one path are refused, not ordered
([ADR-205](decisions.md)). An inclusion projection answers in the document's order,
not the projection's. **An update keeps the order it found and appends what
it adds:** a `$set` of a field the document already has rewrites it in place,
and a `$set` of a new one puts it last, so `[_id, zeta, alpha]` becomes
`[_id, zeta, alpha, beta]`. `_id` comes first because that is where a stored
document keeps it, whatever position it held in the body that wrote it. This
is the server's end of the wire; the other end is the client's JSON encoder,
and the index route's `fields` array exists because that end is the one a
client cannot always vouch for (see [Indexes](#indexes)).

---

## Tokens

```bash
curl -XPOST localhost:7878/v1/auth/login -d '{"user":"root","password":"…"}'
```

```json
{ "token": "eyJhbGciOi…", "user": "root", "expiresIn": 3600 }
```

`expiresIn` is seconds. A token is **opaque** — do not decode it to find the
expiry; that shape is not promised.

```bash
curl -XPOST localhost:7878/v1/auth/refresh -H "$A"
```

Returns the same shape with a fresh token. **Sliding re-issue, not a second
credential**: there is no refresh token to store, and an application using the
API never re-sends its password. One idle longer than `expiresIn` logs in
again.

Three properties worth stating plainly:

**Refresh cannot revive a revoked session.** The presented token goes through
the same check every route does, so an account deleted, disabled, or whose
password or grants changed is refused *before* refresh runs
([ADR-052](decisions.md), [ADR-059](decisions.md)). A grant change therefore
means logging in again, which is the deliberate cost of grants being carried
in the token.

**The old token keeps working until it expires.** Refreshing does not recall
it, because a stateless token cannot be recalled. To end a session early,
change the password or the grants, or delete the account — each bumps the
token version and invalidates every token that user holds.

**The new token carries current authority**, read from the user record rather
than copied from the old token's claims.

---

## Topology

```bash
curl localhost:7878/v1/topology -H "$A"
```

```json
{ "nodes": [
    { "node": "c88b8984-…", "endpoint": "http://10.0.0.5:7878",
      "version": "0.1.0", "status": "live", "self": true },
    { "node": "fa5b2a9e-…", "endpoint": "http://10.0.0.6:7878",
      "version": "0.1.0", "status": "unknown", "self": false } ],
  "count": 2, "view": "c88b8984-…" }
```

**Every node accepts writes**, so client-side selection is sticky plus retry
elsewhere: a client keeps using the node that last answered and fails over only
when one stops answering. There is no primary to find and none of the machinery
a driver needs to find one — which is the one thing this is straightforwardly
better at than a replica-set driver, rather than merely different.

Sticky rather than round-robin is a deliberate choice, and it is worth knowing
which one you have: it keeps a connection and a page cache warm, and it also
means one node serves everything until it fails rather than the load being
spread. This endpoint gives a client somewhere to *go*, not a rotation to follow.

Two sources, deliberately ([ADR-060](decisions.md)):

- **Where** a node is comes from a replicated registry each node writes itself
  into. Addresses are not in SWIM: its identity carries the *gossip* address,
  and it is postcard-encoded, so adding a field there is a stop-the-cluster
  upgrade.
- **Whether** it is there comes from SWIM membership.

`status` is `live` or `unknown`, never `down`. A node whose gossip is
partitioned while its HTTP is perfectly reachable is a real state here, and
`unknown` is the honest word for "this node has not heard from it". Trying one
costs a round trip, and `retry: elsewhere` already covers the outcome.

**The answering node is always listed, marked `self`, and first** — a client
reading top-down should not be moved off the node already serving it.

**What each node yields.** `view` is the id of the node that answered, and
`yielding` and `classState` on each entry are what **that member sees** of the
node ([ADR-213](decisions.md)). A member that cannot do a class of work (TTL
expiry, webhook delivery, embedding) gives it to a peer that can, and says so in
its block. `yielding` lists the classes the node yields (`ttl`, `webhooks`,
`embeddings`); `classState` says how each class stands on it (`ok`, `idle`,
`suspect`, `stalled`, or `unknown`). For the answering node these are what its own
evaluator last decided; for a peer, what its last block said. They are present
only when there is something to say: for a peer from an older build `yielding` is
`[]` and `classState` is absent, and it is never a target for a yield. They are **advertised, not effective**: a member
keeps owning a class it yields until every live peer has confirmed it, so read
them as "where work is going", and compare members after a lease, never a single
answer. A client choosing where to send a request should still prefer `live`
nodes without `catchingUp` or `degraded`, and need not read these at all.

**`degraded`** ([ADR-220](decisions.md)) is present on a `live` node while that
node cannot serve as it should, as the answering node sees it: `"writer"` when its
single writer has been held for more than 60 seconds, so no write can be made and no
peer hands it work, and `"runtime"` when its async runtime stalled within the last
judged ticks. **Advisory:** writes and owner work on that node may stall, reads may
still be served, prefer a node without it. It says nothing of `catchingUp` or
`classState`. **A missing `degraded` is not "healthy"**: a peer of an older build, a
peer the answering node holds no block from and a member still starting have none.
When every node has it, use them anyway; it orders a preference and forbids nothing.
`status` is unchanged.

A node appears with a null `endpoint` when it has not been told what to
advertise. Set `server.advertise` to the URL clients should use; it cannot be
inferred, because a node bound to `0.0.0.0` has no single address and the
address clients reach may belong to a proxy. A node that guessed would publish
a wrong address to every client in the cluster.

Authenticated, unlike `/v1/version`: a version is a fact about software, this
is a map of where a deployment's data lives.

---

## Version and capabilities

```bash
curl localhost:7878/v1/version
```

```json
{ "protocol": "v1",
  "version": "0.51.0",
  "commit": "9f1c2ab",
  "node": "3e98120f-66df-4cf0-9fa0-690e3d57fcea",
  "capabilities": ["aggregation", "bulk-insert", "backup", "change-streams",
                   "client-supplied-vectors", "cursor-paging", "find-and-modify",
                   "hybrid-search", "partial-indexes", "token-refresh", "topology",
                   "ttl-indexes", "vector-search", "webhooks", "conditional-writes",
                   "local-embeddings", "idempotency-keys"],
  "durability": "durable",
  "idempotencyWindowSecs": 86400 }
```

That is the complete list, in the order the server emits it. Every entry but
two is constant for a build. `local-embeddings` is present on a build compiled
with that feature and absent otherwise, so a `local` embedding provider is
accepted on such a node and refused everywhere else. `idempotency-keys` is
present **while the member takes `Idempotency-Key`**, which can change while it
runs: its `cluster.idempotency_keys` is set and every live member carries
records ([Idempotency keys](#the-gate)). What each name entitles a client to is
tabulated under `Capability` in [`openapi.yaml`](openapi.yaml), and a test
holds the server's enum, that table and this list to one set.

`commit` and `durability` are fields, not capabilities. `commit` is the short
git commit the binary was built from — `unknown` for a build made without git,
which is a supported build — and is for the operator staring at a
half-upgraded cluster where two nodes claim the same `version`. `durability`
is how this node makes a commit durable, `durable` or `coalesced`; both are
durable when a write returns, and the value is a fact about the node's
configuration rather than its build, which is why it is not in the capability
list ([Storage](storage.md#durability-classes), [ADR-088](decisions.md)).
`idempotencyWindowSecs` is how long this node remembers an `Idempotency-Key`,
its `storage.oplog_retention_secs`, a field for the same reason
([Idempotency keys](#how-long-a-key-is-remembered)).

**Branch on `capabilities`, not on `version`.** A version number only answers
"can I use this" if the client also carries a table mapping versions to
features — the table this endpoint replaces. Nodes are upgraded one at a time,
so a client that fails over between nodes can reach an older node right after a
newer one; the answer describes *the node that answered* and is worth caching
per node.

Unauthenticated, so a client can negotiate before it holds a token. It names no
database, collection or user.

[Compatibility](compatibility.md) is the policy this serves: what `/v1`
promises, what counts as additive, and what a correct client must tolerate.

---

## Errors

```json
{ "error": "duplicate_key",
  "message": "duplicate key: document with _id 1 already exists",
  "retry": "no" }
```

The status carries the class of failure, `error` is what a client branches on,
and **`retry` is what a client library can act on without knowing the code**.
The set of codes is closed by an enum in the server ([ADR-057](decisions.md)),
so a new failure cannot appear without its retry class being decided in the
same commit. **The set of retry classes is open**: `no`, `wait`, `elsewhere`
and `verify` today, and a client treats a class it does not know as `no`.

| Status | `error` | `retry` | Cause |
|---|---|---|---|
| 400 | `bad_request` | no | Malformed filter, update, projection, or Extended JSON; a bulk batch over 1000 documents; an `Idempotency-Key` on a route that does not take one, a malformed one, or one while the member does not take keys, naming the header and the cause ([Idempotency keys](#idempotency-keys)); a query parameter the route does not define, or one it cannot parse or honour — `?sample=0` on `describe` ([ADR-121](decisions.md)); an `if_stamp` that is not a stamp the server issued; a `vector_search` or `hybrid_search` on a collection with **no vector configuration**, where the message names the `POST …/vector` route that enables it; **a key `$unwind`'s document form, `$lookup`, or `$replaceRoot` does not define, or a document whose `$unwind` path crosses an array** ([ADR-129](decisions.md), [ADR-130](decisions.md)) |
| 422 | `bad_request` | no | A body that is valid JSON but the wrong shape — an object where `/bulk` wants an array, a required field missing, a field the route does not define, or an explicit `null` on a declared *optional* field that does not accept one; the message names it ([ADR-121](decisions.md), [ADR-128](decisions.md)). A `null` sent for a *required* field is refused too, but by that field's own type — see [The JSON boundary](#the-json-boundary) — and is not always this status. **Not** a pipeline stage operand's unknown key, which is untyped JSON at this layer and is refused `400` once `kimmy-query` parses it (ADR-129) |
| 401 | `unauthorized` | no | Missing, malformed, invalid, or expired token; bad credentials; a token whose account was deleted, disabled, or had its password or grants changed ([ADR-052](decisions.md)) |
| 403 | `forbidden` | no | Denied by RBAC |
| 404 | `not_found` | no | Document, collection, or user absent. **A collection absent on a node that has peers answers `elsewhere` instead**: created through a load balancer, it lands on one member and reaches the rest a sync round later, and another member has it meanwhile |
| 409 | `conflict` | no | Collection exists; an index name already taken by a different definition; last user; self-deletion |
| 409 | `duplicate_key` | no | `_id` already present |
| 409 | `unique_violation` | no | A unique index would be violated |
| 422 | `idempotency_key_reused` | no | The request's `Idempotency-Key` is remembered on this member for a different request (another body, path, query or method) from this principal on this collection. Nothing was written. Use a new key for a new request ([Idempotency keys](#what-each-case-answers)) |
| 409 | `idempotency_key_in_flight` | wait | A `multi: true` update or delete with this `Idempotency-Key` is still running on this member. Nothing was written. Carries `Retry-After: 1`; sent again here once it has finished, it is answered from its record. Not elsewhere: a member that has not received the record would apply it a second time |
| 409 | `stale` | no | A conditional write's `if_stamp` did not match: the document is at another version, or is gone. Nothing was written — re-read, decide again, and send a new request with the current stamp ([conditional writes](#get-replace-delete-by-id)) |
| 503 | `collection_purging` | wait | Creating a collection, or enabling vectors, under a name whose earlier collection's drop is still being removed on this node. Nothing was written, and that removal was moved to the front. Carries `Retry-After` in seconds; the retry succeeds once the removal is done ([ADR-189](decisions.md)) |
| 503 | `resolver_busy` | wait | A host the request had to resolve — a webhook's on registration, an embedding provider's on configuration or for a search's `query` — was not looked up, because the lookups this node allows at once were all waiting on a resolver that has not answered ([Operations](operations.md#discovery-formats)). Nothing was done. Carries `Retry-After` in seconds, how long a failed lookup's answer is kept. A host that does not resolve is a `400` on registration and configuration, and a `502 provider_error` on a search |
| 409 | `no_vectors` | no | A search against a collection that **is configured** for vectors but has none stored — ingestion never ran, or has not caught up. A refusal rather than an empty result, which would be indistinguishable from "nothing matched". The unconfigured case is the `400` above; this answer also comes *before* the embedding provider is built, so it is what a node whose provider it could not build answers too, for as long as the collection is empty. The three are different questions, and [Vectors](vectors.md#search) puts them side by side |
| 413 | `payload_too_large` | no | Request body over `server.max_body_bytes` (2 MiB by default) |
| 415 | `unsupported_media_type` | no | A JSON body without a JSON content type |
| 501 | `not_implemented` | no | A reserved capability that does not exist yet |
| 410 | `resume_token_expired` | no | Resume point collected from the oplog. Resubscribe — retrying the token loops forever |
| 429 | `rate_limited` | wait | Too many failed logins from this caller, or an authenticated principal over its request budget (`server.rate_limit.per_principal`). Carries `Retry-After` in seconds |
| 502 | `provider_error` | wait | An upstream embedding provider failed. Every node calls the same provider, so waiting helps and moving does not |
| 503 | `timeout` | wait | The request was still waiting — for the rest of its body, or for an embedding provider — at `server.request_timeout_secs` (30 s by default) and this node abandoned it. Not a query timeout: storage work already running completes and is answered ([ADR-099](decisions.md)). **An index create or drop is never answered this way once it has committed**, even when building the index took the whole deadline: members that have not confirmed it by then are reported as pending instead ([ADR-140](decisions.md), and ADR-099's addendum) |
| 500 | `outcome_unknown` | verify | A write reached the storage engine's durability step and then failed, so it **may or may not have been applied** — and if it was, it replicates. **Read the target back before sending it again**, unless the write is idempotent: see [retrying after an unknown outcome](clients.md#retrying-after-an-unknown-outcome). A node whose own fsync fails usually stops (ADR-188) before it can answer, so a client more often meets this as a dropped connection after its request was sent, which it must treat the same way |
| 500 | `partially_applied` | verify | A request that commits in more than one transaction — a `multi: true` update or delete, a database drop — failed after its first commit. **What landed stands and replicates**, and `applied` says how much; `cause` says why the rest did not. Read back, or resend a request built to skip what is done: see [a request that was partly applied](#a-request-that-was-partly-applied). Never resend it blindly |
| 500 | `internal` | elsewhere | Storage failure on this node — details logged, never returned |
| 503 | `starting` | elsewhere | This node has bound its port and is still starting, and answers only `/healthz` and `/readyz` until it is up: nothing was read or written. Send it to another member ([ADR-198](decisions.md)) |
| 503 | `catching_up` | elsewhere | This member knows it is behind (it created its store in a cluster it has seeds for, was restored from a backup, or fell behind its peers' retention horizon, and its catching-up marker is set) and answers only `/healthz`, `/readyz`, `/metrics` and `/v1/topology` until it has caught up: nothing was read or written. Send it to another member ([ADR-202](decisions.md)) |
| 503 | `node_stopping` | elsewhere | This node is shutting down and did not complete the request: nothing was written. Send it to another member ([ADR-192](decisions.md)) |
| 500 | `misconfigured` | elsewhere | This node lacks something it needs to build the embedding provider a stored vector configuration names — the environment variable holding its API key is unset here, its provider is one this node's egress policy refuses, or it names a profile this node does not define. Reached only by a search that asks the server to **embed `query` text** on a collection that **already holds vectors**: a request carrying its own `vector` builds no provider, and an empty collection answers `409 no_vectors` first. See [Vectors](vectors.md#search) |
| 500 | `snapshot` | elsewhere | A vector index snapshot on this node could not be used |

**`retry` is three-valued because KimmyDB is leaderless.** Every node accepts
writes, so `elsewhere` — ask a different node — is an answer a primary-based
database cannot give, and it is the right one for a failure that belongs to the
node that answered rather than to the request. A boolean would tell a client
that `internal` is "retryable" and have it hammer the machine that just failed.

Act on `retry`, not on a table of codes compiled into a client at release time.
That is what makes adding a code an additive change.

Three deliberate properties:

**403 does not reveal existence.** Authorization is checked *before* the
collection is resolved, so a denied request looks identical whether the target
exists or not. A 404 there would let a caller probe for collections they cannot
access.

**Login failures are indistinguishable.** A wrong password and a nonexistent
user return byte-identical responses, and the missing-user path still performs a
hash so the timing matches. Otherwise the endpoint is a user-enumeration oracle.

**A 429 says nothing about the attempt.** The limit is keyed on the caller, so a
real username and an invented one over the same budget get identical responses.
A 429 for one and a 401 for the other would reintroduce the enumeration oracle
the property above removes.

---

## Not yet available

| | |
|---|---|
| Database- and cluster-scoped watch routes | Implemented in storage, no route yet |
| Client certificates (mTLS) | Not planned — the server proves itself, clients authenticate with a bearer token |

---

## Next

- [Query Language](query-language.md) — operator reference
- [Security](security.md) — the grant model
- [Operations](operations.md) — deployment and configuration
