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
| `POST` | `/v1/db/{db}/coll/{coll}/aggregate` | `read` — see [Aggregation](aggregation.md) |
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

`_id` is generated if absent. A duplicate `_id` is **409** with
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

`PUT` returns `{"matched": 0|1, "modified": 0|1, "upserted": true|false}` —
**counts**, as `update` and `find_and_modify` return, even though a replace
touches at most one document. Replacement is **not** a merge — unnamed fields
are dropped — and `_id` always comes from the path, never the body.

**Without `?upsert=true` a missing document is not an error.** The answer is
`200 {"matched": 0}` and nothing is written. A test built on the assumption
that this creates the document writes nothing and passes.

`DELETE` returns `{"deleted": 1, "stamp": "…"}` when it removed the document
and `{"deleted": 0}` when there was none to remove — a count, like `PUT`, and
not an error either way. The `stamp` is the **tombstone's** version, the one
the delete produced, exactly as `POST .../delete` of one document reports it;
it is present when and only when `deleted` is `1`.

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

> **`modified` counts documents written, not documents changed.** A `$set` to
> the value a field already holds is still a write, so it still counts —
> `{"matched": 2, "modified": 2}` for an update that moved nothing. MongoDB's
> `nModified` excludes those, so the two disagree on exactly the question
> "did anything change?". To ask that, compare `matched` against a `count`
> with a filter describing the state you want. Recorded in
> [Deviations](deviations.md).

**The operators run inside the write transaction.** An `update` matches and
writes in one transaction, on the image that transaction holds, so two
concurrent `$inc`s on one document both land — the same guarantee
`find_and_modify` makes, through the same engine path (ADR-083). A
`multi: true` request commits in **chunks** of `storage.multi_chunk_docs`
documents (default 1,000): each chunk is one transaction and one fsync, the
writer is released between chunks, and the response's `commits` field says
how many chunks landed ([ADR-086](decisions.md)).

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
> request: a failure in a later chunk leaves the earlier chunks committed and
> answers with an error. Nothing is visited twice and the oplog reflects
> exactly what landed, but a caller that needs the count reads it back. See
> [Storage](storage.md).

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
decision is made where it is applied. `DELETE /v1/db/{db}` drops each
collection in turn (`ddl` over the database); system databases (`__…`) are
refused.

**Issue a database drop once, on one member, and let it replicate.** It drops
the collections that member holds **at the moment it is applied**, and that is
the whole of what replicates. Sending the same `DELETE` to every member is not
belt and braces — it is a race each peer can lose: a collection still in flight
to that peer arrives after its local drop and recreates the database there. A
vector-enabled collection is the easy way to see it, because the owning member
builds the shadow collection locally and it replicates a moment behind, leaving
a peer holding a database whose only contents are a shadow whose base
collection is gone. Drop on one member and confirm the drop on the others (a
drop is not instant — see [Operations](operations.md)); do not drop on each.

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
an index was used:

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
curl localhost:7878/readyz     # {"status":"ready","node":"9d5200f2-…"}
curl localhost:7878/metrics    # Prometheus text format
```

All three are unauthenticated so a load balancer can probe them without
credentials. `/healthz` is liveness; `/readyz` proves the storage engine
actually responds, so a node with a wedged database is taken out of rotation.

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
with nothing else configured, and it is how a conformant MCP client discovers
where to authenticate — see [mcp.md](mcp.md).

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

**A Decimal128 is stored, returned, and never compared.** `$numberDecimal`
is read on input and written on output, and the digits come back exactly as
sent. It cannot be an index key or an `_id` — a document holding one at an
indexed path is stored and filed under the index's unkeyed run
([Indexes](indexes.md#documents-an-index-cannot-key)); one as `_id` is
refused — and it cannot be a filter operand: `{"v": {"$numberDecimal":
"1.5"}}`, or a Decimal128 under `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`,
`$in`, `$nin`, `$all`, or inside a document or array literal, is `400
bad_request` saying that a Decimal128 *cannot be compared in a filter*; as a
literal in an expression, `$expr` included, the `400` says *a Decimal128
literal is not supported in an expression*. The canonical order has no exact
place for it — it ranks equal to every other number — so a filter that let
one through would match every numeric value of the field. A sort over a
field where a matching document holds one is refused naming the document
(*cannot sort by "v": document 3 holds a Decimal128 there*), and the update
operators that compare their operand — `$min`, `$max`, `$addToSet`, `$pull`,
`$pullAll` — refuse one (*`$pull` cannot compare a Decimal128 operand*);
`$type: "decimal"` finds such documents without comparing them
([Query language](query-language.md#3-comparisons-do-not-cross-type-groups)).
Through 0.24.0 the edge did not read `$numberDecimal` at all: the wrapper
arrived as a nested document, which is why a Decimal128 written to a member
was keyed there and filed unkeyed on its peers ([ADR-139](decisions.md)).

**Whole numbers stay integers.** A JSON `42` becomes `Int32`, not `Double`.
Widening would break `$type` queries and lose precision above 2^53; `2^53 + 1`
round-trips exactly, and there is a test pinning it.

**Non-finite doubles** come back as `{"$numberDouble": "NaN"}` rather than
`null`, so a number never silently becomes a missing value.

**Every request body is closed.** A field the route does not define — a
misspelt `limitt`, an `explain` on `aggregate`, anything the route's section
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
arrive (ADR-120). An inclusion projection answers in the document's order,
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
  "count": 2 }
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
  "version": "0.24.0",
  "commit": "9f1c2ab",
  "node": "3e98120f-66df-4cf0-9fa0-690e3d57fcea",
  "capabilities": ["aggregation", "bulk-insert", "backup", "change-streams",
                   "client-supplied-vectors", "cursor-paging", "find-and-modify",
                   "hybrid-search", "partial-indexes", "token-refresh", "topology",
                   "ttl-indexes", "vector-search", "webhooks", "conditional-writes",
                   "local-embeddings"],
  "durability": "durable" }
```

That is the complete list, in the order the server emits it. Every entry but
one is constant for a build: `local-embeddings` is **the only conditional
capability**, present on a build compiled with that feature and absent
otherwise, so a `local` embedding provider is accepted on such a node and
refused everywhere else. What each name entitles a client to is tabulated
under `Capability` in [`openapi.yaml`](openapi.yaml), and a test holds the
server's enum, that table and this list to one set.

`commit` and `durability` are fields, not capabilities. `commit` is the short
git commit the binary was built from — `unknown` for a build made without git,
which is a supported build — and is for the operator staring at a
half-upgraded cluster where two nodes claim the same `version`. `durability`
is how this node makes a commit durable, `durable` or `coalesced`; both are
durable when a write returns, and the value is a fact about the node's
configuration rather than its build, which is why it is not in the capability
list ([Storage](storage.md#durability-classes), [ADR-088](decisions.md)).

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
The set is closed by an enum in the server ([ADR-057](decisions.md)), so a new
failure cannot appear without its retry class being decided in the same commit.

| Status | `error` | `retry` | Cause |
|---|---|---|---|
| 400 | `bad_request` | no | Malformed filter, update, projection, or Extended JSON; a bulk batch over 1000 documents; a query parameter the route does not define, or one it cannot parse or honour — `?sample=0` on `describe` ([ADR-121](decisions.md)); an `if_stamp` that is not a stamp the server issued; a `vector_search` or `hybrid_search` on a collection with **no vector configuration**, where the message names the `POST …/vector` route that enables it; **a key `$unwind`'s document form, `$lookup`, or `$replaceRoot` does not define, or a document whose `$unwind` path crosses an array** ([ADR-129](decisions.md), [ADR-130](decisions.md)) |
| 422 | `bad_request` | no | A body that is valid JSON but the wrong shape — an object where `/bulk` wants an array, a required field missing, a field the route does not define, or an explicit `null` on a declared *optional* field that does not accept one; the message names it ([ADR-121](decisions.md), [ADR-128](decisions.md)). A `null` sent for a *required* field is refused too, but by that field's own type — see [The JSON boundary](#the-json-boundary) — and is not always this status. **Not** a pipeline stage operand's unknown key, which is untyped JSON at this layer and is refused `400` once `kimmy-query` parses it (ADR-129) |
| 401 | `unauthorized` | no | Missing, malformed, invalid, or expired token; bad credentials; a token whose account was deleted, disabled, or had its password or grants changed ([ADR-052](decisions.md)) |
| 403 | `forbidden` | no | Denied by RBAC |
| 404 | `not_found` | no | Document, collection, or user absent. **A collection absent on a node that has peers answers `elsewhere` instead**: created through a load balancer, it lands on one member and reaches the rest a sync round later, and another member has it meanwhile |
| 409 | `conflict` | no | Collection exists; an index name already taken by a different definition; last user; self-deletion |
| 409 | `duplicate_key` | no | `_id` already present |
| 409 | `unique_violation` | no | A unique index would be violated |
| 409 | `stale` | no | A conditional write's `if_stamp` did not match: the document is at another version, or is gone. Nothing was written — re-read, decide again, and send a new request with the current stamp ([conditional writes](#get-replace-delete-by-id)) |
| 409 | `no_vectors` | no | A search against a collection that **is configured** for vectors but has none stored — ingestion never ran, or has not caught up. A refusal rather than an empty result, which would be indistinguishable from "nothing matched". The unconfigured case is the `400` above; this answer also comes *before* the embedding provider is built, so it is what a node whose provider it could not build answers too, for as long as the collection is empty. The three are different questions, and [Vectors](vectors.md#search) puts them side by side |
| 413 | `payload_too_large` | no | Request body over `server.max_body_bytes` (2 MiB by default) |
| 415 | `unsupported_media_type` | no | A JSON body without a JSON content type |
| 501 | `not_implemented` | no | A reserved capability that does not exist yet |
| 410 | `resume_token_expired` | no | Resume point collected from the oplog. Resubscribe — retrying the token loops forever |
| 429 | `rate_limited` | wait | Too many failed logins from this caller, or an authenticated principal over its request budget (`server.rate_limit.per_principal`). Carries `Retry-After` in seconds |
| 502 | `provider_error` | wait | An upstream embedding provider failed. Every node calls the same provider, so waiting helps and moving does not |
| 503 | `timeout` | wait | The request was still waiting — for the rest of its body, or for an embedding provider — at `server.request_timeout_secs` (30 s by default) and this node abandoned it. Not a query timeout: storage work already running completes and is answered ([ADR-099](decisions.md)) |
| 500 | `internal` | elsewhere | Storage failure on this node — details logged, never returned |
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
| `$vectorSearch` as a pipeline stage | The pipeline itself is built ([Aggregation](aggregation.md)); vector search remains its own endpoint |
| Database- and cluster-scoped watch routes | Implemented in storage, no route yet |
| Client certificates (mTLS) | Not planned — the server proves itself, clients authenticate with a bearer token |

---

## Next

- [Query Language](query-language.md) — operator reference
- [Security](security.md) — the grant model
- [Operations](operations.md) — deployment and configuration
