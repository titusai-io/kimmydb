# HTTP API

[← Documentation index](README.md)

Everything speaks JSON over HTTP; change streams use WebSocket. Implemented in
`kimmy-api`.

Default port **7878**.

**This is KimmyDB's client protocol, not one of several.** HTTP provides
framing, Extended JSON v2 provides the encoding for types JSON cannot express,
a bearer token from `/v1/auth/login` provides authentication, `{status, tag,
message}` provides errors, and the WebSocket at `/watch` provides streaming.
The MongoDB wire protocol, gRPC and GraphQL were all considered and rejected —
[ADR-055](decisions.md) has the reasoning.

This page is a *reference*, not yet a *specification*: nothing here is
versioned or machine-checked against the routes, so it can drift. Making it a
contract, with a test that fails when it does, is M10 task 1 on the
[Roadmap](roadmap.md).

---

## Endpoints

| Method | Path | Action required |
|---|---|---|
| `GET` | `/healthz` | — public |
| `GET` | `/readyz` | — public |
| `GET` | `/metrics` | — public |
| `POST` | `/v1/auth/login` | — public |
| `GET` | `/v1/auth/whoami` | authenticated |
| `GET` `POST` | `/v1/users` | server admin |
| `GET` `DELETE` | `/v1/users/{name}` | server admin |
| `POST` | `/v1/users/{name}/password` | own account, or server admin |
| `POST` | `/v1/users/{name}/grants` | server admin |
| `GET` | `/v1/databases` | `read` (filtered) |
| `GET` `POST` | `/v1/db/{db}/collections` | `read` (filtered) / `admin` |
| `DELETE` | `/v1/db/{db}/coll/{coll}` | `admin` |
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
| `POST` | `/v1/db/{db}/coll/{coll}/vector` | `admin` — configure embedding ([Vectors](vectors.md)) |
| `GET` `PUT` `DELETE` | `/v1/db/{db}/coll/{coll}/docs/{id}/vectors` | `read` / `write` / `write` |
| `POST` | `/v1/db/{db}/coll/{coll}/vector_search` | `search` ([Vectors](vectors.md)) |
| `POST` | `/v1/db/{db}/coll/{coll}/hybrid_search` | `search` ([Vectors](vectors.md)) |
| `GET` `POST` | `/v1/db/{db}/coll/{coll}/webhooks` | `webhook` ([Webhooks](webhooks.md)) |
| `DELETE` | `/v1/db/{db}/coll/{coll}/webhooks/{id}` | `webhook` ([Webhooks](webhooks.md)) |
| `GET` `POST` | `/v1/db/{db}/coll/{coll}/indexes` | `read` / `admin` |
| `DELETE` | `/v1/db/{db}/coll/{coll}/indexes/{name}` | `admin` |
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
TOKEN=$(curl -s -XPOST localhost:7878/v1/auth/login \
  -H 'content-type: application/json' \
  -d '{"user":"root","password":"change-me"}' | jq -r .token)
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

Two ceilings, whichever binds first: **1000 documents**, and the **2 MB
request body limit** (`413`, `"error": "payload_too_large"`), which is the
lower of the two for documents over about 2 KB. Over the document cap is
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

`PUT` returns `{"matched": …, "modified": …, "upserted": …}`. Replacement is
**not** a merge — unnamed fields are dropped — and `_id` always comes from the
path, never the body.

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

Every field is optional. Default limit **100**, maximum **10,000**. `count` is
the size of the returned page, not the total match count — use `/count` for
that. Operator reference: [Query Language](query-language.md).

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

`multi` defaults to `false` — without it, one document is affected.

> **Sharp edge.** A multi-document update or delete is **not atomic as a batch**.
> Each write is individually atomic and logged, but a crash partway leaves a
> partial result. See [Storage](storage.md).

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
| `sample` | Documents to inspect. Default 100, max 1000 |
| `examples` | Include one example value per field |

Array elements are reported under `path[]`, matching how a query on the field
matches an *element*. `presence` is a fraction of the **sample**, counting
documents — it is inference, not a schema, and a field missing from the sample
may still exist.

The same information backs the MCP `describe_collection` tool; see
[MCP](mcp.md).

---

## Databases and collections

```bash
curl localhost:7878/v1/databases -H "$A"
curl localhost:7878/v1/db/shop/collections -H "$A"
curl -XPOST localhost:7878/v1/db/shop/collections -H "$A" -d '{"name":"orders"}'
curl -XDELETE localhost:7878/v1/db/shop/coll/orders -H "$A"
```

Databases are created implicitly by their first collection. Listing responses
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
a client can rely on.

A duplicate against a `unique` index returns **409 `unique_violation`**. Setting
`"enforcement": "coordinated"` returns **501** until clustering lands — a
`local` unique index is a single-node guarantee. See [Indexes](indexes.md).

Add `"explain": true` to `find`, `count`, `update` or `delete` to see whether
an index was used:

```json
{ "explain": { "strategy": "index", "index": "qty_1", "indexFieldsUsed": 1,
               "documentsExamined": 10, "documentsMatched": 10 } }
```

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

## The JSON boundary

Documents are stored as BSON. JSON cannot express several BSON types, so the
edge uses **Extended JSON v2**:

| BSON type | JSON form |
|---|---|
| ObjectId | `{"$oid": "6745f2a1b3c4d5e6f7081920"}` |
| DateTime | `{"$date": 1700000000000}` or RFC 3339 |
| Int64 | `{"$numberLong": "9007199254740993"}` |
| Binary | `{"$binary": {"base64": "…", "subType": "00"}}` |
| MinKey / MaxKey | `{"$minKey": 1}` / `{"$maxKey": 1}` |

Plain JSON keeps working for anything expressible in it — you only meet this
when you need a type JSON lacks.

**Whole numbers stay integers.** A JSON `42` becomes `Int32`, not `Double`.
Widening would break `$type` queries and lose precision above 2^53; `2^53 + 1`
round-trips exactly, and there is a test pinning it.

**Non-finite doubles** come back as `{"$numberDouble": "NaN"}` rather than
`null`, so a number never silently becomes a missing value.

---

## Errors

```json
{ "error": "duplicate_key",
  "message": "duplicate key: document with _id 1 already exists" }
```

| Status | `error` | Cause |
|---|---|---|
| 400 | `bad_request` | Malformed filter, update, projection, or Extended JSON; a bulk batch over 1000 documents |
| 422 | `bad_request` | A body that is valid JSON but the wrong shape — an object where `/bulk` wants an array |
| 401 | `unauthorized` | Missing, malformed, invalid, or expired token; bad credentials; a token whose account was deleted, disabled, or had its password or grants changed ([ADR-052](decisions.md)) |
| 403 | `forbidden` | Denied by RBAC |
| 404 | `not_found` | Document, collection, or user absent |
| 409 | `conflict` | Collection exists; last user; self-deletion |
| 409 | `duplicate_key` | `_id` already present |
| 409 | `unique_violation` | A unique index would be violated |
| 413 | `payload_too_large` | Request body over 2 MB |
| 415 | `unsupported_media_type` | A JSON body without a JSON content type |
| 501 | `not_implemented` | A reserved capability that does not exist yet |
| 410 | `resume_token_expired` | Resume point collected from the oplog |
| 429 | `rate_limited` | Too many failed logins from this caller. Carries `Retry-After` in seconds |
| 500 | `internal` | Storage failure — details logged, never returned |

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
