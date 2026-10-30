# Validators

A collection can carry a **validator**: a `$jsonSchema` that every document a
client writes to it must satisfy. It is collection metadata, replicated like an
index definition or a vector configuration, and enforced by the member that
accepts the write. The decision is [ADR-257](decisions.md); the dialect the schema
is written in is the `$jsonSchema` operator's, in [Query Language](query-language.md).

A validator is a guard on **client writes**. It does not rewrite data, it does not
re-examine what is already stored, and it never refuses anything a peer sends. That
last property is what keeps a leaderless cluster convergent, and it has a cost,
stated [below](#what-a-validator-does-not-promise).

## Setting one

```bash
# at creation
curl -XPOST localhost:7878/v1/db/shop/collections -H "$A" -d '{
  "name": "orders",
  "validator": { "$jsonSchema": {
    "bsonType": "object",
    "required": ["sku", "qty"],
    "properties": { "qty": { "bsonType": "number", "minimum": 0 } }
  }},
  "validationAction": "error",
  "validationLevel": "strict"
}'

# later: replaces the validator and both policies
curl -XPUT localhost:7878/v1/db/shop/coll/orders/validator -H "$A" -d '{
  "validator": { "$jsonSchema": { "required": ["sku"] } },
  "validationAction": "warn"
}'

# remove it
curl -XDELETE localhost:7878/v1/db/shop/coll/orders/validator -H "$A"
```

| Field | Values | Default |
|---|---|---|
| `validator` | `{ "$jsonSchema": <schema> }` and nothing else | none |
| `validationAction` | `error` refuses a failing write; `warn` accepts, logs and counts it | `error` |
| `validationLevel` | `strict` checks every insert and update; `moderate` checks inserts, and updates only of documents that conformed before | `strict` |

- **Only `$jsonSchema` is a validator.** A query filter written as a validator
  (`{ "qty": { "$gte": 0 } }`), or `$jsonSchema` beside another key, is `400`
  saying so. Any other value for either policy is `400`, as is a policy with no
  validator to apply to, and an unknown field in the body.
- **The schema is parsed when it is set.** A schema this build does not read (an
  unknown keyword, a `$ref`, a bad pattern, too many patterns) is `400` with the
  parser's message and changes nothing. A schema holds at most 32 `pattern`s and
  2 MiB of compiled pattern; see [Query Language](query-language.md) for the
  keywords.
- `PUT` replaces all three fields: a body naming no `validationAction` sets the
  default, not the one before. `PUT` needs `validator`; removing one is `DELETE`.
  Both need `ddl` on the collection. A collection that is not there is `404`.
- `DELETE` answers `{ "removed": true }`, or `false` when there was nothing to remove.
  It is a change all the same: stamped and replicated, so a member that was down
  learns the validator is gone.
- `GET …/describe` shows `validator` (`null` when there is none) and, with one,
  `validationAction` and `validationLevel`.
- Creating with a validator is the creation and then the same change `PUT` makes,
  as two replicated entries. The schema is parsed first, so a bad one creates
  nothing.

## What it checks

The check is on the **resulting document**, the image that would be stored:

| Write | Checked as |
|---|---|
| insert, bulk insert, an upsert that creates the document | an insert |
| replace by id (`PUT …/docs/{id}`) | the new document, against the stored one |
| `update` (one or many, every operator) | the document after the operators |
| `find_and_modify` | the updated document; an upsert's inserted document |
| `$merge`, `$out` | each output document; a merge of a matched document in its final form, stored fields and incoming fields together |

A write that changes nothing (the document is byte for byte what is stored) writes
nothing and is not judged. Deleting is never judged, and neither is the expiry of a
TTL index.

**`error`** refuses the write with `422` and the code `validation_failed`
([HTTP API](http-api.md#errors)), retry `no`. The message names the keyword path
that failed, in the schema's own account of it (`qty: minimum: -1 is below the
minimum 0`). Nothing is written. A bulk insert is refused whole, as for any other
document it cannot store, and the message names the position. A `multi` update
that fails in a later chunk is `partially_applied`, as for a size-cap refusal
(ADR-192), and the earlier chunks stay.

**`warn`** accepts the write, writes a `WARN` line with the collection and the
reason (at most one a second per collection; the others are counted into the next),
and counts it in `kimmy_validation_warnings_total`. A refusal is counted in
`kimmy_validation_refusals_total`. Both are totals for the member, with
no collection named, because `/metrics` is served without a credential.

**`moderate`** is for a collection that already holds documents that do not conform.
An insert is checked. An update or replace of a stored document is checked only if
that document, as stored before the write, conforms; one that does not may be
changed freely, to anything, so the data can be repaired in steps. `strict` judges
every update.

**`$merge` and `$out`** with action `error` check every output document that the
output alone decides (all of them for `$out`, and for a `$merge` that replaces and
inserts under `strict`) before the first chunk is written, so a refused output
writes nothing. A `$merge` that merges into a matched document is judged in its
final form inside the transaction that writes it, where an earlier chunk has
already committed; a failure there is a failure in a later chunk, `partially_applied`
with the chunks before it applied.

## Replication

The validator is stamped and replicates as a `ConfigureValidator` oplog entry (a collection created with one carries it on its `CreateCollection` entry, applied in the same transaction as the creation),
last writer winning on the stamp, exactly as a vector configuration does
([ADR-226](decisions.md#adr-226--a-vector-configuration-carries-the-stamp-of-its-latest-change-and-a-snapshot-page-says-when-it-was-turned-off)):
two members that changed it concurrently converge on the later change, a replayed
older entry is ignored, and a snapshot page carries it, including **"removed at
s"**, so a member that was down while it was removed does not keep it. It takes part
in the definition digest (`kimmy_sync_divergence_definition_probes_total`), as a
finding for the operator like any definition difference that is not a missing plain
index; ordinary replication is what heals it.

**Enforcement is on the accepting member only.** A replicated entry, a snapshot
page, a repair, a catch-up and a restore are never validated, and a change of the
validator never re-examines what is stored.

## What a validator does not promise

A leaderless cluster cannot make a schema change atomic across members, so a
validator is a guard and not an invariant:

1. **A member that has not received the change accepts a write the validator would
   refuse.** Between `PUT` on one member and its arrival on another, the other
   accepts documents the new schema forbids. A collection created with a validator
   has no such gap on a member that applies its `CreateCollection` entry, which
   creates both together; a member that has not yet received the entry does not
   hold the collection.
2. **A document accepted that way is kept.** It replicates to the members that have
   the validator, which store it without judging it, so a collection can hold
   documents that fail its validator. So can a collection whose validator was added
   after its documents were written, and one whose documents arrived by restore.

Both are in [Deviations](deviations.md). Neither is repaired automatically: the
report below is how to find them.

## Finding the documents that fail it

```bash
curl "localhost:7878/v1/db/shop/coll/orders/validator/violations?limit=100" -H "$A"
```

```json
{
  "violations": [ { "_id": 2, "reason": "qty: minimum: -1 is below the minimum 0" } ],
  "count": 1,
  "scanned": 2,
  "validator": true,
  "nextCursor": "…"
}
```

**This is a scan.** Nothing is kept: each call reads the collection's documents in
`_id` order and tests them against the current validator on the member that answers.
A page examines at most 10,000 documents and returns at most `limit` failures
(default 100, at most 10,000), so its work is bounded, and `nextCursor` is present
whenever the collection holds more to examine, **even on a page with fewer failures
than `limit`**: follow it until it is absent. The cursor is the same opaque token
`find` pages with, portable between members. `read` on the collection is the
permission. A collection with no validator answers an empty page with
`"validator": false`. Each member answers for what it holds, so a document one
member has not yet received is not in its report.

## Metrics

| Series | |
|---|---|
| `kimmy_validation_warnings_total` | Writes accepted under `warn` that failed the schema |
| `kimmy_validation_refusals_total` | Writes refused under `error` |
| `kimmy_validator_unparseable_total` | Stored validators this build could not parse; such a collection is **not validated** and stays writable, and the cause is logged at `ERROR` |
