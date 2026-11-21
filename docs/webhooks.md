# Webhooks

[← Documentation index](README.md)

Register a URL and the cluster pushes change events to it. The same events a
[change stream](change-streams.md) carries, for consumers that cannot hold a
WebSocket open — so an update that leaves a document as it was delivers
nothing ([ADR-208](decisions.md)).

```bash
curl -X POST https://node:7878/v1/db/shop/coll/orders/webhooks \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"url":"https://example.com/hook","operations":["insert","delete"]}'

{"id":"wh_…","url":"https://example.com/hook","secret":"…",
 "note":"the secret is shown once and cannot be retrieved later"}
```

`operations` is optional: absent or empty is `insert`, `update`, `replace` and `delete`. `conflictResolved` is the one operation that is
never implied: name it to be told of [lost writes](#lost-writes-conflictresolved).

Registering requires the **`webhook`** action on the collection. It is separate
from `watch` on purpose: a change stream ends when the client disconnects and
dies with its token, while a webhook keeps sending to an address the grant never
named. Only `admin` implies it.

---

## Verifying a delivery

Every request carries three headers:

| Header | |
|---|---|
| `X-Kimmy-Event-Id` | Stable, globally unique id of the first event in the batch |
| `X-Kimmy-Timestamp` | Milliseconds since the epoch |
| `X-Kimmy-Signature` | `HMAC-SHA256(secret, timestamp + "." + body)`, hex |

```python
expected = hmac.new(secret, f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()
if not hmac.compare_digest(signature, expected):
    return 401
```

The timestamp is **inside** the signature. Signing the body alone would leave it
free to change, so a captured delivery could be replayed later with a fresh
timestamp and still verify.

The body is a batch:

```json
{ "subscription": "wh_…",
  "events": [ { "eventId": "1786…-ec4c…-0000…", "operationType": "insert",
                "database": "shop", "collection": "orders",
                "clusterTime": "1786…", "documentKey": {"_id": 1},
                "fullDocument": {"_id": 1, "item": "widget"} } ] }
```

Every event names its database and collection, so one receiver fed by several
subscriptions routes on the body rather than on which URL was called.

---

## Delivery is at-least-once

**Deduplicate on `eventId`.** Exactly-once is not achievable over a network by
any design, so the id is stable across redeliveries and identical on every node
— a set-membership test is all a receiver needs.

The id is the change's stamp, `{hlc}-{node}-{epoch}`: the HLC as
`<wall ms>.<counter>`, the node id as its hyphenated UUID, and the epoch as 36
lowercase hex digits, so the epoch is always the last 36 characters. For
example:

```
1794482424658.0-92182685-4a9c-4903-9123-223d2c353993-000001a1ca93180000005c1e9a07d2b4f836
```

The epoch is in it since 0.61.0 (the id was `{hlc}-{node}` before): a node's
epoch changes when it is restored or rewound, and a change made after that can
share an HLC with one made before, so the epoch is what keeps the two ids apart.
**A receiver that deduplicates on the id needs no change other than accepting
the longer id.** Treat it as an opaque string.

Duplicates should be rare rather than routine: a crash mid-delivery, or a brief
disagreement about cluster membership. Answer `2xx` and the batch is marked
delivered; answer anything else, or time out, and it is retried.

**Ordering holds per subscription per origin node.** There is no total order
across nodes, which is what the leaderless design says everywhere else.

**A new subscription starts from now.** Registering does not replay history —
otherwise a webhook on a busy collection would be answered with up to a whole
`oplog_retention_secs` of events nobody asked for.

---

## In a cluster

One node delivers each subscription. It is chosen by hashing the subscription id
over the **node ids** in the live SWIM member set — a pure function every node
computes independently, so there is no leader, no election, and no coordination.

**When that node dies, another takes over** and resumes from replicated
progress, so nothing is lost and nothing is sent five times.
[ADR-045](decisions.md) has the reasoning.

Progress is kept in the system collection `__webhook_progress`, one record per
subscription and node (`_id` = `{subscription}:{node}`), whose `delivered` document is
keyed by origin, `<node uuid>/<36 hex digits of the epoch>`, with the position as 10
bytes of HLC. **An entry the reader cannot use counts as nothing delivered**: a key that
is not an origin (for example the bare node id written before 0.61.0), a value that is
not binary, or a value that is not 10 bytes. The events it covered are sent again
(at-least-once, as before), not skipped. The node logs it once per process for each
subscription and key, at `WARN`, naming the subscription, the record and the key and
what is wrong with it (`a webhook progress entry cannot be read`); further hits are
`DEBUG`. No metric counts it.

**Moving a node does not move its subscriptions.** Ownership follows the node
id, which lives in the database file — so a pod rescheduled onto a new IP, a
changed port, or a renumbered host keeps every subscription it had. Until M8
the address was the hash input, and re-addressing one node of three moved
**50.8%** of all subscriptions: worse than that node dying outright (25%),
because it counted as a departure *and* an arrival at once
([ADR-051](decisions.md)).

The only way an event is never delivered is if the write never replicated off
the node that accepted it — in which case the data is gone from the database
too, and webhooks are not what failed.

---

## When an endpoint stops answering

**Backoff is per subscription**, doubling to a five-minute ceiling. One dead
endpoint delays only its own deliveries; every other subscription on the node
keeps its cadence.

**A subscription that falls too far behind is invalidated.** If the events it
still owes have been collected under `storage.oplog_retention_secs`, they no
longer exist anywhere, so it stops rather than resuming from whatever is left —
which would silently skip everything collected, a gap the receiver could never
detect. The same contract a lagging change stream gets as `410`.

An invalidated subscription records why, and a listing shows it:

```json
{ "id": "wh_…", "state": "invalidated",
  "invalidReason": "delivery fell behind storage.oplog_retention_secs…" }
```

Recovery is to register a new one. It will start from now, so the gap is not
silently papered over.

---

## Where a webhook may point

Loopback, link-local (`169.254.0.0/16` — cloud metadata), RFC1918, carrier NAT
and reserved ranges are **refused**. Without that, anyone who can register a
webhook could make the database probe its own network and read the instance's
cloud credentials.

```toml
[webhooks]
allowed_hosts = ["internal.corp"]   # exempt specific hosts
```

The host is resolved and **every** address checked, at registration *and* before
each delivery — a name that resolves publicly today can resolve inward tomorrow.
Redirects are refused for the same reason.

The delivery check runs **inside the HTTP client's own resolver**, so the
addresses it approves are, by construction, the addresses the connection uses.
Checking on one resolution and dialling on another would leave a zero-TTL name
a window between the two. The check before the send stays as well: behind
`HTTP_PROXY` or `HTTPS_PROXY` the client resolves only the proxy's name, and
that check is then the only one of the target's addresses.

**A host that does not resolve is a failed delivery, not a stalled node.** The
lookup runs off the node's async workers and inside the ten-second delivery
timeout, so a resolver that has stopped answering fails that delivery at the
timeout and the subscription backs off, exactly as for an endpoint that has
stopped answering. Lookups are shared and bounded: deliveries to one host wait
on one lookup, a failed lookup is reused for 5 s, and at most 16 run at once
across the node ([Operations](operations.md#discovery-formats)). A delivery
refused because all 16 were busy is tried again at the next pass and does not
count against the subscription's backoff: what fills them need not be its own.
Registration resolves the host the same way, under the request's deadline,
from 4 slots of its own, so registrations cannot take the deliveries'; a
registration refused because those are busy is answered `503 resolver_busy`
with `Retry-After`, and a host that does not resolve is a `400`.

---

## How much a node will spend on delivery

```toml
[webhooks]
max_concurrent_deliveries = 8        # in flight at once, all subscriptions
max_payload_bytes = 1048576          # largest request body
```

Deliveries run **concurrently up to the bound**. That is not a throughput
tweak: a node that delivered one at a time would let a single endpoint that has
stopped answering hold everything else for the full ten-second timeout, so a
webhook you do not control would decide when the ones you do control fire. The
bound is the other half — a webhook on a hot collection cannot consume every
outbound connection the node has.

Ordering is unaffected. Each subscription still sends one batch at a time, so
events arrive in oplog order per subscription per origin node, exactly as
before.

### When a document is too large

Batches are trimmed to fit `max_payload_bytes`; whatever does not fit goes in
the next request. A **single** event whose document alone exceeds the cap is
delivered without it:

```json
{
  "eventId": "1754923010455.0-9f2c…",
  "operationType": "update",
  "documentKey": { "_id": 42 },
  "fullDocumentOmitted": true,
  "omittedReason": "document exceeds webhooks.max_payload_bytes; fetch it from the collection"
}
```

The change is still delivered — only the copy of the document is not, and the
receiver can read it from the collection. Dropping the event instead would
leave a gap the receiver could never detect, which is the whole reason
invalidation exists rather than silently resuming.

---

## Observability

| Series | |
|---|---|
| `kimmy_webhook_deliveries_total{outcome}` | `delivered` / `failed` batches |
| `kimmy_webhook_events_total` | Events pushed. A `conflictResolved` event counts each time it is sent, and one lost write can be sent more than once |
| `kimmy_webhook_subscriptions{state}` | `active` / `invalidated`, as this node sees the registry |
| `kimmy_webhook_backlog_seconds` | Age of the oldest undelivered event |

**`kimmy_webhook_backlog_seconds` is the one to alert on.** It answers "how far
behind is this node's delivery", which is the question a failing endpoint,
a slow one and a saturated one all show up in. It covers only the subscriptions
this node **owns** — a node that has stood down reports nothing, so summing
across a cluster does not multiply one subscription's lag by the number of
nodes.

`kimmy_webhook_subscriptions` is per node and counts the whole registry, which
replicates; take the max across nodes rather than the sum.

Registering and removing a subscription, and any invalidation, are recorded on
the `kimmy::audit` target — registering one grants ongoing egress, so it belongs
in the same stream as authorization decisions.

---

## Lost writes: `conflictResolved`

A `field` collection merges two writes to different fields of one document instead of keeping only the newer document. When the merge
has to choose (two writes to one field, a delete against a write, an insert against another insert) it keeps one and records what it
dropped. A subscription that **lists** `conflictResolved` is sent that record as an event:

```bash
curl -X POST https://node:7878/v1/db/shop/coll/orders/webhooks \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"url":"https://example.com/hook","operations":["conflictResolved"]}'
```

It is **opt-in**. An absent or empty `operations` is `insert`, `update`, `replace` and `delete`, and never this one, so a subscription
registered before the event existed does not start receiving a new kind of event. Only a collection created with `"mergePolicy":
"field"` can be subscribed to it: the policy cannot change, a `document` collection never loses a write to a merge, and registering is
refused with `400` rather than accepted and left silent.

```json
{ "subscription": "wh_…",
  "events": [ { "eventId": "cr1-77950a6b…", "operationType": "conflictResolved",
                "database": "shop", "collection": "orders", "clusterTime": "1795…",
                "documentKey": {"_id": 1}, "path": "x", "kind": "clash",
                "loser":  {"stamp": "1795…-1a2b…-0000…", "value": 1, "truncated": false},
                "winner": {"stamp": "1795…-9f3c…-0000…", "value": 2} } ] }
```

The fields are the ones a [change stream](change-streams.md#conflictresolved) carries, without the resume token. `loser` names the write
that was dropped (its stamp, its value, cut to `storage.loss_value_cut_bytes` (64 by default) and marked `truncated` when longer). `winner` is what stands **at the moment of
delivery**, read by the node that delivers: the stamp and value of the cell now at `path`, or `null` when the document is gone.

### What to do with it

**Deduplicate on `eventId`, and never compare bodies.** The `eventId` is `cr1-` and a SHA-256 of the collection, the document key, the
kind, the path and the lost write's stamp. It is the same on every node and in every redelivery, and it never looks like the stamp
`eventId` of a document event. Two deliveries of one `eventId` carry the same `loser` and may carry a different `winner`, because
each is read when it is sent and a write may have landed in between. The `X-Kimmy-Event-Id` header of a batch is the `eventId` of its
first event, so a batch that opens with a lost write has a `cr1-` id there.

**Expect a lost write more than once, up to once per member.** Every member that takes the record into its state keeps an entry of its
own for it, and the node that delivers a subscription sends every such entry it holds, its own and the ones it was sent by other
members, so that a record is not lost when the delivering node never held it itself. In a cluster of *M* members one lost write can
therefore arrive up to *M* times under one `eventId`, and more after the delivering node changes. That is the at-least-once contract
above, and the reason for the id. Each carries the `clusterTime` of the entry that carried it, so that is the one field of copies that differs. A delivery's order is, as for every event, per subscription per origin node.

**A lost write the cluster dropped never arrives.** A member keeps at most `storage.loss_records_per_document` records for a document
(4 by default) and cuts the values it keeps; a record past that is not kept, so no event is made of it.

### When the winner is too large

The loser's value is cut, but the winner can be a whole subdocument or array. An event that alone exceeds
`webhooks.max_payload_bytes` is sent with the winner's value left out, and says so, never dropped:

```json
"winner": { "stamp": "1795…-9f3c…-0000…", "valueOmitted": true,
            "omittedReason": "value exceeds webhooks.max_payload_bytes; read the cell from the collection" }
```

---

## Next

- [Change streams](change-streams.md) — the same events, client-held
- [Decisions](decisions.md) — ADR-045 on ownership and progress
- [Security](security.md) — the `webhook` action among the others
