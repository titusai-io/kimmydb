# Webhooks

[← Documentation index](README.md)

Register a URL and the cluster pushes change events to it. The same events a
[change stream](change-streams.md) carries, for consumers that cannot hold a
WebSocket open.

```bash
curl -X POST https://node:7878/v1/db/shop/coll/orders/webhooks \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"url":"https://example.com/hook","operations":["insert","delete"]}'

{"id":"wh_…","url":"https://example.com/hook","secret":"…",
 "note":"the secret is shown once and cannot be retrieved later"}
```

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
  "events": [ { "eventId": "1786…-ec4c…", "operationType": "insert",
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
over the live SWIM member set — a pure function every node computes
independently, so there is no leader, no election, and no coordination.

**When that node dies, another takes over** and resumes from replicated
progress, so nothing is lost and nothing is sent five times.
[ADR-045](decisions.md) has the reasoning.

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
a window between the two.

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
| `kimmy_webhook_events_total` | Events pushed |
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

## Next

- [Change streams](change-streams.md) — the same events, client-held
- [Decisions](decisions.md) — ADR-045 on ownership and progress
- [Security](security.md) — the `webhook` action among the others
