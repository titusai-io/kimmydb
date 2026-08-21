# Compatibility — what `/v1` promises

[← Documentation index](README.md)

What a client may rely on, what may change under it, and what a correct client
has to tolerate. This is the contract half of the protocol; the shapes are in
[`openapi.yaml`](openapi.yaml) and the reference is [HTTP API](http-api.md).

Settled by [ADR-058](decisions.md).

---

## The promise, in one paragraph

**The path carries the major version, and `/v1` does not break.** A client
written against `/v1` today keeps working against every later build that still
serves `/v1`. Anything that would break such a client mints `/v2`, which is
served *alongside* `/v1` rather than replacing it. There is no negotiation
header, no per-request version pinning, and no compatibility shim layer.

The cost of that simplicity is paid by the client: a correct client has to
tolerate additions. The rules below say exactly which ones.

---

## Additive — ships in `/v1`, at any time, without notice

| Change | Why it cannot break a correct client |
|---|---|
| A **new route** | Nothing existing changes |
| A **new optional request field** | Omitting it keeps the previous behaviour |
| A **new response field** | A client that reads the fields it knows is unaffected. `expiresIn` on the login response, added in task 4, is the first one spent |
| A **new error code** | The envelope carries `retry`, so a client acts correctly on a code it has never seen ([ADR-057](decisions.md)) |
| A **new value in a response enum** a client is told to tolerate | `capabilities` is the case that exists today |
| A **new capability** in `GET /v1/version` | It is a fact about the node, not a change to any route |
| **Relaxing** a refusal — accepting something previously rejected | Nothing that worked stops working |
| A new **optional** query parameter | Same as a request field |

**What a correct client must therefore do**, and what the clients in tasks 8–10
will do:

- **Ignore unknown response fields.** Do not deserialize into a type that
  refuses them; do not assert on the exact shape of an object.
- **Treat an unknown error code as its `retry` class**, and an unknown `retry`
  value as `no`.
- **Treat an unknown capability as one it does not use**, and a *missing*
  capability as a feature to avoid — never as an error.
- **Not depend on field order, on the absence of a field, or on a message
  string.** `message` is prose for a human and changes freely.

A client that does these things is what "does not break" is measured against.
One that does not is outside the promise, and no versioning scheme can rescue
it.

---

## Breaking — requires `/v2`

| Change | |
|---|---|
| Removing or renaming a route, field, or error code | |
| Changing the type of a response field | The `matched` boolean-to-count change in M10 task 1 was exactly this, made before `/v1` had ever been published |
| Making an optional request field required | |
| Tightening a refusal so something that worked now fails | |
| Changing what a route *means* while keeping its shape | The worst kind, because nothing observable changes for a client until its data is wrong |
| Changing a default | A client that omitted the field gets different behaviour without asking for it |

When `/v2` arrives, `/v1` keeps being served for **at least one minor release
line, and no less than six months from the day `/v2` first ships**, whichever
is longer. Removal is a release-note event, not a quiet one.

---

## How a client learns what a node has

```
GET /v1/version
```

```json
{
  "protocol": "v1",
  "version": "0.1.0",
  "node": "3e98120f-66df-4cf0-9fa0-690e3d57fcea",
  "capabilities": ["aggregation", "backup", "bulk-insert", "..."]
}
```

**Branch on `capabilities`, not on `version`.** A version number only answers
"can I use this feature" if the client also carries a table mapping versions to
features — the table this endpoint exists to replace. `version` is for
operators, and `node` says which machine answered.

The capability set is closed by an enum in the server, and the contract test
holds it to the list in `openapi.yaml`, so a node cannot advertise something it
does not have.

---

## A cluster is not one version

Nodes are upgraded one at a time, and a client that fails over between nodes may
reach an older node immediately after a newer one. Three consequences, all of which
follow from the rules above rather than adding to them:

- **Ask each node.** `/v1/version` describes the node that answered it, and the
  answer is worth caching per node rather than per cluster.
- **A request using a field an older node does not know is refused**, with
  `422` and `bad_request` — several request bodies reject unknown fields
  deliberately, so a typo is an error rather than a silent no-op. That refusal
  is *correct*, and it is why capability discovery exists: check first, do not
  send and hope.
- **Failover does not paper over this.** A `retry: elsewhere` failure means the
  node was unable; it does not mean the next node is newer. A client that
  retries a capability-dependent request around the cluster will get the same
  refusal from every node that lacks the feature.

**The cluster wire is a different contract entirely.** SWIM identities are
encoded with postcard, which is not self-describing, so adding or reordering a
field there breaks membership outright — three such upgrades are already
documented in [Operations](operations.md) as stop-the-cluster events
([ADR-040](decisions.md), [ADR-051](decisions.md), [ADR-053](decisions.md)).
The client protocol deliberately has the opposite property, and the contrast is
the point: the internal wire is optimized for size and changed under an outage
window; the client wire is optimized for not breaking anyone.

---

## What is checked, and what is only written down

Some of this is mechanism and some is prose. The difference matters, because
this project has been wrong before about claims nothing checked.

**Checked by `crates/kimmy-api/tests/openapi.rs`:**

- Every versioned route sits under `/v1/`, and the prefix agrees with the
  `protocol` the server reports and with `info.version` in the specification.
- The capability list the server serves is exactly the one the specification
  documents.
- No response schema forbids unknown properties, so a client validating against
  today's document still validates tomorrow's responses — which is what makes
  "a new response field is additive" true rather than merely intended.
- Every documented operation still answers with the shape it declares.

**Written down and not checked:**

- The six-month `/v2` window. A promise about calendar time cannot be a test.
- "Changing what a route means" being breaking. Nothing can detect a change of
  meaning that preserves shape; that one is a review responsibility, and it is
  the reason a route's *semantics* belong in the specification's prose rather
  than only in its schemas.

---

## Next

- [HTTP API](http-api.md) — the reference
- [`openapi.yaml`](openapi.yaml) — the specification
- [Decisions](decisions.md) — ADR-055, ADR-056, ADR-057, ADR-058
