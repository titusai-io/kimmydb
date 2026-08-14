# Clients

[← Documentation index](README.md)

First-party libraries for talking to KimmyDB. The protocol is
[`openapi.yaml`](openapi.yaml) and anyone may write their own; these exist so
that most people do not have to, and so that the specification has three
independent readers keeping it honest.

| Language | Crate / package | Status |
|---|---|---|
| Rust | `kimmy-client` | ✅ M10 task 8 |
| Python | — | 📋 M10 task 9 |
| Go | — | 📋 M10 task 10 |

---

## What a client is expected to do

The server's promises are only useful if a client keeps its half. These are the
behaviours every first-party client implements, and what the conformance suite
(M10 task 11) will hold all three to.

**Keep a token alive without holding credentials in every call.** Log in once,
then refresh before expiry using `expiresIn` — never by decoding the token,
which is opaque and whose shape nothing promises.

**Treat an unknown error code as its retry class.** Codes are additive: a
server newer than the client will use ones it has never heard of. `retry` is in
the envelope precisely so that a client does not need a table
([ADR-057](decisions.md)).

**Page with cursors, and end the walk on an empty page.** A `find` with no
`limit` returns 100 documents rather than all of them, and a final page that is
exactly full still carries a token — so a walk that stopped when the token
stopped arriving would read one page too few.

**Fail over between nodes, and be careful about writes.** Every node accepts
writes, so selection is round-robin plus retry with no primary to find. But
`retry: elsewhere` means *this node* did not answer, **not** that the work did
not happen: repeating an insert that failed after its commit would apply it
twice, and no status distinguishes the two. Reads move freely; writes are the
caller's decision.

**Resume change streams from the last token seen.** Tokens are portable across
nodes, so a reconnect may land elsewhere and continue correctly — verified on a
real cluster rather than argued from the design.

**Ignore what it does not recognize.** Unknown response fields, unknown
capabilities, unknown enum values. [Compatibility](compatibility.md) is the
full contract.

---

## Rust — `kimmy-client`

```rust
use kimmy_client::{Client, Query};
use serde_json::json;

let client = Client::builder("http://localhost:7878")
    .credentials("root", "hunter2")
    .discover_nodes(true)
    .connect()
    .await?;

client.insert("shop", "orders", &json!({ "sku": "widget", "qty": 5 })).await?;

let mut pages = client.pages("shop", "orders", Query::new().limit(500));
while let Some(page) = pages.next().await? {
    for document in page {
        println!("{document}");
    }
}
```

### It depends on no `kimmy-*` crate

Deliberately, and a test keeps it that way. The client sees exactly what a
Python or Go client sees — the specification and the bytes on the wire — so it
cannot quietly rely on something the protocol never promised. A shared type
would make this the one client that works for a reason the others cannot have,
and the first sign of that would be a bug the other two have and this one does
not.

### Retries, and the one it will not do for you

| Failure | Reads | Writes |
|---|---|---|
| Transport — no answer at all | next node | returned to the caller |
| `retry: elsewhere` | next node | returned to the caller |
| `retry: wait` | one bounded wait, then the next node | returned to the caller |
| `retry: no` | returned | returned |

A write can be moved to another node by declaring it idempotent —
`Safety::Idempotent` — which is a claim only the caller can make. An insert
carrying its own `_id` qualifies: a repeat fails with `duplicate_key`, which is
a fact. An insert without one does not: a repeat inserts a second document.

### Change streams

```rust
let mut stream = client.watch("shop", "orders", WatchOptions::new().full_document(true)).await?;
while let Some(event) = stream.next().await? {
    println!("{} {:?}", event.operation, event.document_id());
    if event.is_invalidate() {
        break;   // the collection is gone; there is nothing to resume to
    }
}
```

Reconnects with backoff and resumes from the last token it saw. It stops for
exactly two reasons: an `invalidate`, and a resume point that has fallen past
the retention horizon — which cannot be waited out, because retrying the same
token loops forever.

### The escape hatch

`Client::request` reaches any route by path. It exists because a client that
covers a subset of an API and cannot reach the rest sends people back to `curl`
for one call; every named method above is a convenience over it.

---

## The CLI is a consumer, not a second implementation

`kimmy` speaks through `kimmy-client` — nothing in it builds a URL or reads a
status code. That is what proves the library is pleasant rather than merely
present, and it worked: converting it found a public API that forced consumers
to depend on `reqwest`, a login that could not fail over, and a missing
`create-collection` that made a fresh database unusable from the tool.

[CLI](cli.md) has the commands.

---

## Next

- [`openapi.yaml`](openapi.yaml) — the specification these are written against
- [Compatibility](compatibility.md) — what `/v1` promises, and what a correct client must tolerate
- [HTTP API](http-api.md) — the reference
