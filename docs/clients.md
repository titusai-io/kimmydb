# Clients

[← Documentation index](README.md)

First-party libraries for talking to KimmyDB. The protocol is
[`openapi.yaml`](openapi.yaml) and anyone may write their own; these exist so
that most people do not have to, and so that the specification has three
independent readers keeping it honest.

| Language | Crate / package | Status |
|---|---|---|
| Rust | `kimmy-client` | ✅ M10 task 8 |
| Python | `kimmydb` | ✅ M10 task 9 |
| Go | `clients/go/kimmydb` | ✅ M10 task 10 |

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
writes, so there is no primary to find. Selection is **sticky rather than
round-robin**: a client keeps using whichever node last answered and moves only
when one stops answering. That keeps a connection warm, and it means load is not
spread across the cluster by default — one node serves everything until it
fails. But
`retry: elsewhere` means *this node* did not answer, **not** that the work did
not happen: repeating an insert that failed after its commit would apply it
twice, and no status distinguishes the two. Reads move freely; writes are the
caller's decision.

**Resume change streams from the last token seen.** Tokens are portable across
nodes, so a reconnect may land elsewhere and continue correctly — verified on a
real cluster rather than argued from the design.

**Make a check-then-act conditional, and never retry a `stale` refusal.**
Every write answers with the version it produced (`stamp`), `find` returns
versions on request, and a single-document write carrying `if_stamp` lands
only if the document is still at that version. A `409 stale` means the
document moved on: the client re-reads, decides again, and sends a *new*
request — the first-party clients expose the code (`ErrorCode::Stale`,
`is_stale`, `Stale()`) and do not retry it, because repeating the same request
can only fail the same way.

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

### Federated tokens: a callback, not an OAuth2 client

Against a node federated with an OIDC provider ([Security](security.md)),
`credentials()` cannot produce a token — that endpoint is this database's own
login and it stays local-only. `token_provider` takes an async callback the
client calls at connect time, shortly before each token expires, and once more
when a request comes back 401:

```rust
let client = Client::builder("https://kimmy.internal:7878")
    .token_provider(|| async { my_oidc_library.access_token().await })
    .connect()
    .await?;
```

The callback is whatever the application already uses to talk to its provider.
This crate deliberately does not implement OAuth2: an OAuth2 implementation
inside a database client is a second one to keep correct, and an application
that needs a token from a provider already has one. It is also why nothing
here ever holds a refresh token.

`/v1/auth/refresh` **refuses a federated principal** — a federated token is the
provider's to renew, and minting a local one from it would launder the identity
(ADR-065). So a `token_provider` is the whole renewal mechanism, and a
`token()` handed in by hand is used until the server stops accepting it.

---

## Python — `kimmydb`

```python
from kimmydb import Client

db = Client("http://localhost:7878", user="root", password="hunter2",
            discover_nodes=True)

db.create_collection("shop", "orders")
db.insert("shop", "orders", {"sku": "widget", "qty": 5})

for document in db.documents("shop", "orders"):
    print(document)

for event in db.watch("shop", "orders", full_document=True):
    print(event.operation, event.document_id)
```

**Synchronous.** `httpx` and `websockets` both have async APIs behind nearly
the same surface, so an async client is a second class over the same request
path rather than a different design — a decision for when someone wants it, not
an assumption made now.

**Two dependencies, and connection pooling is why there are two rather than
zero.** The stdlib can speak HTTP, but `urllib` opens a connection per request:
against a measured ~0.1 ms request, a handshake per call would dominate
everything the client does.

**It shares no code with the Rust client.** They are independent readers of one
specification, which is what makes a disagreement between them mean something.
The test suites are deliberately the same scenario list for the same reason.

### Idioms, not translations

The behaviour matches the Rust client; the surface does not pretend to.
Iteration is where a Python caller expects it, and `documents()` exists because
the shape most people want is "every matching document" rather than "a sequence
of pages":

```python
for page in db.pages("shop", "orders", limit=500):   # pages
for document in db.documents("shop", "orders"):      # documents
```

Errors are raised, not returned. `KimmyError` carries `.code`, `.retry`,
`.status` and `.message`; `.code` is a **plain string** rather than an enum,
because codes are additive and an enum would turn "a code I have not heard of"
into an error in itself.

### One thing that had to fight the idiom

A change stream connects **when you ask for it**, not when you first read from
it. Python's natural shape here is a lazy generator, and a lazy one would open
the socket on the first `next()` — so anything written between `watch()` and
that first read would be missed silently. Found by a test that wrote a document
immediately after opening a stream and then waited for it forever.

---

## Go — `clients/go/kimmydb`

```go
import "github.com/titusai-io/kimmydb/clients/go/kimmydb"

db, err := kimmydb.New(ctx, "http://localhost:7878",
    kimmydb.WithCredentials("root", "hunter2"),
    kimmydb.WithDiscovery(true),
)
defer db.Close()

for document, err := range db.Documents(ctx, "shop", "orders", kimmydb.Query{}) {
    if err != nil { return err }
    fmt.Println(document)
}

for event, err := range db.Watch(ctx, "shop", "orders", kimmydb.WatchOptions{}) {
    if err != nil { return err }
    fmt.Println(event.Operation, event.DocumentID())
}
```

**One dependency.** `net/http` pools connections, so the reasoning that ruled
out Python's standard library does not apply — the only thing Go's does not
have is WebSocket framing.

**`coder/websocket` rather than `gorilla/websocket`**, and for a specific
reason: it handshakes through an ordinary `*http.Client`, so a change stream
inherits the same client, TLS configuration, proxy and timeouts as every other
request. `gorilla` dials with its own `Dialer`, which means two configurations
that can drift apart — the class of split this project has been bitten by
before.

### Idioms, not translations

Paging and streaming are **range-over-function iterators**, which is where a Go
caller expects them and which makes the error impossible to skip — it is the
second loop variable:

```go
for page, err := range db.Pages(...)      // pages
for document, err := range db.Documents(...)  // documents
for event, err := range db.Watch(...)     // events
```

Errors are values, and `*APIError` carries `Status`, `Code`, `Retry` and
`Message`. `Code` is a **plain string** rather than a set of constants, for the
same reason it is a string in Python: codes are additive, and making an
unfamiliar one an error in itself is exactly what the retry class exists to
prevent.

Everything takes a `context.Context`, including the change stream — cancelling
it is how a caller stops watching.

---

## Conformance: one set of scenarios, three clients

Three clients passed matching scenarios only because they were **written** to
match. Nothing enforced it, and nothing would have noticed the day one started
paging differently — except a user.

`clients/conformance/scenarios.json` declares every scenario and the
observations a correct client must produce. Each client ships a small driver
that runs a named scenario and prints what it observed; the runner starts a
fresh node per scenario, runs it against every driver, and compares.

```bash
./clients/conformance/run.py
```

```
16 scenarios x 3 clients: go, python, rust

paging_walks_everything                        go:ok  python:ok  rust:ok
change_stream_resumes                          go:ok  python:ok  rust:ok
...
48 runs, three clients, one set of scenarios: no disagreements
```

Two different checks. **Coverage**: every declared scenario must be implemented
by every driver, so a client that quietly stops covering one fails rather than
falls silent. **Behaviour**: the observations must match what is declared —
which is the part a per-language suite cannot do, because three suites can each
have a `failover` test and disagree about what failover means.

**A driver reports; it never judges.** Three clients that each decided whether
they had passed would be three opinions. There is one oracle and three answers.

It was verified by breaking a client on purpose: the Python driver made to stop
one page early produced `documents_seen: expected 250, observed 200` while the
other two passed. A suite that has never gone red is a suite nobody has tested.

**And it found something on its first full run**: the specification had claimed
since M10 task 1 that collection creation is idempotent, while the server has
always answered `409`. Nothing had caught it because every test in the
repository created a collection exactly once.

---

## One application, written three times

[`examples/`](../examples/README.md) holds `shelf`, a small library catalogue
in all three languages. It uses the features KimmyDB exists for rather than the
ones every database has: bulk insert in one commit, cursor paging, an
aggregation, semantic search over client-supplied vectors, and a change stream
watching the collection while it is written to.

```bash
./examples/run-all.sh     # all three, against a fresh node
```

The deferred decision that became M10 named *running KimmyDB on something real*
as the trigger for judging whether this direction was right. A snippet that
inserts one document does not test that; an application that has to find
something, page through results and react to a change does.

**The embedding is a toy and says so** — a deterministic bag-of-words hash, the
same in all three languages, so the pipeline is real without an API key or a
model download. All three produce identical scores, which is a pleasant way to
notice that nothing language-specific leaks into the protocol.

**They run in CI.** An example nobody runs rots into a document that used to be
true.

---

## Federation in the Python and Go clients: not yet

Both accept a bearer token today, which is all a federated deployment strictly
needs — obtain one from your provider, hand it over, and every route works. What
they do **not** have is the Rust client's `token_provider`: a long-lived Python
or Go application has to notice a 401 and rebuild its client rather than being
renewed underneath.

Deferred rather than forgotten, and deliberately: the callback's shape is the
part worth getting right per language — an `async def` returning a string is not
the same decision as a `func() (string, error)` versus a `TokenSource` in the
shape `golang.org/x/oauth2` already established — and the conformance suite
drives all three clients against a real server, so proving the behaviour needs a
stub identity provider in the harness. That is a piece of work with its own
design, not a translation of this one.

Until then, both clients are usable against a federated node exactly as they are
against a local one, with a token from `kimmy login` or from the
application's own provider.

---

## The CLI is a consumer, not a second implementation

`kimmy` speaks through `kimmy-client` — nothing in it builds a URL to a *node*
or reads a status code from one. That is what proves the library is pleasant
rather than merely present, and it worked: converting it found a public API that
forced consumers to depend on `reqwest`, a login that could not fail over, and a
missing `create-collection` that made a fresh database unusable from the tool.

The one exception is `kimmy login`, which talks
to the **identity provider** rather than to a node. That is a different service
with a different protocol, and putting it behind `kimmy-client` would give every
application linking the crate an OAuth2 implementation it did not ask for.

[CLI](cli.md) has the commands.

---

## Next

- [`openapi.yaml`](openapi.yaml) — the specification these are written against
- [Compatibility](compatibility.md) — what `/v1` promises, and what a correct client must tolerate
- [HTTP API](http-api.md) — the reference
