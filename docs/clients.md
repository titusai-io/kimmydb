# Clients

[← Documentation index](README.md)

What a client of KimmyDB is expected to do. The protocol is
[`openapi.yaml`](openapi.yaml), and anyone may write a client against it.

## Client libraries

The first-party client libraries (Rust, Python and Go) and the `kimmy`
terminal client moved out of this repository into their own
([ADR-193](decisions.md)). They are **frozen, and not currently
distributed**: they are not updated for server changes, and they will be
brought up to date together once the server is stable and performing well.
Until then the HTTP API is the interface — [HTTP API](http-api.md) is the
reference, and this page is what a client, first-party or not, has to do to
use it correctly.

The conformance scenarios stay here as the protocol's contract:
[`clients/conformance/scenarios.json`](../clients/conformance/scenarios.json)
declares what a correct client observes, and `run.py` compares drivers
against it. The drivers live with the clients, and this repository's CI no
longer runs them.

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
not happen. A write whose outcome the server cannot know is answered
`500 outcome_unknown` with `retry: verify`, and a write whose connection
dropped after the request was sent is in the same state: see
[retrying after an unknown outcome](#retrying-after-an-unknown-outcome). Reads
move freely; writes are the caller's decision.

**Resume change streams from the last token seen.** A token resumes on any node:
exactly on the node that issued it, and on any other with every event the stream
had not sent but possibly some it had (up to 1,024 once the stream had caught
up, and everything since it opened before then), so a consumer that fails over
must tolerate a repeated event. Verified on a real cluster with a stream cut mid-flow
and resumed on each kind of node ([ADR-173](decisions.md)).

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

### Retrying after an unknown outcome

A write has three outcomes, not two: it happened, it did not, or **it is not
known whether it did**. The third comes two ways:

- **`500 outcome_unknown`, `retry: verify`.** The write reached the storage
  engine's durability step, and the step failed. The pages may be on disk, and
  a node that restarts and repairs its file keeps them. So the write may be
  there, and if it is, it replicates.
- **A connection that closed after the request was fully sent, with no
  answer.** This is the more common form. A node whose own fsync fails stops at
  once (ADR-188), without answering. A crash, an OOM kill or a restart looks the
  same. A connection that failed **before** the request was sent (refused, a
  failed TLS handshake) is not this case: that request cannot have been applied.

**What to do:**

- **Retry only a write that is idempotent**, where applying it twice is the same
  as once:
  - an insert with a **client-assigned `_id`**, where the second attempt's
    `duplicate_key` then means *it happened*;
  - an update or delete carrying **`if_stamp`**, where `stale` then means *it
    happened, or something else did: re-read*;
  - a replace to a known, complete value.
- **Otherwise read the target back first**, and decide from what you find.
  Read it from **the same node once it serves again**, or from another member
  once it has caught up. A read on another member that finds nothing is **not**
  proof the write failed: the only copy may be on the node that stopped, until
  it restarts and serves its peers.
- **Never blindly resend** an insert with a server-assigned `_id`, or an
  `$inc`: that is how one write becomes two.
- **Never resend a multi-document write automatically**, whatever it was
  answered: a `multi: true` update or delete, or a database drop. It commits
  in several transactions, and a failure after the first is answered
  `partially_applied` with what landed; a resend re-applies that part. Build a
  non-idempotent one with a marker so it can be sent again safely: see
  [a request that was partly applied](http-api.md#a-request-that-was-partly-applied).
- **Proxies and service meshes:** a layer that retries a request on a `5xx` or
  a reset must not retry non-idempotent writes. See
  [Operations](operations.md#a-write-whose-outcome-is-unknown).

**The planned cure** is server-side idempotency keys: a key the client sends
with a write, so that the server applies any retry of it only once. With them,
every write becomes safe to retry after an unknown outcome. They are planned,
on the [roadmap](roadmap.md#planned-not-scheduled) with no version set, and not
in this release.

---

## Next

- [`openapi.yaml`](openapi.yaml) — the specification a client is written against
- [Compatibility](compatibility.md) — what `/v1` promises, and what a correct client must tolerate
- [HTTP API](http-api.md) — the reference
