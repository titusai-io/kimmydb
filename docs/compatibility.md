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
- **Not depend on field order in the envelope, on the absence of a field, or
  on a message string.** `message` is prose for a human and changes freely.
  This is about the response's own fields — `count`, `documents`, `error` —
  not the documents inside it: a stored document's fields come back in the
  order they were written, as BSON keeps them (ADR-120), and a client may rely
  on that.

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
| Tightening a refusal so something that worked now fails | With one recorded exception: a **capacity ceiling** on a request the node could only answer by holding more than it may — the 10,000-document sort window of [ADR-098](decisions.md), the same class as `MAX_LIMIT` and the body limit — may be introduced in a `0.MINOR` release with a release note. A request that could take the node, and every other client's requests, down with it was never one this promise could keep at every collection size |
| Changing what a route *means* while keeping its shape | The worst kind, because nothing observable changes for a client until its data is wrong |
| Changing a default | A client that omitted the field gets different behaviour without asking for it |

**Recorded exceptions, 0.51.0 ([ADR-238](decisions.md)).** The release makes
these changes on purpose, under Breaking in the CHANGELOG, each because the old behaviour
was wrong in a way no client could have depended on safely: the index's `undecidable`
field, `explain`'s `undecidableCandidates` and the metric `kimmy_index_undecidable_total`
are removed (a decimal is decided by its filter, so there is nothing to count); `$sum` and
`$avg` over a `Decimal128` answer `400` where they skipped the decimal and answered a wrong
total; a `$numberDecimal` text that is not a number is a `400` where it was stored as one;
a partial unique index enforces on a `Decimal128` at its filter path, so a write that was
accepted can be refused; and a stored `Decimal128` is compared by its exact value, which
changes which documents a filter selects. A client that read the removed field or relied
on one of these must change; none of them changes a route's shape.

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
  "commit": "416672024e20",
  "node": "3e98120f-66df-4cf0-9fa0-690e3d57fcea",
  "capabilities": ["aggregation", "backup", "bulk-insert", "..."]
}
```

**Branch on `capabilities`, not on `version`.** A version number only answers
"can I use this feature" if the client also carries a table mapping versions to
features — the table this endpoint exists to replace. `version` is for
operators, `commit` names the exact build when two nodes claim the same
version, and `node` says which machine answered.

The capability set is closed by an enum in the server, and the contract test
holds it to the list in `openapi.yaml`, so a node cannot advertise something it
does not have.

---

## A cluster is one version

Cluster protocol 2 makes a member refuse a peer of another release at the
handshake, so every member of a cluster answers `/v1/version` alike. A request
that uses a field a node does not know is refused with `422` and `bad_request`,
as every request body rejects unknown fields ([ADR-121](decisions.md)); a query
string is held to the same rule at `400` ([ADR-124](decisions.md)).

**Storage schema 7 since 0.58.0 ([ADR-259](decisions.md#adr-259--the-arrays-a-compound-index-spans-are-counted-by-their-place-in-the-path-and-0580-is-a-wipe-release)): 0.58.0 refuses a store or backup written by 0.57 or older, by name, and the upgrade is stop all, wipe, start all, reseed.**

**Storage schema 6 and cluster protocol 2 (0.53.0, [ADR-251](decisions.md#adr-251--binary-_id-subtypes)) make 0.53.0 the release that does not upgrade a store.** A binary `_id` is keyed with its subtype, so a schema-5 store's keys mean something else. 0.53.0 refuses, by name, a data directory or a backup written by 0.52 or older, and a peer that speaks no cluster protocol 2, and the schema 1 to 5 migrations are removed. The upgrade is stop all, wipe, start all, reseed ([Upgrades](operations.md#upgrades)); there is no mixed-version cluster. 0.52 refuses a schema-6 store, so the way back is to start 0.52.x on the data directories moved aside.

**There is no mixed-version cluster to test.** A member refuses a peer that speaks
another cluster protocol at the handshake (the protocol has been 2 since 0.53.0, so it does not tell 0.53 to 0.58
apart); what keeps releases from mixing is the store, which a build of another
storage schema refuses by name. A release that changes the cluster wire bumps the
protocol, and then needs no mixed-version test and no default for a field an older
member would omit: a new field is written by every member and required, and a
frame without it is malformed. The tests beside the protocol hold that.

---

## Release versioning — what the build number promises

Settled by [ADR-062](decisions.md). The protocol promise above is about the
*wire*; this section is about the *artifacts*, and the two are deliberately
decoupled — `/v1` can outlive many build versions, and a build version says
nothing about which protocol majors a node serves.

**Pre-1.0 SemVer.** While the workspace is `0.x`:

- **`0.MINOR` bumps** carry features — and are the only place a breaking
  change of any kind may land: a config key renamed, a CLI flag retired, a
  default changed, an internal wire format touched. Read the release notes
  before crossing a minor.
- **`0.x.PATCH` bumps** are fixes only. Upgrading across a patch must never
  require reading anything.

The `/v1` promise holds *through* every one of these: "breaking" here means
things the protocol contract does not cover — operator surface, packaging,
the cluster wire.

**A resume token's format is a client contract, and changes only in a
`0.MINOR`.** A token is opaque, but clients store it across restarts and hand
it to whichever node answers, so a new format breaks anything holding the old
one. It ships in a `0.MINOR` release, with a release note. The single-stamp token
0.30.0 replaced ([ADR-173](decisions.md)) was the first case; it is refused as
malformed.

**A downgrade across a storage boundary is refused before anything is
written.** A storage schema change, a redb major.minor bump or a redb
file-format change is a boundary. From 0.36.0, a build started on a store that
a newer build has written across one of these boundaries refuses to start, and
leaves the store unchanged ([ADR-190](decisions.md)). A redb patch bump is not
a boundary. The way back across a
boundary is a logical backup and restore; see
[operations.md](operations.md#rolling-back-and-kimmyformat).

**One version, two binaries.** `[workspace.package] version` in the root
`Cargo.toml` is the single source of truth. `kimmyd` and `kimmy` are always
released together, carry the same number, and a test pins that neither can
drift from the workspace. There is no per-crate versioning and no promise
about the library crates' APIs — the crates are not published to crates.io,
and the released artifacts are the two binaries and the container image.

**Releases are tag-driven.** Pushing `v{MAJOR.MINOR.PATCH}` builds and
publishes everything; nothing is released by hand. `GET /v1/version` reports
the build version and the exact commit, so a running node can always be
matched to its release.

**A tag is a deliberate act, not a step in the merge ritual.** There is no
schedule and nothing tags automatically: a release happens when there is
something an operator needs, and `## Unreleased` in
[CHANGELOG.md](../CHANGELOG.md) accumulates until then.

This is worth stating because the alternative habit — a tag per round of merges
— is easy to fall into, and the cost is paid by the people downstream rather
than by the person tagging. Every tag is a published release with notes, a
container image and a set of SBOMs; twenty of those in a week is version churn
that tells a reader nothing about which one they should be running. Trying a
change on a cluster does **not** need a tag: the container image workflow can
be dispatched by hand for that.

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
- Every request shape is closed — the "every request body rejects unknown
  fields" claim above — so a client validating against the document before
  sending is told what the server will refuse
  (`every_request_shape_is_closed`).

**Written down and not checked:**

- The six-month `/v2` window. A promise about calendar time cannot be a test.
- "Changing what a route means" being breaking. Nothing can detect a change of
  meaning that preserves shape; that one is a review responsibility, and it is
  the reason a route's *semantics* belong in the specification's prose rather
  than only in its schemas.

---

## What each operation guarantees

The one table to read before relying on a write. The short form: **ACID at
the granularity of one request on one node, BASE everywhere else** — AP by
design. Every row below is enforced by one redb write transaction on the
accepting node; nothing below coordinates across nodes, and nothing spans
two requests.

| Operation | Guarantee | Enforced by | Defended by |
|---|---|---|---|
| `POST .../docs`, `PUT .../docs/{id}`, `DELETE .../docs/{id}` | Atomic and durable at commit: the document, its index entries and its oplog entry land together or not at all; the commit is an fsync — its own under `durable`, a shared one it waited for under `coalesced` (ADR-088) | One write transaction in `docs.rs` (`insert`, `replace`, `delete_guarded`) | `one_insert_is_one_commit` |
| `POST .../bulk` (`insert_many`) | **All or nothing.** A duplicate `_id` anywhere in the batch inserts nothing, mints no oplog entry, and does not move the clock | One transaction for the whole batch (`insert_in_txn`) | `a_batch_is_one_commit_however_many_documents_it_holds`, `a_bulk_insert_with_a_duplicate_id_inserts_nothing` |
| A sync batch arriving from a peer | **One commit per run** of consecutive document entries: the run's documents, index entries, oplog entries and the batch's witnessed vector land together; a schema change in the batch ends one run and starts the next, so a DDL-free batch is one commit and one fsync (ADR-119). Last-writer-wins per document inside the run; a losing entry writes nothing and the run continues. The one other commit a batch causes on a member is the embedding worker's position checkpoint, at most one per second however many entries arrive (ADR-125) | `sync.rs` `apply_batch` → `apply_remote_in_txn` | `a_replicated_batch_is_one_commit_however_many_entries_it_holds`, `a_schema_change_mid_batch_splits_it_into_runs_that_commit_once_each`, `a_superseded_entry_mid_batch_does_not_stop_the_rest_of_the_run` |
| `find`, `count`, `GET .../docs/{id}`, aggregation | Snapshot-isolated per request: one read transaction, so a query never sees half a write | redb read transaction per query | Snapshot isolation is redb's; the executor opens exactly one read transaction per request |
| `find_and_modify` | Atomic claim-and-return: filter, sort, operators and write inside one write transaction; two callers never claim the same document | `modify.rs` `find_and_modify` → `modify_in_txn` | `concurrent_claims_never_hand_out_the_same_job_twice` |
| `update` / `delete` by filter, single document | Atomic read-modify-write on the accepting node: the operators run on the image the write transaction holds, so concurrent `$inc`s all land (ADR-083) | `modify.rs` `modify_where` — the same body as `find_and_modify` | `concurrent_increments_through_update_are_all_kept`, `concurrent_increments_are_all_kept` |
| `aggregate` ending in `$merge` / `$out` | **Atomic per chunk** of `storage.multi_chunk_docs`, not atomic overall; a later failure is `500 partially_applied` (ADR-255) | `merge.rs` `write_documents` | `a_failure_in_a_later_chunk_leaves_the_earlier_chunks_written_and_says_so`, `chunks_commit_once_each_and_the_boundary_is_exact` |
| `update` / `delete` with `multi: true` | **Atomic per chunk** of `storage.multi_chunk_docs` documents (default 1,000): each chunk one transaction and one fsync, the writer released between chunks, a later failure leaving earlier chunks committed; `commits` reports how many landed (ADR-086) | `modify_where`, resuming after the last key written | `a_multi_write_commits_in_chunks_of_the_configured_size`, `a_failure_in_a_later_chunk_leaves_the_earlier_chunks_committed`, `a_filtered_write_past_the_cap_commits_in_chunks_rather_than_refusing`, `a_multi_update_reports_its_commits` |
| Unique indexes | Enforced on the accepting node before the write is acknowledged; **across nodes, detected after the fact** — a collision that replicated in surfaces as a `UniqueViolation` oplog entry and a counter (ADR-029) | `index::maintain` locally; `sync.rs` on merge | `unique_violation_entries_are_never_sent` and the index tests |
| Any single-document write with `if_stamp` | **Compare-and-set on the accepting node**: written only if the document is still at the named version, else `409 stale` and nothing written — no oplog entry, no event (ADR-084) | The stamp check inside `modify_in_txn` / `replace_if` / `delete_where` | `racing_conditional_writers_produce_exactly_one_winner`, `a_stale_write_leaves_the_document_the_oplog_and_the_commit_count_alone`, `racing_conditional_claims_have_exactly_one_winner` |
| Anything across two requests | **No guarantee.** There are no multi-request transactions; two writes are two commits, and a reader may see the state between them | — | — |
| The cluster | Basically available, soft state, eventually consistent: every node accepts writes, anti-entropy carries the oplog, whole-document last-writer-wins on a hybrid logical clock. Read-your-writes holds only on the node written to; convergence holds while a partition is shorter than tombstone retention | `kimmy-cluster` + `sync.rs` | `two_engines_converge_after_one_round`, `conflicting_writes_converge_to_the_same_document`, `three_nodes_converge_through_a_middle_peer` |

**Durability mechanism, stated once.** Under the default `durable` class
every commit runs redb's `Durability::Immediate` and does not return until
the data is fsynced. Under `coalesced` (ADR-088) a commit is written with
`Durability::None` and then waits at a barrier for the next shared fsync
before its response returns — the only `set_durability` calls in the
workspace are that switch and the barrier's own flush. Either way a commit
has reached the disk when the caller hears about it: that is why the
per-commit write rate in [Benchmarks](benchmarks.md) is a physical floor
rather than a tuning problem, and why a process killed mid-flight loses
nothing it acknowledged.

**What this rules out.** Cross-node unique constraints, counters that are
correct *across* nodes under a partition (each node's increments are correct;
concurrent increments on two nodes resolve by LWW), and check-then-act across
requests. The atomic tools are `find_and_modify` and `update` for one
document, `insert_many` and `multi: true` for one bounded batch, and
`if_stamp` for check-then-act on one document across two requests — on the
node that holds it.

Where the same facts are told from another angle: [Storage](storage.md)
(the durability table, from the engine's side),
[Time and conflicts](time-and-conflicts.md) (the cluster's side), and the
README's "Data guarantees" section (the summary). This table is the authority
when they disagree.

---

## Next

- [HTTP API](http-api.md) — the reference
- [`openapi.yaml`](openapi.yaml) — the specification
- [Decisions](decisions.md) — ADR-055, ADR-056, ADR-057, ADR-058, ADR-062
