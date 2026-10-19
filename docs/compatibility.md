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

**Recorded exception, 0.52.0 ([ADR-240](decisions.md)).** `Idempotency-Key` is refused
with `400` on every route that does not take it, where earlier releases ignored it, and
on every route that does while the member does not take keys (in a cluster, until the
operator sets `cluster.idempotency_keys`), and with `503 peer_unheard` while the only
cause is a live member not heard from yet, within a lease. A tightened refusal, made on purpose: a
client that sent the header to a route that ignored it believed it was protected and
was not, which is worse than being told no (the reasoning of ADR-121 and ADR-124). A
client or proxy that stamps the header on every request must stop, or send it only to
the routes that take it, on a member that lists `idempotency-keys`.

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

## A cluster is not one version

Nodes are upgraded one at a time, and a client that fails over between nodes may
reach an older node immediately after a newer one. Three consequences, all of which
follow from the rules above rather than adding to them:

- **Ask each node.** `/v1/version` describes the node that answered it, and the
  answer is worth caching per node rather than per cluster.
- **A request using a field an older node does not know is refused**, with
  `422` and `bad_request` — every request body rejects unknown fields
  ([ADR-121](decisions.md)), so a typo is an error rather than a silent
  no-op. A query string is held to the same rule at `400`: a parameter the
  route does not define, or any query string on a route that takes none
  ([ADR-124](decisions.md)). That refusal is *correct*, and it is why
  capability discovery exists: check first, do not send and hope.
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

**Replication frames are not SWIM datagrams, and break differently.** The
replication protocol is BSON, which *is* self-describing, so changing the
*shape of a message variant* is a decode error on arrival rather than a value
misread as something else. That is narrower than "this wire is safe": messages
are plain serde with no `deny_unknown_fields`, so an **added optional field**
crosses versions silently in both directions, which is exactly how
`AskEntries::held` shipped ([ADR-097](decisions.md)). A change is loud only
when it cannot be read as the old shape.

The first break of that loud kind — `Message::Entries` becoming a struct
variant to carry where the sender's window ended ([ADR-127](decisions.md)) — is
therefore the first cluster-wire change that is safe to **roll**: mixed members
fail each other's sync rounds, `kimmy_sync_failures_total` rises on both sides,
membership and failure detection are untouched, and anti-entropy reconciles
when the roll finishes. Being unreadable rather than misreadable is the
property being bought, and it is why this one does not join the
stop-the-cluster list. Finish inside `storage.oplog_retention_secs` — see
[Operations](operations.md).

**A database drop (0.47.0, [ADR-229](decisions.md)) changes what a route means,
and a roll has a clock on it.** `DELETE /v1/db/{db}` now also deletes, on every
member, the collections created before it that the answering member had not
received. A 0.46.0 member in a mixed cluster does not act on the drop's tombstone
(it applies it as the drop of a collection it never held) and keeps serving a
collection the dropper never held until it restarts on 0.47.0, which buries it.
That restart needs the tombstone, which every member collects after
`storage.tombstone_retention_secs`: **a roll or rollback must finish inside it of
any database drop made during it**, or the collection, and with it the database,
comes back on every member.

**A database drop keeps what is stamped after it (0.50.0, [ADR-236](decisions.md)),
which changes what the route means again.** A write made into a collection of the
database after the drop's stamp, through a member that had not received the drop,
is kept, as for a collection's drop since 0.49.0, so the collection and the database
can come back holding only such writes. A 0.49.x member in a mixed cluster buries such
a collection with the write, and refuses the kept collection from a 0.50.0 member
until it is upgraded; **a store rolled back to 0.49.x buries, at its next start, every
collection 0.50.0 kept under a database drop, with the writes it kept**, so the members
differ until they run one version. Two other differences outlast the roll. A creation
in a drop's own millisecond that a 0.49.x member applied stays where it was applied
(0.50.0 gives such a tie to the drop, and nothing repairs the life a 0.49.x member
kept). And a 0.49.x member that buried a collection its database's drop covered,
indexes and all, still serves an index or vector change stamped after the drop for as
long as it retains the entry: a 0.50.0 member that lacks the collection stops at it
once, and the 0.49.x member's snapshot of the collection, which holds nothing, lets it
pass. No table, schema, wire field, backup tag or entry kind changed; a 0.50.0 member
relays a drop it ignored and a creation its database's drop refused, as entries a
0.49.x member reads and judges as it would the originals.

**Decimal128 and storage schema 5 (0.51.0, [ADR-238](decisions.md)) make 0.51.0 a
rollback boundary to 0.50.x and 0.49.x, and give the roll a declaration.** Every number
key on disk changes, so the first start migrates the store before it serves (about 16 to 19 minutes
for 2,000,000 documents with integer `_id`s and three indexes on the lab host) and, from the
moment it raises the sidecar, a 0.50 or 0.49 build refuses the store (leaving `kimmy.redb` and `kimmy.format` as they were, though it rewrites `kimmy.last-exit` to record the failed start); the way back is the
backup taken before the upgrade. The wire does not change shape: every key a member sends
or hashes into the cut digest keeps its schema 4 form, so a 0.50 and a 0.51 member in one
cluster compare and repair as two 0.50 members do, and a 0.51 member's tombstone horizon
resets at its first start. What a 0.50 member cannot hold is not sent to it: a `Decimal128`
`_id` fails to decode on 0.50 and 0.49, and a `Decimal128` partial-index bound is refused
there and never retried, so the creation of either is refused by the operator's declaration
`cluster.decimal128` (default `false` with clustering on) and by every live peer's
advertisement. A 0.50 or 0.49 binary refuses a configuration that names the key, and
**once a decimal `_id` exists a member can no longer be rolled back by wipe and resync**
(a durable marker records it). A migration that outlasts `storage.oplog_retention_secs`
turns the member's return into a whole-database snapshot. Two differences outlast the roll
until a member migrates: its partial unique indexes enforce on decimals (and its TTL
indexes decide them by exact value), where a 0.50 member's do not.

**Idempotency keys (0.52.0, [ADR-240](decisions.md)) add optional wire fields and give
the roll a declaration.** A keyed write's record travels with the coverage a window
grants, as an optional `records` field on entries, replays, pushes and snapshot pages (a
list that may carry records riding no entry the frame ships), with an optional records
position and `records_base` in the snapshot cursor, `records_epoch` on snapshot pages, a `replay` flag and a `replay_records`
cursor on the ask for entries (and that cursor on its answer), and `idempotency_records` in
the facts block; a 0.51 member ignores all of them and sends none. That is safe to roll and not safe to
take keys across. A 0.51 member ignores the header (it applies the write, with a random
`_id`) and serves every entry from its own oplog without its record, and an entry a
member already holds is never served to it again, so a 0.52 member that pulls a keyed
write through a 0.51 one holds its effect without the record for good, and a retry there
applies it a second time. So a member takes keys only while its `cluster.idempotency_keys`
is set, which the operator declares once every member runs 0.52 and as a change of its
own, and every live member says it carries records. During the roll the members of 0.52
refuse the header with `400` and do not list `idempotency-keys`; a client branches on the
capability per node. A live member not heard from yet, after any restart or rejoin, is
the transient `503 peer_unheard` (`retry: wait`, `Retry-After`) instead, for at most a
lease, a new code that
a client reads by its retry class; the 0.51 `Decimal128` gate's refusal of such a case
changes from `400` to the same `503`. **Rolling any member back after the switch is set reopens the gap**,
and 0.51 refuses a configuration that names the key. The store keeps schema 5; the three
tables are new and 0.51 never opens them; **a backup holding a record is a rollback
boundary**, since backup tags 12 and 13 are unknown to 0.51's restore. The deliberate
mixed-version test this release names is C9, in process
(`c9_a_live_member_that_does_not_carry_records_closes_the_gate`) and over HTTP on real
processes in the `lab-hooks` build
(`lab_hook_c9_a_live_member_that_does_not_carry_records_closes_the_gate`): a member that serves
without records and sends a block without the field, as 0.51 does, closes the gate on the
others, and with the gate removed the same schedule applies a keyed `$inc` twice. Each new
field also has its version-boundary test beside the protocol.

**A member-at-a-time roll is not evidence that mixed versions work together.**
The 0.28.0 roll took about 10 s from first member to last, the cluster was
mixed for 6.8 s of it, and no sync round crossed versions in that window, so a
roll that goes cleanly has exercised nothing across the boundary. A release
that changes the cluster wire therefore names a deliberate mixed-version test
in its plan, one that keeps members on both builds long enough for rounds,
snapshots and probes to cross. Otherwise the evidence is the version-boundary
tests beside the protocol, which decode each added field as an older receiver
and as an older sender would, and not the roll.

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
one. It ships in a `0.MINOR` release, and the previous format stays accepted
for that whole minor release line; a later minor may refuse it, with a release
note. An older node does not understand a newer token and refuses it as
`400 bad_request`, so resuming on a node not yet upgraded fails until a roll
completes. The single-stamp token 0.30.0 replaced is the first case
([ADR-173](decisions.md)).

**A downgrade across a storage boundary is refused before anything is
written.** A storage schema change, a redb major.minor bump or a redb
file-format change is a boundary. From 0.36.0, a build started on a store that
a newer build has written across one of these boundaries refuses to start, and
leaves the store unchanged ([ADR-190](decisions.md)). A redb patch bump is not
a boundary. Builds before 0.36.0 do not have this check, so a rollback to one
of them still writes to the store before refusing it. The way back across a
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
| `update` / `delete` with `multi: true` | **Atomic per chunk** of `storage.multi_chunk_docs` documents (default 1,000): each chunk one transaction and one fsync, the writer released between chunks, a later failure leaving earlier chunks committed; `commits` reports how many landed (ADR-086) | `modify_where`, resuming after the last key written | `a_multi_write_commits_in_chunks_of_the_configured_size`, `a_failure_in_a_later_chunk_leaves_the_earlier_chunks_committed`, `a_filtered_write_past_the_cap_commits_in_chunks_rather_than_refusing`, `a_multi_update_reports_its_commits` |
| Unique indexes | Enforced on the accepting node before the write is acknowledged; **across nodes, detected after the fact** — a collision that replicated in surfaces as a `UniqueViolation` oplog entry and a counter (ADR-029) | `index::maintain` locally; `sync.rs` on merge | `unique_violation_entries_are_never_sent` and the index tests |
| Any single-document write with `if_stamp` | **Compare-and-set on the accepting node**: written only if the document is still at the named version, else `409 stale` and nothing written — no oplog entry, no event (ADR-084) | The stamp check inside `modify_in_txn` / `replace_if` / `delete_where` | `racing_conditional_writers_produce_exactly_one_winner`, `a_stale_write_leaves_the_document_the_oplog_and_the_commit_count_alone`, `racing_conditional_claims_have_exactly_one_winner` |
| Any document write with an `Idempotency-Key` | **Applied once on the member that took it**, and on any member that has received it: the record is in the write's own transaction, travels with the coverage a window grants and with snapshot pages, and a retry of the same request is answered from it with `Idempotency-Replayed: ?1`, writing nothing. A member that has not yet received the write applies a retry a second time. A keyed `multi: true` request resumes, writing only matches stamped below its first write (ADR-240) | `idempotency.rs`, the keyed twin of each write path; `sync.rs` and `snapshot.rs` for the records a frame or a page carries | `a_keyed_insert_replays_the_same_stamp_and_leaves_one_document`, `f1_a_keyed_insert_whose_fsync_fails_replays_after_the_repair`, `a_multi_chunk_request_across_crashes_writes_each_document_once`, `c1_a_retry_on_the_member_the_write_replicated_to_replays`, `c9_a_live_member_that_does_not_carry_records_closes_the_gate` |
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
