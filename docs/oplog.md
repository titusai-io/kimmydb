# The Oplog

[← Documentation index](README.md)

The operation log is the spine of KimmyDB. Understanding it explains most of the
system's shape.

Implemented in `kimmy-storage/src/engine.rs` and `kimmy-storage/src/codec.rs`.

---

## What it is

An append-only, totally-ordered record of every mutation. Each entry is written
in the **same redb transaction** as the change it describes:

```rust
docs.insert((coll_id, key), encode_doc_record(&record))?;
append_oplog(&txn, &entry)?;
txn.commit()?;          // both, or neither
self.publish(vec![entry]);   // only after commit
```

There is no window in which a document is changed but unlogged, or logged but
not applied. That is what makes the log trustworthy enough to build on.

---

## Three consumers, one log

```mermaid
graph TB
    W["Every mutation"] --> O[("Oplog<br/>(hlc ‖ node) → OplogEntry")]

    O --> C1["<b>Change streams</b><br/>WebSocket subscribers<br/>resumable by token<br/><i>✅ working</i>"]
    O --> C2["<b>Embedding pipeline</b><br/>extract → chunk → embed<br/>an ordinary subscriber<br/><i>✅ working</i>"]
    O --> C3["<b>Cluster anti-entropy</b><br/>peers pull missing ranges<br/><i>📋 M4</i>"]

    style O fill:#4a5568,color:#fff
```

Build the log once, get three features. This is the central design bet, and it
pays out in specific ways:

**Change streams need no replica set.** The log is written whether or not the
node has ever seen a peer, so a single container has fully working change
streams. In MongoDB they are a byproduct of replication and therefore require
one.

**Auto-embeddings need no scheduler.** "Keep vectors in sync with documents"
becomes "consume the log", which is already solved. The embedding worker is a
change-stream subscriber with no special privileges.

**Replication needs no separate write path.** Anti-entropy pulls oplog ranges
and applies them through `apply_remote`, which routes conflict resolution
through the same `DocRecord::merge` that local writes use.

---

## Entry format

```
┌────────┬────────────┬──────────┬───────────┬──────────────┬────────────┐
│ ver(1) │ stamp(26)  │ kind(1)  │ coll(8)   │ doc_id opt   │ body opt   │
└────────┴────────────┴──────────┴───────────┴──────────────┴────────────┘
```

```rust
struct OplogEntry {
    stamp: Stamp,               // (Hlc, NodeId) — the total order
    kind: OpKind,               // Insert | Update | Replace | Delete | Collection
    collection: CollectionId,
    doc_id: Option<DocId>,      // absent for Collection ops
    body: Option<Vec<u8>>,      // full post-image; absent for deletes
}
```

### Full post-images, not diffs

Entries carry the whole document, not a delta. Three reasons:

1. **Idempotent application.** Applying an entry is "compare stamps, overwrite" —
   safe to repeat, which matters because peers resend overlapping ranges.
2. **Order independence.** A replica can apply entries out of order and still
   converge, because each is self-contained.
3. **`fullDocument` for free.** A change-stream subscriber gets the post-image
   without a second read.

The cost is size. A collection of large documents under heavy update produces a
large log — bounded by retention, but worth knowing.

### `Some(vec![])` is not `None`

Optional fields use `u32::MAX` as the absent sentinel rather than a zero length,
so an empty body stays distinct from no body. A replicated *replace with an
empty document* must not be applied as a *delete*. Covered by
`an_empty_body_is_distinct_from_no_body`.

---

## Keys and ordering

The oplog key is a flat 26-byte slice:

```
┌──────────────────┬─────────────────┐
│  Hlc (10 bytes)  │  NodeId (16)    │
└──────────────────┴─────────────────┘
```

Because both halves are order-preserving big-endian encodings, `memcmp` on this
key yields exactly the total write order `(wall_ms, counter, node_id)`. So:

- **Scanning the log forward** is a plain redb range scan.
- **"Everything since time T"** is a range from an encoded lower bound.
- **A resume token** names a position in this key space.

```rust
pub fn read_oplog_from(&self, from: Hlc, limit: usize) -> Result<Vec<OplogEntry>>
```

### The second ordering, for change streams

That key is the **origin** stamp, which is what conflict resolution and
anti-entropy compare. But it is the wrong order for a change stream: an applied
remote entry keeps its originating stamp, so it lands *behind* the local tail,
and a subscriber already past that point would never see it.

So a second index maps a monotonic local counter — assigned when an entry is
appended *here* — to its stamp:

```
oplog_arrival:      arrival_seq -> (hlc || node)
oplog_arrival_seq:  (hlc || node) -> arrival_seq
```

```rust
pub fn read_arrival_from(&self, from: u64, limit: usize) -> Result<Vec<OplogEntry>>
```

| Consumer | Order it needs | Why |
|---|---|---|
| Conflict resolution | Origin stamp | The stamp *is* the input to last-writer-wins |
| Anti-entropy (M4) | Origin stamp, per node | Version vectors ask "what do you hold after this logical time" |
| Change streams | Arrival | Must be append-only *locally*, whatever the entry's origin |

On a single node the two orderings are identical, which is why single-node
semantics did not change. Both index tables are **derived** from the oplog:
`Engine::open` compares their size against it and rebuilds when they disagree,
so a database written before they existed is repaired rather than refused.

Resume tokens are unchanged — they still name an entry by stamp, and are
translated to an arrival position by point lookup when a stream opens. Tokens
live in *clients*, where no migration can reach them.

---

## Resume tokens

A resume token is the opaque, URL-safe encoding of an entry's `(hlc, node)`:

```
base64url( hlc(10 bytes) ‖ node(16 bytes) )   →   35 characters
```

```rust
pub fn exclusive_start(self) -> Hlc {
    self.hlc.successor()      // resuming is EXCLUSIVE of the token itself
}
```

Returning the *successor* is what makes resumption deliver each event exactly
once. Resuming *at* the token would redeliver the last event the client already
acknowledged. `successor()` is property-tested to be the immediate next value,
so nothing can sort between a timestamp and its successor — a gap there would
silently skip an event.

Tokens serialize as plain JSON strings, so clients see an opaque blob rather
than a structure they might be tempted to interpret.

---

## Schema-change entries

Five kinds carry schema changes between nodes:

| Kind | Body |
|---|---|
| `CreateCollection` | `{ db, name }` |
| `DropCollection` | `{ db, name }` |
| `CreateIndex` | the full `IndexMeta`, including its derived id |
| `DropIndex` | `{ db, collection, index }` |
| `ConfigureVectors` | `{ db, collection, config }` — `config: null` disables |

Each names its target by **name**, not only by the entry's collection id: ids
are derived from names by a hash, and a hash cannot be inverted, so a node
meeting a collection for the first time could not otherwise learn what to call
it.

They are *operations*, not a metadata snapshot, so two nodes adding different
indexes during a partition both keep theirs — see [ADR-033](decisions.md).

**Applying one must not log a new entry.** The originating entry is appended as
received; minting a local one would send the change back to the peer, which
would apply it and mint another, trading the same change forever.

The older payload-free `Collection` kind is still decoded so existing oplogs
load, but is never written and cannot be applied — it names nothing.

---

## Collection-level entries

DDL appends entries too, with `kind = Collection`, no `doc_id`, and no `body`.
They exist so that a future replica can learn about collections through the same
channel as documents.

> Consumers must filter these out when they expect document events. The
> WebSocket layer does (`kimmy-api/src/watch.rs`), and so does the change-stream
> test helper — which was itself a test bug at first: counting collection-create
> entries as document events made two tests fail confusingly.

---

## Retention

Configured by `storage.oplog_retention_secs` (default 24 h) and enforced by a
background pass every `storage.gc_interval_secs` (default 10 min). Set the
interval to `0` to disable collection; validation refuses an interval *longer*
than the retention window, since that would make the retention number mean
"the window, plus up to one interval".

Retention bounds two things:

| Bounded thing | Consequence when exceeded |
|---|---|
| How long a disconnected subscriber can be away and still resume | `410 Gone`, must resubscribe |
| How far a peer can fall behind and still catch up incrementally | Full resync needed (M4) |

The expiry check exists already:

```rust
// A token NEWER than everything on disk is fine — nothing has happened since.
// Only a token OLDER than the oldest retained entry is expired.
if token.to_stamp() < oldest { return Err(ResumeTokenExpired) }
```

Returning `410 Gone` rather than silently restarting from the beginning is
deliberate: a silent restart would hide the fact that events were missed.

### The newest entry is never collected

Whatever its age. The logical clock is not stored separately — it is resumed on
startup from the oplog tail. Collect the last entry and a restart reads an empty
log, resumes at `Hlc::ZERO`, and begins minting stamps *below* ones already on
disk; every later write to an existing document would then lose to its own older
version under last-writer-wins, silently.

An idle node is where this would bite, because no writes means every entry
eventually ages out — so the naive rule empties the log exactly when nothing is
happening to reveal the damage. See [ADR-028](decisions.md).

---

## Growth characteristics

Because entries carry full post-images:

| Workload | Log growth |
|---|---|
| Insert-heavy | ≈ data size |
| Update-heavy, large documents | ≈ document size × update count |
| Delete-heavy | Small (deletes carry no body) |

Retention bounds this, but only up to the window: a collection of 10 KB
documents updated once a second produces roughly 860 MB of log per day, so a
24-hour window means provisioning for about that much log on top of the data.
Shorten `oplog_retention_secs` to trade resume range for disk.

---

## Version vectors

A third derived index summarizes coverage: the highest `Hlc` held from each
*originating* node.

```
oplog_versions:  node id (16 bytes) -> Hlc (10 bytes)
```

Maintained on every append rather than computed, because deriving it would mean
scanning the whole log for a max-per-node — for a value read on every gossip
round. Like the arrival index it is derived state, and `Engine::open` rebuilds
it if it disagrees with the oplog.

Two peers exchange vectors and each works out what to ask for:

```rust
match mine.behind(&theirs) {
    Some(from) => apply_batch(&peer.entries_for_peer(from, limit)?)?,
    None => { /* already covered */ }
}
```

**The request is a single threshold, not a range per node.** The oplog sorts by
time first, so "everything from node N after H" would be a scan and filter,
while "everything after H" is one range read. `behind` therefore returns the
*lowest* point this node is deficient at — starting any later would silently
skip whatever the furthest-behind peer was missing.

The cost is over-fetching entries already held, and that is deliberately cheap:
`apply_remote` requires the incoming stamp to *strictly* win, so a re-delivered
entry is compared and discarded without touching the document or republishing an
event.

**Unique-violation entries are never sent.** They record what one node observed
when it merged, and every node observes the same collision independently.

### Past the horizon

A peer can ask for entries the oplog no longer holds. `oplog_collected_through`
records what retention has actually removed, so the sender can tell — and
answers `BeyondHorizon` rather than serving what is left, which would hand the
peer a silent gap: it would apply the remainder, advance its version vector, and
never learn what it missed.

The peer then asks for a **snapshot** — current state rather than history — and
adopts the sender's coverage once it is complete. That is why the version vector
is no longer derived from the oplog: coverage can be granted by a snapshot for
entries this node will never hold, so opening only ever *raises* the vector to
cover the log. See [ADR-036](decisions.md).

---

## Replication (📋 M4)

The intended flow, with the primitives that already exist marked:

```mermaid
sequenceDiagram
    participant A as Node A
    participant B as Node B

    Note over A,B: gossip carries version vectors<br/>{node_id → max_hlc_applied}
    A->>B: SWIM message + version vector
    Note over B: B sees A has entries B lacks
    B->>A: open TCP, request range (from_hlc, limit)
    A-->>B: oplog entries
    loop each entry
        B->>B: apply_remote() ✅ implemented
        Note right of B: compare stamps via merge();<br/>strictly-greater wins;<br/>witness() advances the clock
    end
```

`apply_remote` is implemented and tested, including convergence of concurrent
writes applied in opposite orders on two engines. What is missing is the
transport.

> **Known sharp edge for M4.** An applied remote entry keeps its *originating*
> stamp, so it lands in the local oplog at its original position — which may be
> **behind** the local tail. A change-stream subscriber that has already read
> past that point will not see it. Local-only streams are unaffected; this
> matters when clustering lands, and the resolution is deferred to M4 by design
> rather than by oversight.

---

## Next

- [Change Streams](change-streams.md) — the log's first consumer, in detail
- [Time & Conflicts](time-and-conflicts.md) — what orders these entries
- [Storage](storage.md) — where the log physically lives
