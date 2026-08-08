# Indexes

[← Documentation index](README.md)

Secondary indexes: how entries are maintained, how the planner picks one, and
what a unique constraint does and does not promise.

Implemented in `kimmy-storage/src/index.rs` and `kimmy-query/src/plan.rs`.

---

## Using them

```bash
curl -XPOST localhost:7878/v1/db/shop/coll/orders/indexes -H "$A" -d '{
  "fields": [ { "path": "item" }, { "path": "qty", "descending": true } ],
  "unique": false,
  "name":   "item_qty"
}'

curl        localhost:7878/v1/db/shop/coll/orders/indexes -H "$A"
curl -XDELETE localhost:7878/v1/db/shop/coll/orders/indexes/item_qty -H "$A"
```

`fields` is an **array**, not a `{field: 1}` object, deliberately: field order
decides which queries a compound index can answer, and JSON object key order is
not something a client can rely on surviving serialization.

Creating and dropping require the `admin` action; listing requires `read`.

### Did it get used?

```bash
curl -XPOST .../orders/find -H "$A" -d '{"filter":{"qty":7},"explain":true}'
```

```json
{ "documents": [ … ],
  "explain": {
    "strategy": "index",
    "index": "qty_1",
    "indexFieldsUsed": 1,
    "documentsExamined": 10,
    "documentsMatched": 10
  } }
```

`strategy` is `index` or `collectionScan`. Watching `documentsExamined` fall
while `documentsMatched` stays the same is the whole point of an index.

---

## The rule everything follows from

> An index answers **"which documents might match"**. Only the filter decides
> membership.

So every candidate an index produces is **re-checked against the full filter**
before it is returned. Skipping that recheck is how index-backed queries start
returning documents that do not match.

It follows that a computed key range may be **too wide, but never too narrow**.
A wide range costs time; a narrow one silently drops matching documents. Every
uncertain decision in the planner resolves toward "wider".

```mermaid
graph TB
    F["Filter AST"] --> P["planner"]
    P --> D{"usable index?"}
    D -->|yes| S["index range scan<br/>→ candidate doc keys"]
    D -->|no| C["collection scan"]
    S --> R["<b>re-apply the full filter</b>"]
    C --> R
    R --> O["results"]

    style R fill:#2d3748,color:#fff
```

---

## Entry maintenance

Index entries are written in the **same redb transaction** as the document they
describe. Anything less lets an index disagree with its data — which does not
crash, it returns wrong answers.

```mermaid
sequenceDiagram
    participant W as write
    participant T as one transaction

    W->>T: begin
    T->>T: check unique constraints (before any mutation)
    T->>T: remove entries derived from the OLD document
    T->>T: insert entries derived from the NEW document
    T->>T: write the document record
    T->>T: append the oplog entry
    T-->>W: commit — all of it, or none
```

A replace needs the document's **previous image** to know which entries to
remove; they are derived from the old value, not the new one. A delete removes
every entry, so a scan cannot surface a candidate whose document is gone.

Constraints are checked before anything is mutated, so a rejected write leaves
the index exactly as it was.

---

## What gets indexed

| Document | Keys produced for an index on `a` |
|---|---|
| `{a: 5}` | `5` |
| `{b: 1}` — `a` absent | `null` |
| `{a: null}` | `null` |
| `{a: ["x", "y"]}` | `"x"`, `"y"`, **and** `["x","y"]` |
| `{a: ["x", "x"]}` | `"x"`, `["x","x"]` |

**A missing field indexes as `null`.** That is what keeps `{a: null}` —
which matches missing fields — answerable from the index.

**An array indexes its elements *and* itself.** Mongo indexes only the elements;
that would leave `{tags: ["a","b"]}` (whole-array equality) with no entry, so an
index-backed query would return an *incomplete* result. Storing both costs a
little space and removes the hole.

**Values equal across numeric types share a key.** `5i32`, `5i64`, and `5.0`
encode identically, so a lookup for `5` finds a document that stored `5.0`. See
[Key Encoding](key-encoding.md).

### Compound indexes over two arrays are rejected

A compound index spanning two array fields would write the cartesian product —
`|a| × |b|` entries for a single document. Mongo rejects this; so does KimmyDB,
with a hard cap of 1,000 keys per document as a backstop.

---

## The planner

Rule-based, no cost model. The operator surface is small enough that
predictability beats sophistication.

1. Collect the predicates that must hold for **every** match.
2. For each index, count how many **leading** fields those predicates cover with
   equality, plus an optional range on the next field.
3. Take the index covering the most fields. If that count is zero, scan.

```javascript
// index: [item, qty]
{ item: "w1", qty: 3 }              // 2 fields  ✓ best
{ item: "w1", qty: { $gt: 3 } }     // 2 fields — equality prefix + range
{ item: "w1" }                      // 1 field
{ qty: 3 }                          // 0 — the leading field is unconstrained
```

### What the planner deliberately ignores

Each of these only costs selectivity, never correctness, because the filter is
re-applied regardless.

| Ignored | Why |
|---|---|
| `$or` / `$nor` branches | Their branches need not all hold; narrowing on one would drop what the other matches |
| `$ne` `$nin` `$not` | Describe what a document is *not* — no bounded range |
| `$in` | Needs a *union* of point lookups. 📋 Planned |
| `$exists` `$regex` `$size` `$all` `$elemMatch` | Cannot be turned into a key range safely |
| A range on a **descending** field | Inverted encoding swaps which end each bound belongs to; getting it backwards yields a range that is too **narrow**. Falls back to the equality prefix |
| The **second end** of a two-sided range | See below — an array field can satisfy each bound with a *different element* |

A conjunction *containing* an `$or` still uses its other conjuncts —
`{a: 1, $or: [...]}` narrows on `a == 1`, which must hold for every match.

### Only one end of a range is used

A field may hold an array — a **multikey** index — and Mongo semantics let
*different elements* satisfy each end of a range:

```javascript
// document
{ a: [2, 0] }

// matches: element 2 satisfies $gte, element 0 satisfies $lte
{ a: { $gte: 1, $lte: 1 } }
```

Neither element satisfies *both* bounds, so intersecting them into a single key
range `[1, 1]` excludes the document entirely — a range that is too narrow, and
therefore silently wrong. The planner uses **one** bound only, keeping the range
a superset; the recheck removes the extras.

> This was a real bug, found by the equivalence property test once it began
> generating two-sided ranges. It had passed hundreds of one-sided cases first —
> a one-sided range with a bad bound comes out too *wide*, which the recheck
> silently repairs.

**The cost is selectivity, not correctness.** `{qty: {$gte: 5, $lte: 9}}` scans
the index from 5 upward rather than stopping at 9. Tracking multikey-ness per
index — as MongoDB does — would let both bounds be used for fields that never
hold arrays. 📋 Planned.

### Bounds

The upper bound carries a `0xFF` sentinel so it reaches past every continuation
of the prefix. Type tags occupy `0x01..=0xF0`, and a descending component
inverts them into `0x0F..=0xFE`, so `0xFF` exceeds any possible first byte. That
is what lets a one-field equality on a two-field index still find documents
whose key carries a second component.

---

## Unique indexes

```json
{ "fields": [{ "path": "email" }], "unique": true }
```

A duplicate write is rejected with **409 `unique_violation`**. A document's own
existing entry never counts against it, so updating in place works. Creating a
unique index over data that *already* violates it is refused — building it
anyway would advertise a constraint that does not hold.

### The cross-node limit, stated plainly

> **A `local` unique index is a single-node guarantee.**

Uniqueness is a *global* invariant: deciding whether a write is legal requires
knowing what every other node is concurrently doing. That is provably not
maintainable without coordination, so a leaderless cluster that accepts writes
everywhere during a partition cannot also guarantee it. Full reasoning in
[ADR-020](decisions.md).

| `enforcement` | Reach | Availability | Status |
|---|---|---|---|
| `local` (default) | The accepting node. Cross-node violations are **detected after merge**, not prevented | Full | ✅ |
| `coordinated` | Cluster-wide, by reserving the value at its owning node | That value's writes fail while its owner is unreachable | 📋 M4 — refused with `501` until then |

**`_id` needs none of this.** Two nodes inserting the same `_id` collide on one
key and last-writer-wins converges them to a single document, so primary-key
uniqueness holds by construction.

---

## Backfill

Creating an index on a non-empty collection populates it **inside one
transaction**. The index is therefore either fully present or entirely absent —
a crash partway can never leave a half-built index silently answering queries
with incomplete results.

The cost: writes to that collection wait for the build. Acceptable at current
scale; an online backfill is a later concern.

---

## Sharp edges

**`skip` is still O(n).** Skipped documents are visited even with an index. Deep
paging remains expensive.

**Order without an explicit `sort` is unspecified.** An index-backed query and a
scan visit documents in different orders, so which documents a `limit` returns
can differ between them. Add a `sort` when the subset matters. This matches
MongoDB.

**Index ids are derived from the index name**, not allocated from a counter, so
every node in a cluster computes the same id for the same index. That is what
lets an index definition replicate at all: entry keys embed the id, so a
node-local counter would mean two nodes keying the same storage while describing
different indexes. See [ADR-032](decisions.md).

The consequence is that **recreating an index under the same name reuses its
id** — unavoidable, since "same name means same id everywhere" and "recreating
yields a fresh id" cannot both hold. Purging on drop is therefore load-bearing
rather than tidy, and `drop_index` removes the entries in the same transaction
as the metadata change.

**No index statistics.** The planner counts covered fields; it has no idea which
index is more *selective*. Two indexes covering the same number of fields are
resolved by declaration order.

---

## How this is verified

The load-bearing test asserts that **index-backed results are identical to a
full collection scan** — across a dataset built from the cases indexes most
easily get wrong (missing fields, nulls, arrays, mixed numeric types,
duplicates), and again after replace, delete, insert, and delete-then-reinsert.

Mutation testing found a real gap in that suite: the equivalence proptest
originally generated only *one-sided* ranges, where a mis-encoded bound makes the
range too **wide** and the recheck silently repairs it. Only a **two-sided**
range exposes a range that is too narrow. The generator now produces them, and a
descending-only index test was added so the planner cannot sidestep the
descending path by preferring an ascending index.

See [Testing](testing.md).

---

## Next

- [Key Encoding](key-encoding.md) — the byte ordering indexes rest on
- [Query Language](query-language.md) — the operators the planner reads
- [Decisions](decisions.md) — ADR-020 on uniqueness and coordination
