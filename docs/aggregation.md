# Aggregation

[← Documentation index](README.md)

Grouping, reshaping and joining, done in the database rather than in the client.

```
POST /v1/db/{db}/coll/{coll}/aggregate
{ "pipeline": [ { "$match": … }, { "$group": … } ] }
```

A pipeline is an array of stages applied in order. Each stage takes the previous
stage's output, so ordering is the main thing that decides what a pipeline
costs: **put `$match` first**, so every later stage sees less — and so an index
can answer it, which only a *leading* `$match` gets (see
[Performance](#performance)).

---

## Stages

| Stage | Notes |
|---|---|
| `$match` | The same filter language as `find` — every operator it has, `$expr`, `$mod` and `$jsonSchema` included. **Planned like `find` when it is the first stage** |
| `$project` | The same [projection language](query-language.md#sort-and-projection) as `find`, a path through an array included, **plus computed fields**. Below |
| `$addFields`, `$set` | Add computed fields, keeping everything else. Two names for one stage |
| `$replaceRoot` | `{$replaceRoot: {newRoot: <expression>}}` — the computed document becomes the document |
| `$sort` | The same [sort language](query-language.md#sort-and-projection), down to which element a key through an array reads. [Blocking](#the-memory-limit) |
| `$skip`, `$limit` | Non-negative whole numbers |
| `$unwind` | One output document per array element. Below |
| `$group` | [Blocking](#the-memory-limit). **One row per distinct key**, so an empty input produces no rows at all. Accumulators below |
| `$count` | `{$count: "name"}` — a document holding the count, **always**: over an empty input it is one document holding `0`, not no document. [Blocking](#the-memory-limit) |
| `$lookup` | Join another collection, by one key or by a sub-pipeline. **Authorized separately** |
| `$bucket`, `$bucketAuto` | A histogram: documents counted into ranges you give, or into about equal parts. [Blocking](#the-memory-limit). Below |
| `$facet` | Several sub-pipelines over the same input, answered as **one document** of arrays. Below |
| `$graphLookup` | Walk another collection (or the same one) link by link from each document, to a depth you give. **Authorized separately**. Below |
| `$merge` | Write the pipeline's output into a collection, each document matched to a stored one by `_id`. **Last stage only; writes**. [Below](#merge) |
| `$out` | Insert the pipeline's output into a collection that is **new or empty**. **Last stage only; writes**. [Below](#out) |
| `$vectorSearch` | Semantic search as the pipeline's **source**: the hits, as documents, flow into the rest of the pipeline. **First stage only**. [Below](#vectorsearch) |

**Stage operands with a fixed key set are closed; field-path maps stay
open.** `$unwind`'s document form, `$lookup`'s both forms and `$replaceRoot`
refuse a key they do not define — `400`, naming it — the same closure
[ADR-121](decisions.md) gives the request body and the shapes nested in it. A
`$match` filter and a `$project` specification are **not** put through this:
every key in either is a document field name the caller chose, not vocabulary
this database defines, so there is no fixed list to check a key against —
closing them would refuse ordinary pipelines rather than typos. See
[ADR-129](decisions.md).

**An empty input stream is not an empty answer, and `$count` and `$group`
answer it differently.** `$count` computes one number over the whole stream,
and that number is defined when the stream holds nothing: it is `0`. So a
`$count` over an empty input emits **one document holding `0`**, never no
document, and it does not matter how the stream came to be empty — a `$match`
that selected nothing, an `$unwind` that dropped every row, a `$skip` past the
end, an explicit `$limit: 0`. All of them answer `[{"n": 0}]`. `$group` over
that same stream emits **nothing at all**, `{"_id": null}` included, because it
produces one row per distinct key and an empty stream has no keys to produce a
row for.

Neither is a special case — each stage is answering its own question about a
stream that happens to hold nothing — but the difference decides how the answer
is read. `[0]["n"]` after a `$count` is always there, so a total needs no
defensive read; `[0]` after a `$group` may not be, so a pipeline ending in
`$group` can legitimately answer `[]`, and `{"_id": null}` is not a promise of
one row.

### `$graphLookup`

```json
{ "$graphLookup": { "from": "employees", "startWith": "$reportsTo",
                    "connectFromField": "reportsTo", "connectToField": "name",
                    "as": "chain", "maxDepth": 10, "depthField": "level",
                    "restrictSearchWithMatch": { "active": true } } }
```

For each input document, walks `from`: the documents whose `connectToField` equals a
`startWith` value are the first step (`level: 0`), those matched by the
`connectFromField` values of the documents just found are the next, and so on, **to
`maxDepth` steps after the first**; the documents reached, each once, are attached as
the array `as` ([ADR-254](decisions.md)). `maxDepth: 0` is the first match alone, an
equality join.

- **`maxDepth` is required**, a whole number from 0 to 1000: a walk with no bound the
  caller chose is not run (`400`). The depth is the one thing that ends a walk early,
  and it is yours; nothing else cuts a walk off. **A walk that stops at `maxDepth` is
  complete for that depth**: every document within `maxDepth` steps is there, and only
  those further out are not. A document is visited once per walk, so a cycle ends.
- **Memory is a budget, refused when over, never silently cut off.** The foreign
  collection is read once and held, as a `$lookup`'s is, and what the walks reach is held
  with the input; both count against the pipeline's document limit (100,000), `400`
  naming `$graphLookup`. **The read stops at the first document past the limit**, not
  at the end of the collection, so a very large one is refused promptly. Narrow the
  foreign side with `restrictSearchWithMatch` (it applies as the collection is read, so
  what it declines is not held or counted), the input with an earlier `$match`, or the
  walk with `maxDepth`.
- `startWith` is an expression: a value, or an array of values to start from each. A
  `null` or missing one starts nothing, a `null` in an array starts nothing, and a
  `null` link is not followed (a `null` is not a key to look up). Values match by the
  equality join's rule (numbers of every type by value, an array field by its elements).
- `connectFromField`, `connectToField`, `as` and `depthField` are field names, not
  expressions; one beginning with `$` is `400`, and so is a dotted `as` or `depthField`
  (each is the name of one field, not a path). `depthField` holds the step as a long.
  `restrictSearchWithMatch` is a filter document over the foreign documents; one it does
  not admit is neither reached nor followed. It is parsed with no `let` variables, so
  inside a `$lookup` sub-pipeline a `"$$name"` string in it is `400` (not read as the
  variable, and not the literal text), and `$expr` there cannot read one either: restrict
  with a field condition and compare against the variable in a stage after the
  `$graphLookup`.
- **Order**: step 0 first, then step 1, and so on; within a step, the order of the
  foreign scan. (Other systems leave the order unspecified; sort the array if you rely
  on it.) A key not listed above is `400`.
- Run inside a `$facet` and a `$lookup` sub-pipeline like any stage. `from` needs read
  access, like a `$lookup`'s.

### `$bucket` and `$bucketAuto`

```json
{ "$bucket": { "groupBy": "$price", "boundaries": [0, 10, 100, 1000], "default": "other",
               "output": { "n": { "$sum": 1 }, "top": { "$max": "$price" } } } }
```

**`$bucket`** puts each document in the range `[b_i, b_i+1)` its `groupBy` value
falls in (the lower boundary is included, the upper is not) and answers one
document per range **that received one**, in boundary order: `_id` is the lower
boundary as written, then the `output` accumulators (the `$group` ones), or
`count` if there is no `output` ([ADR-254](decisions.md)). A range nothing fell
in is not answered.

- **`boundaries`** are constants, at least two, **of one kind** (every number
  type is one kind and compares by value; or all strings, all dates, all
  ObjectIds, all booleans) and **strictly ascending**; anything else is `400`, as
  is a `NaN`, a `null`, an array or a document, and a string starting with `$`.
- **A value that falls in no range is refused**, `400` naming the value, **unless
  there is a `default`**: a constant, which must be below the first boundary or at
  or above the last (not inside), and which takes every such value and also a
  `null`, a missing field and a value of another kind than the boundaries'. The
  `default` bucket is answered last, with `_id` the `default`.
- `output` is a non-empty document of accumulators and **may not name `_id`**
  (`400`: it is the bucket's key); a key the stage does not define (`buckets`,
  `granularity`) is `400`. A `default` is a constant like a boundary: one beginning
  with `$` is `400`.

**`$bucketAuto`** sorts the documents by `groupBy` (the order `$sort` uses) and
cuts them into `buckets` runs of about equal size:

```json
{ "$bucketAuto": { "groupBy": "$price", "buckets": 4 } }
```

Each bucket is `{_id: {min, max}, count}` (or the `output` accumulators): `min` is
its first value and `max` the **next bucket's first value**, the last bucket's its
own largest. With `r` documents and `b` buckets left, the next bucket takes
`ceil(r / b)` documents and then every further document holding the value of its
last: **a value is never split across two buckets**, so buckets are as even as
the values allow and there are fewer than asked for when values repeat (four
distinct values in 3 buckets of `[1, 1, 1, 1, 2, 3]` are `[1×4] [2] [3]`). An
empty input answers nothing. **`granularity` is refused**, `400`: no
preferred-number series rounds the boundaries here, and a stage that ignored it
would answer boundaries the caller did not ask for. `buckets` is a whole number of
at least 1.

Both are blocking, hold the input, and count their answer against the pipeline's
document limit like `$group`.

### `$facet`

```json
{ "$facet": {
    "total":  [ { "$count": "n" } ],
    "byCity": [ { "$group": { "_id": "$city", "sum": { "$sum": "$qty" } } }, { "$sort": { "_id": 1 } } ],
    "newest": [ { "$sort": { "placed": -1 } }, { "$limit": 5 }, { "$project": { "sku": 1 } } ] } }
```

answers **one document**, `{total: [{n: 3}], byCity: [...], newest: [...]}`: each name
is a field, holding the array of documents its sub-pipeline produced
([ADR-254](decisions.md)). Every sub-pipeline reads the **same input**, the documents
that reached the stage, and none sees another's output. The stages after a `$facet`
see its one document (`$unwind` a facet and `$replaceRoot` it to continue with its
rows). Over an empty input every facet runs over nothing: `$count` still answers
`[{n: 0}]` and the others `[]`, and the answer is still one document.

- **A facet is an array of stages**, as long as you like, empty included (an empty
  one answers the input as it is). Any stage but the two below, `$lookup` among them
  (authorized like any other), and a `$lookup` `let` name is readable inside.
- **Refused, `400`, at parse**: a `$facet` inside a facet, and a `$vectorSearch`
  inside one at any depth, a `$lookup` sub-pipeline of one included (a `$vectorSearch`
  is a pipeline's source, and a facet's source is the stage's input);
  no facets; more than 64; a name that is empty, starts with `$` or holds a `.` or a
  NUL (it becomes a field of the answer); a facet that is not an array of stage
  documents. A stage that is wrong inside a facet is refused as it is anywhere.
- **Bounded, and refused when over, never cut short.** The facets' outputs together
  count against the pipeline's document limit (100,000), because they are held while
  the next facet runs; and the answer is one document, so it may not encode to more
  than a stored document may (16 MiB), `413`. The size is counted as each facet
  finishes and the request is refused at the first document that passes it, so a
  later facet does not run for an answer that cannot be returned (one document that
  holds a whole facet, a `$group` with `$push: "$$ROOT"`, counts for its bytes, not
  for one). A pipeline that needs more takes the facets as separate requests, or
  narrows each with `$limit` and `$project`.
- **`$$NOW` is the same date in every facet**: it is read once when the pipeline is
  parsed. A facet that fails fails the request.
- **Facets run one after the other.** One copy of the input is alive while a facet
  runs (the last facet takes the input itself), and the outputs of the facets that
  finished are held, up to the limits above; put the `$match` that narrows the input
  before the `$facet`.

### `$merge`

```json
[ { "$match": { "status": "paid" } },
  { "$group": { "_id": "$day", "total": { "$sum": "$amount" }, "n": { "$sum": 1 } } },
  { "$merge": { "into": "daily_sales", "on": "_id" } } ]
```

writes the pipeline's output into `daily_sales` instead of returning it ([ADR-255](decisions.md)):
a rollup that can be run again. The spec is `{into, on, whenMatched, whenNotMatched}`:

| Field | Values |
|---|---|
| `into` | A collection name in the pipeline's database, or `{db, coll}` for one in another. **Required** |
| `on` | `"_id"`, or left out. Nothing else (below) |
| `whenMatched` | What a document does when a stored one has its `_id`. `"merge"` (the default): the incoming document's top-level fields are laid over the stored one, fields only the stored one has are kept, the others overwritten. `"replace"`: the stored document is replaced whole, its `_id` kept. `"keepExisting"`: the stored document is left alone |
| `whenNotMatched` | `"insert"` (the default), or `"discard"`: the document is dropped |

A document with no `_id` (a `$project` took it out) cannot match, so it is inserted
with a generated `_id`, or discarded. Two output documents with one `_id` are applied
in order: the second sees the first. The answer carries counts and **no documents**:

```json
{ "documents": [], "count": 0,
  "merge": { "matched": 1, "inserted": 1, "replaced": 0, "merged": 1, "kept": 0,
             "discarded": 0, "commits": 1 } }
```

The counts are the decisions taken, so `matched` is always `replaced + merged + kept`;
a `merge` over a stored document that already held those values is counted `merged`
and writes nothing. `commits` is the write transactions that committed something.

- **The key is `_id`, and only `_id`.** `on` as another field, or as an array (an
  array of `["_id"]` too), is `400` with the reason: a match on another key is a read
  then a write, no uniqueness is enforced across members, and two members running the
  same `$merge` at the same time would each find no match and each insert. `_id` is
  the key the store keeps one document per.
- **Refused, `400`, each naming what and why**: `whenMatched: "fail"` and
  `whenNotMatched: "fail"` (neither can be checked across members), a pipeline-form
  `whenMatched` (an array), and `let` (it only serves the pipeline form).
- **Last stage only.** In any other position, and inside a `$facet` or a `$lookup`
  sub-pipeline, it is `400` at parse.
- **It may not write a collection the pipeline reads**: the source, and every
  `$lookup` and `$graphLookup` collection, nested sub-pipelines and `$facet` branches
  included, compared by database and collection. `400`, before anything is read. A
  pipeline that reads what it writes has no answer, because every stage reads storage
  when it runs.
- **Permissions.** `read` on the source and on every collection it joins, as for any
  aggregation, **and `write` on the target**. A target in another database (`{db,
  coll}`) needs `write` there; a grant on the same name in the pipeline's database is
  not one. **A target that does not exist is created, which needs `ddl` on it as well**:
  an insert never creates a collection, and `write` is not a grant to shape the data.
  Neither is checked after the pipeline has run: a refused request has read and written
  nothing, and a refused creation leaves nothing behind.
- **The output's `_id` must be one a document can have**: an ObjectId, string,
  integer, binary or finite decimal. Any other type, `null` (the `_id` of a `$group`
  on a key some documents lack), a document (a compound `$group` key), an array and
  a double included, is `400`. Build a compound key with `$concat` first. Every `_id`
  of the output is checked, in both stages, before the target is created and before
  the first chunk is written, so a refused output writes nothing and creates nothing.
  (A string or binary `_id` over the size cap is refused in the same place.)
- **The output is checked against the target's indexes before anything is written.**
  A document that an index of the target cannot key (parallel arrays in a compound or
  multikey index, or more than 1,000 index keys) is `400` with the message a client
  write of it gets, naming the index, the `_id` and the document's place in the output,
  and the request writes nothing; a target that does not exist yet has no index, so a
  refusal never leaves one created. It covers what is stored as it stands: every
  document of `$out`, and of a `$merge` the documents inserted because the target holds
  none with their `_id` and, under `whenMatched: "replace"`, the matched ones. A matched
  document under `merge` is stored as the stored document with the incoming fields laid
  over it, which only the write knows, so the write checks it, and a refusal there can
  come after earlier chunks committed (`partially_applied`). A document the stage never
  writes (kept, or unmatched under `whenNotMatched: "discard"`) is not checked.
- **It is the ordinary write path.** Each document is an ordinary insert or replace
  inside a write transaction: the unique and other indexes, the document and `_id` size
  caps, the oplog, change streams, webhooks, expiry and replication see the same writes
  a client's would. A document that violates a unique index is refused the way an
  insert is (`409 unique_violation`).
- **Not atomic.** The documents are written in transactions of at most
  `storage.multi_chunk_docs` (default 1,000), as a `multi: true` update is, and the
  writer is released between them. A failure aborts the chunk it is in and **leaves
  the earlier chunks written, published and replicating**; the answer is then
  `500 partially_applied`, whose `applied` holds the counts of what landed and
  `cause` why it stopped. A failure in the first chunk writes nothing and is the
  plain error (a `409` for a unique violation, `413` for a document over the cap).
  Run it again: a `$merge` that is idempotent for its documents (`merge`, `replace`,
  `keepExisting`) lands the rest when every output document carries its `_id`; a
  document without one is given a new ObjectId each time it is inserted.
- **The pipeline's output obeys the pipeline's ceilings** (100,000 documents per
  stage, [The memory limit](#the-memory-limit)); what is written obeys the stored
  document's caps.
- **`explain: true` plans it and writes nothing**: the stage is listed with
  `runs: "write"`, `into`, `on` and the modes, and the target is not created. It
  applies a run's refusals (a target the pipeline reads, a pipeline that does not
  parse) and needs the grants a run needs, `write` and `ddl` on the target included.

### `$out`

```json
[ { "$match": { "year": 2026 } },
  { "$out": "orders_2026" } ]
```

inserts the pipeline's output into `orders_2026`, or into `{db, coll}` of another
database. **The target must be new or empty, and no `_id` already stored, as this
member holds the target when the `$out` runs.** A collection that holds any live
document is `400` (`$out writes only into a new or empty collection ... Drop the
collection first, or use $merge`), and a missing one is created. A collection
emptied by deletes is empty. Across members the target can already hold documents,
and an `_id` written elsewhere meets the output by last-writer-wins, as any two
inserts of one `_id` do ([ADR-006](decisions.md)): the check is a guard against an
obvious mistake on this member, not a promise about the cluster. There is no atomic replace of a collection that holds
documents; drop it first, or `$merge` into it.

It is the insert half of `$merge`, with the same rules: last stage only, never
inside a `$facet` or a `$lookup` sub-pipeline, never a collection the pipeline
reads, `write` on the target and `ddl` when it must be created, the ordinary write
path, chunks of `storage.multi_chunk_docs` and **not atomic**. The answer is
`{"documents": [], "count": 0, "out": {"inserted": 2, "commits": 1}}`.

Two output documents with one `_id` are refused **before anything is written**
(`409 duplicate_key`, as a bulk insert refuses them, and the target is not
created). An `_id` that is already stored cannot happen in a target
this member holds as new or empty, except to a writer that slips in between the
check and the write, which is a `409` in the first chunk and `partially_applied`
after it. The `timeseries` form,
`{db, coll, timeseries: ...}`, is refused by name.

### `$project`

The flags are `find`'s projection, rule for rule — one implementation serves
both — so **a path through an array keeps the array and reduces every
element** ([Query language](query-language.md#sort-and-projection)):

```json
// Stored: { "_id": 7, "p": [ { "_id": 1, "name": "a" }, 5, { "_id": 2, "name": "b" } ] }

{ "$project": { "p._id": 1 } }
// { "_id": 7, "p": [ { "_id": 1 }, { "_id": 2 } ] }

{ "$project": { "p.name": 0 } }
// { "_id": 7, "p": [ { "_id": 1 }, 5, { "_id": 2 } ] }

{ "$project": { "p._id": 1, "count": { "$size": "$p" } } }
// { "_id": 7, "p": [ { "_id": 1 }, { "_id": 2 } ], "count": 3 }
```

A computed field reads the input document, not the projection's output, and
is written after the flags are applied. A computed field written *into* the
kept array through a named segment — `{"p._id": 1, "p.total": …}` with `p` an
array — has no one place to go and is refused, `400`, `cannot set "p.total"`,
as `$addFields` refuses it; it is not written into a document standing in for
the array. **A numeric segment is the exception, and it is a position:** a
computed field is written the way an update writes, so `{"p._id": 1, "p.0":
"$qty"}` over `p: [{_id: 1}, {_id: 2}]` and `qty: 2` replaces element `0` and
answers `p: [2, {_id: 2}]`, exactly as `$addFields` with `"p.0"` does. The
flags beside it read `p.0` as a field name; the write does not.

### `$unwind`

```json
{ "$unwind": "$tags" }
{ "$unwind": { "path": "$tags", "preserveNullAndEmptyArrays": true, "includeArrayIndex": "i" } }
```

The shorthand string form takes only a path. The document form takes:

| Key | Default | Meaning |
|---|---|---|
| `path` | required | The field to expand, `"$"`-prefixed |
| `preserveNullAndEmptyArrays` | `false` | Keep a document whose path is missing, `null` or an empty array, as one row with the path unset, instead of dropping it. Wrong-typed is a `400`, not read as `false` |
| `includeArrayIndex` | none | The name of a field to hold the position of the element that produced each row, or `null` on a row that was not produced by fanning one out. Cannot begin with `$` (this language cannot read such a field back) or name `path` itself (it would overwrite the unwound element) |

**A path that crosses an array *by name* is refused outright, before
anything at the far end of it is even read — see [ADR-130](decisions.md) and
[Arrays](#arrays).** `$unwind` needs a single place to write the expanded
element back to. Whenever a non-terminal segment of `path` is itself an
array on a given document, and the segment after it **names a field rather
than a numeric position** — `x.b` where `x` holds an array, at any length
down to one — there is no such place, and the request is refused, `400`,
naming `$unwind` and the path. This holds regardless of what that array
turns out to contain: a scalar, another array, an empty array, nothing —
none of it changes the answer, because the refusal is decided by the
document's *shape* along the path, before `$unwind` looks at what is there
to unwind.

**A numeric segment addresses a position instead, and is not refused.**
`$unwind: "$a.0"` writes into element `0` of `a` directly — a single place,
the same as any array write by index — and reads it the same way `find` and
every other field-path option here does: a numeric segment is read as
*both* an index and a field literally named that number ([Arrays](#arrays)
has the full rule and the one place it disagrees with an expression path).
So `$unwind: "$a.0.b"` over `a: [{b: [1, 2]}, {b: [3]}]` does not cross
anything by this rule — it finds `a.0.b` as `[1, 2]` (element `0`'s `b`) and
unwinds it into **two rows, `a[0].b` replaced by `1` then by `2`, with the
rest of `a` untouched** — `[{b: 1}, {b: [3]}]`, then `[{b: 2}, {b: [3]}]`.
This is a genuine difference from a computed expression over the identical
path: `{$addFields: {r: "$a.0.b"}}` reads `"0"` as a field name only, finds
no element actually named `"0"`, and gives `r: []`.

**This makes the refusal data-dependent: whether a pipeline is legal at all
depends on the documents it meets, not on the pipeline text.** The same
`{"$unwind": "$a.b"}` answers `200` for every document where `a` is a plain
subdocument and `400` for any document where `a` is an array — a schemaless
collection can hold both shapes under the same field name, so a pipeline
that has run correctly for months can start answering `400` the day an
ordinary write adds one document shaped that way, with nothing about the
pipeline having changed. And it fails **the whole request**, not just the
offending document: one such document among a thousand others refuses
every row, because the refusal is a stage-level error, not a per-document
skip. A field a pipeline unwinds should be one every document in the
collection either holds as a plain field or never holds as an array at all.

For every document where `path` does not cross an array this way, what is
found there decides what happens:

| Value at `path` | Without `preserveNullAndEmptyArrays` | With it |
|---|---|---|
| A non-empty array | One row per element, `path` set to that element | Same |
| Missing, `null`, or `[]` | Dropped | Kept once, `path` unset (removed) |
| Any other scalar | One row, unchanged — the value unwinds to itself | Same |

`includeArrayIndex` is `null` on every row in the second and third cases: a
row that was not produced by an array element has no index to report.

### Accumulators

`$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, `$addToSet`.

```json
[ { "$match":  { "status": "shipped" } },
  { "$group":  { "_id": "$city",
                 "revenue": { "$sum": "$total" },
                 "orders":  { "$sum": 1 },
                 "biggest": { "$max": "$total" } } },
  { "$sort":   { "revenue": -1 } },
  { "$limit":  10 } ]
```

`{"_id": null}` groups everything into one bucket.

**A missing field is `null` to every accumulator**, not a value they never
see — a missing field is null everywhere in this language — so what each one
does with `null` is also what it does with a field half the collection lacks.
That is where they differ, and the differences are the ones a report gets
wrong quietly:

| | Over non-numeric values | Over `null` and missing | When nothing was usable |
|---|---|---|---|
| `$sum` | **Ignored** — a string, a bool, an array, a document contributes nothing. A field holding `42` on some documents and `"42"` on others sums only the first kind. **A group that holds a `Decimal128` is summed exactly** ([Decimal128 arithmetic](#decimal128-arithmetic)): a `Decimal128`, rounded once to 34 digits, the same bytes in any document order; an array or a document holding one is ignored like any other | Ignored | `0` |
| `$avg` | Ignored, in the numerator **and** the count — a field present on half the documents gives the mean of that half, not half the mean. **A group that holds a `Decimal128` is averaged exactly**: the exact sum divided by the count of numeric values, rounded once | Ignored | `null` |
| `$min` `$max` | **Compared**, across types, in the [canonical order](key-encoding.md#type-ordering) — numbers below strings below documents below arrays below binary below ObjectIds below bools below dates | **Skipped**, both of them | `null` |
| `$first` `$last` | Taken as they are | Taken as they are — `null` is a value here | — every group has a first and a last |
| `$push` `$addToSet` | Taken as they are | Appended as `null`; `$addToSet` keeps one of them, and `undefined` is that same member | — nothing is unusable |

`$first` evaluates its expression for the first document of a group only, and
`$last` fails only when the last document's value cannot be evaluated: an
earlier document's value is not part of the answer, so its failure is not
either ([ADR-211](decisions.md)).

Two consequences worth spelling out. **`$sum` and `$avg` disagree about a
group with nothing to work on**: `0` against `null`, because a total of
nothing is zero and a mean of nothing is not a number — so a `$sum` reading
`0` cannot be told apart from a genuine total of zero, where `$avg` says
plainly that it had nothing. And **`$min`/`$max` over a mixed-type field
answer across types rather than refusing**, so the maximum of a field holding
integers, strings, arrays and booleans is a boolean — the highest rank in the
order, not the largest number. Neither is a bug to work around; both are
reasons to `$match` the type you mean first, or to `$group` after a
`{"$type": …}` filter.

Both of those are about a group that **exists** and had nothing usable in it.
A `$group` whose whole input is empty has no group to report at all — not a
bucket holding `0`, no bucket — while a `$count` over the same input still
answers one document holding `0`. See [Stages](#stages).

`$sum` **stays integral** while every value it has seen is an integer,
promoting to a double only when a double arrives or an `i64` sum would
overflow. `$avg` is a double whenever it has an answer at all — it never
returns an integer, and the one thing it returns that is not a double is the
`null` above. `$addToSet` identifies its members the way `$group` identifies a bucket
([ADR-186](decisions.md#adr-186--a-sets-members-are-identified-the-way-group-identifies-a-bucket)),
keeping the first of each in input order: `5`, `5.0`, `{"$numberLong": "5"}`
and `{"$numberDecimal": "5.00"}` are one member, every `NaN` (a decimal one
too) is one, every zero (`0.0`, `-0.0`, a decimal zero of any exponent) is one,
and `null` and `undefined` are one. `0.1` the decimal and `0.1` the double are
two members: they are different numbers. Two documents are one member only with
their keys in the same order, and a decimal nested in a document or array is
identified by its value as any nested number is. `$group` buckets by the same
identity, and a bucket's `_id` is the first member seen: `$group` over
`[Decimal128("1.00"), 1]` reports `_id: Decimal128("1.00")`. `$max` and `$min`
rank a decimal by its value: over `[Decimal128("1.5"), 5]`, `$max` is `5`.

---

## Expressions

Anywhere a value is computed — a `$project` field, `$addFields`, `$replaceRoot`,
a `$group` key, an accumulator's argument — takes an **expression**, not just a
field path.

| Group | Operators |
|---|---|
| Arithmetic | `$add` `$subtract` `$multiply` `$divide` `$mod` |
| Math | `$abs` `$ceil` `$floor` `$round` `$trunc` `$pow` `$sqrt` `$exp` `$ln` `$log10` — see [Math](#math) |
| Strings | `$concat` `$toUpper` `$toLower` `$substr` (`$substrCP`) `$split` `$strLenCP` |
| Conditional | `$cond` `$ifNull` `$switch` |
| Comparison | `$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$cmp` |
| Boolean | `$and` `$or` `$not` |
| Dates | `$year` `$month` `$dayOfMonth` `$hour` `$minute` `$second` `$dateToString` |
| Arrays | `$size` `$arrayElemAt` `$first` `$last` `$slice` `$concatArrays` `$in` `$indexOfArray` `$isArray` `$reverseArray` `$range` `$sortArray` `$zip` |
| Sets | `$setUnion` `$setIntersection` `$setDifference` `$setEquals` `$setIsSubset` `$anyElementTrue` `$allElementsTrue` |
| Documents | `$objectToArray` `$arrayToObject` |
| Iteration | `$filter` `$map` `$reduce` — each binds a variable per element |
| Variables | `$$ROOT` `$$CURRENT` `$$NOW` `$$REMOVE` `$let` |
| Type conversion | `$convert` `$toString` `$toInt` `$toLong` `$toDouble` `$toBool` `$toDate` `$toObjectId` |
| Escape | `$literal` |

```json
[ { "$addFields": {
      "value": { "$multiply": ["$qty", "$price"] },
      "label": { "$toUpper": "$city" },
      "band":  { "$cond": [ { "$gte": ["$qty", 10] }, "bulk", "single" ] },
      "month": { "$dateToString": { "date": "$placed", "format": "%Y-%m" } } } },
  { "$group": { "_id": "$month", "revenue": { "$sum": "$value" } } } ]
```

**The named-argument operators are closed the same way stage operands are**
(see [Stages](#stages)): `$filter`, `$map`, `$reduce`, `$let`, `$convert`,
`$switch` (and each of its branches) and `$dateToString` refuse a key they do
not define — `{"input": "$items", "condition": …}` inside `$filter` is a
`400` naming `condition`, not a silently unfiltered array. **That holds for
all of them, on purpose** ([ADR-217](decisions.md#adr-217--the-refusals-and-differences-ruled-to-stay-are-recorded-as-a-set)): a pipeline carrying an extra key
is refused, and the message names the key to drop. A refused typo is better
than an answer that is wrong without a sign. (See [Deviations](deviations.md)
for a pipeline ported from elsewhere.)

### How a value is read

- `"$field"` is a **field path**; a bare string is a literal. A path that
  crosses an array is the array of what it found — see [Arrays](#arrays).
- `"$$name"` is a **variable** — see below — and `"$$name.path"` reads into its
  value.
- A document whose **first key starts with `$`** is an **operator**, and it may
  not carry any other key.
- **Any other document is computed** — its values are expressions. This is what
  makes a compound `$group` key work.
- **An array is computed** — its elements are expressions. See
  [Arrays and `$literal`](#arrays-and-literal).
- `{$literal: x}` yields `x` untouched, which is how you produce the *string*
  `"$total"`, a document with a `$`-prefixed key, or an array that holds either.

### Arrays and `$literal`

An array written inside an expression is an **array of expressions**: each
element is read as an expression, in the same scope, and the result is the
array of their values
([ADR-215](decisions.md#adr-215--an-array-written-inside-an-expression-is-an-array-of-expressions)).

```javascript
{ "$project": {
    "pair":  ["$a", "$b"],                              // [1, 2]
    "u":     { "$setUnion": ["$tags", ["$extra"]] },    // ["x", "y"]
    "dims":  [{ "w": "$w", "h": "$h" }, { "$multiply": ["$w", "$h"] }],
    "text":  { "$literal": ["$a", "$b"] } } }           // ["$a", "$b"]
```

over `{a: 1, b: 2, tags: ["x"], extra: "y", …}`. The rules:

- **An operator's argument list is not an array value.** `{"$size": ["$tags"]}`
  is `$size` of the field; `{"$size": [["$a", "$b"]]}` is `$size` of a
  two-element array, `2`. An array nested as one argument, or written where a
  single expression goes — a `$project` or `$addFields` value, a `$group` key or
  accumulator, `$map`'s `in` — is read as above.
- **Variables reach inside.** `{"$map": {"input": "$xs", "as": "x", "in":
  ["$$x", {"$multiply": ["$$x", 10]}]}}` pairs each element with ten times it,
  and a `$lookup` `let` name, `$$this` and `$$value` read the same way. Arrays
  nest, and a document inside one is computed too.
- **A missing field is `null` in its place.** `["$a", "$missing"]` is
  `[1, null]`, so positions never shift: `{"$concatArrays": [["$missing"],
  [1]]}` is `[null, 1]`, and `{"$size": [["$missing"]]}` is `1`. An array
  holding a null is an array, so an operator that answers null for a null
  argument does not here.
- **An element that cannot be evaluated fails the array**, with its own error,
  the first in written order. It is a value error like any other, so an
  `$and` or `$or` with a deciding argument still answers.
- **`{"$literal": [...]}` keeps an array as written** — the only way to get the
  string `"$a"` or an operator-shaped document inside an array. An array whose
  elements are all constants (`["new", "paid"]`, `[1, [2, 3]]`, `[{"a": 1}]`)
  needs no `$literal`: it reads as itself.
- **A document in an array is read like any other.** One whose first key starts
  with `$` is an operator, so `[{"$gt": 1}]` is refused (`$gt` takes two
  arguments) and `[{"a": 1, "$b": 2}]` is refused as mixing an operator with a
  field; both were kept as written before. Write them under `$literal`.

### Behaviours worth knowing

**What reads as false.** Wherever an expression asks whether a value is true —
`$cond`, `$switch`'s `case`, `$and`, `$or`, `$not`, `$filter`'s `cond`,
`$anyElementTrue`, `$allElementsTrue`, `$toBool` and a filter's `$expr` — the
false values are `false`, `null`, a missing field, and a zero of any numeric
type: `0`, `0.0` and `-0.0`, and a stored `Decimal128` zero whatever its sign
or exponent (`0`, `-0`, `0.000`, `0E-6176`). **Everything else is true**: `""`,
`[]`, `{}`, a `NaN` and an infinity, of a double or a `Decimal128`, included.

**`$cond` and `$ifNull` do not evaluate the branch they do not take.** A guard
like `{$cond: [{$gt: ["$n", 0]}, {$divide: [100, "$n"]}, null]}` works, rather
than failing on exactly the inputs it exists to protect.

**`$and` and `$or` fail only when the answer depends on the failure**
([ADR-211](decisions.md)). An `$and` is `false` if any argument is false, and
an `$or` `true` if any is true, beside an argument that cannot be evaluated and
in whatever order they are written. So the guard works with `$and` too:
`{$and: [{$isArray: "$tags"}, {$gt: [{$size: "$tags"}, 2]}]}` is `false` over
a document whose `tags` is a string. Only when no argument decides does the
failure stand, with the error of the earliest-written argument that failed.
The result is always a boolean. Evaluation stops at the argument that decides,
so writing the cheap guard first gives the same answer with less work. This
holds wherever an expression is evaluated: a `$match`'s `$expr`, `$project`,
`$addFields`, `$group`, `$filter`'s `cond`, a `$lookup`'s `let` and the rest. An error that is not
about a value, a bug in the server, is never held back this way. `$let` binds
by need ([ADR-219](decisions.md)): a variable that cannot be evaluated fails the
expression only if the body reads it.

**Null propagates; a type violation refuses.** `{$add: ["$typo", 1]}` is null
because a missing field is null. `{$add: ["text", 1]}` is a 400. Returning null
for both would make a typo and a type error indistinguishable in the output.
The same holds for a `$match` stage's `$expr`: a document it cannot be
evaluated against fails the pipeline, it is not dropped
([ADR-206](decisions.md)).

**Integer arithmetic is exact.** `$add`, `$subtract` and `$multiply` compute in
64-bit integers whenever every operand is integral, promoting to a double only
on overflow or when a double is involved. `$divide` is always a double — an
integer result would make `{$divide: [1, 2]}` zero.

**Date arithmetic works.** `$add` shifts a date by milliseconds, `$subtract`
between two dates gives the interval in milliseconds, and `$subtract` of a
number from a date shifts it back.

**Strings are counted in code points, not bytes.** `$substr` is `$substrCP`;
there is no byte-oriented variant, because it can split a character and produce
invalid UTF-8.

**`$dateToString` supports `%Y %m %d %H %M %S %L %%`, all UTC.** An unknown
specifier is an error rather than being copied through — a literal `%q` in every
row of a report is the kind of wrong output nobody notices. BSON dates carry no
zone, so there is nothing for `%z` to convert to.

### Math

`$abs`, `$ceil`, `$floor`, `$round`, `$trunc`, `$pow`, `$sqrt`, `$exp`, `$ln` and
`$log10` take an `Int32`, `Int64`, double or `Decimal128` ([ADR-253](decisions.md)).
A `null` or missing argument gives `null`, as in the rest of arithmetic; a value
that is not a number is `400`.

| Operator | Result |
|---|---|
| `{$abs: x}` | The absolute value; an integer gives an `Int64`, as every integer result of an expression does (an `Int32` becomes an `Int64`). `$abs` of the smallest `Int64` is a double, as an overflowing `$add` is. A decimal keeps its exponent: `$abs` of `-1.50` is `1.50` |
| `{$ceil: x}`, `{$floor: x}` | The integer nearest toward `+∞` or `-∞`. An integer is itself, as an `Int64`; a double stays a double; a decimal has exponent 0 (`$ceil` of `1.20` is `2`) |
| `{$round: [x, place]}`, `{$trunc: [x, place]}` | `place` digits after the point, default 0. **`$round` rounds half to even** (`$round: [2.5]` is `2`, `[3.5]` is `4`, `[-2.5]` is `-2`); `$trunc` cuts toward zero. A negative `place` rounds to tens, hundreds, and so on (`$round: [1250, -2]` is `1200`, an integer). `place` is a whole number from `-20` to `100`, a `null` place gives `null`, and any other is `400`. An integer is exact, and a decimal result has exponent `-place` (`$round: [2.5, 2]` over `Decimal128("2.5")` is `2.50`), unless that needs more than 34 digits, when it is returned as it was. A double is rounded on its **exact binary value**: `$round: [2.675, 2]` is `2.67`, because the double nearest 2.675 is below it, and `$round: [0.125, 2]` is `0.12`, an exact tie |
| `{$pow: [base, exponent]}` | An integer base and a non-negative integer exponent give an integer while it fits `Int64`, a double after; otherwise a double, or a decimal with a decimal operand. `0` to a negative power, and a negative base to a power that is not a whole number, are `400` (not an infinity or a `NaN`) |
| `{$sqrt: x}` | The square root. A negative argument is `400` |
| `{$exp: x}`, `{$ln: x}`, `{$log10: x}` | `e^x`, the natural logarithm and the base-10 logarithm. `$ln` and `$log10` of zero or a negative are `400`; `$log10` of an exact power of ten is exact |

An integer or a double argument to `$sqrt`, `$exp`, `$ln` and `$log10` gives a
double; a `Decimal128` gives a `Decimal128`. A `NaN` is a `NaN` out, and `$exp`,
`$sqrt` and `$ln` of an infinity are the infinity or zero the limit gives.
**A refused argument is never turned into a `NaN` or an infinity.**

**A `Decimal128` is exact to one rule**: the result is the true value rounded
once to 34 digits, half to even, in ADR-252's context, computed in integers, so
it is the same bytes on every member and architecture. Overflow is `Infinity` and
underflow is gradual, as in the rest of the decimal arithmetic. The result of
`$pow`, `$sqrt`, `$exp`, `$ln` and `$log10` has its trailing zeros stripped
(`$sqrt` of `4.00` is `2`, `$pow: [1.1, 2]` is `1.21`), because a quantum means
nothing for them; `$abs`, `$ceil`, `$floor`, `$round` and `$trunc` keep the
scale rules in the table. Where the true value is within 10^-75 of a rounding
midpoint (a case that needs a coincidence of 40 digits) the last digit can differ
from the correctly rounded one.

**A double uses the platform's math library** for `$sqrt`, `$exp`, `$ln`,
`$log10` and a non-integer `$pow`, so the last digit of a double result can
differ between architectures ([Deviations](deviations.md)). Use a `Decimal128`
where the digits must agree.

```json
{ "$project": {
    "cents":  { "$round": [ { "$multiply": ["$price", 100] }, 0 ] },
    "fee":    { "$trunc": [ { "$multiply": ["$amount", 0.029] }, 2 ] },
    "growth": { "$pow": [ 1.07, "$years" ] },
    "decibel": { "$multiply": [ 10, { "$log10": "$ratio" } ] } } }
```

### Variables

An expression evaluates in a **scope**: the document, plus whatever the
constructs around it have bound. A variable is written `$$name`, and
`$$name.path` reads a field out of its value.

| Variable | Bound by | Value |
|---|---|---|
| `$$ROOT` | always | the document the expression is evaluated against |
| `$$CURRENT` | always | the same document — nothing here rebinds it |
| `$$NOW` | always | the date of the request: one value, read from the clock of the member running it |
| `$$REMOVE` | always | no value: the field it would be written to is left out |
| `$$this` | `$filter`, `$map` (or the name given as `as`), `$reduce` | the current element |
| `$$value` | `$reduce` | the accumulator so far |
| any lowercase name | `$let`, a `$lookup` `let` | what its expression evaluated to |

```json
{ "$addFields": {
    "bulk":  { "$filter": { "input": "$items", "as": "it",
                            "cond": { "$gte": ["$$it.qty", "$min"] } } },
    "skus":  { "$map":    { "input": "$items", "in": "$$this.sku" } },
    "total": { "$reduce": { "input": "$items", "initialValue": 0,
                            "in": { "$add": ["$$value", "$$this.qty"] } } },
    "net":   { "$let":    { "vars": { "gross": { "$multiply": ["$qty", "$price"] } },
                            "in": { "$subtract": ["$$gross", "$discount"] } } } } }
```

**Scopes nest lexically.** A `$map` inside a `$map` sees its own element as
`$$this`; the outer element is shadowed, and the way to reach both is to name
them — `as: "row"` outside and `as: "cell"` inside. `$let` inside `$let` shadows
the same way, and the outer binding is back once the inner construct closes.

**An unknown variable is a parse error, not null.** `$$this` outside anything
that binds it, `$$order` where the `let` said `oid`, or a typo in either, is
refused before a single document is read. Reading it as a field called `$this`
would silently yield null in every row. MongoDB's other system variables —
`$$DESCEND`, `$$PRUNE` and `$$KEEP` — are refused with a message that says they
are unsupported rather than misspelled.

**`$$NOW` is one date for the whole request.** The pipeline is parsed once, on
the member that runs it, and every `$$NOW` in it is the wall-clock time of that
moment: the same date in every document, every stage and every `$lookup`
sub-pipeline, and a date a long pipeline cannot move past
([ADR-253](decisions.md)). It is a `Date` with millisecond precision, from the
executing member's clock, so two members answering the same request a second
apart disagree, as any two clocks do. `$$NOW.path` is refused: a date has no
fields. `$$NOW` is a value in an expression; a `$match` takes it through
`$expr`, `{$expr: {$lt: ["$expires", "$$NOW"]}}`, because the filter language
has no variables.

**`$$REMOVE` leaves a field out.** As the value of a field in `$project` or
`$addFields`, or in a document an expression builds, it drops that field, and in
`$addFields` it removes a field the document already had:
`{$addFields: {secret: "$$REMOVE"}}`. A path that names a field *through an array by a name* (`a.b` over `a: [{b: 1}]`) is refused as a value written there would be, with the same error; a numeric position (`a.0`) leaves a `null` in the array, as `$unset` does, so other positions keep their index. It works through the result position of
`$cond`, `$switch` and `$let`, so `{$cond: [{$gt: ["$qty", 0]}, "$qty", "$$REMOVE"]}`
keeps the field only when it is positive. Anywhere else it reads as `null`, the
same as a path to a missing field does in this engine (an expression always
produces a value), so `{$ifNull: ["$$REMOVE", 1]}` is `1`, an element of an array
expression is `null`, and `$$REMOVE.path` is refused ([Deviations](deviations.md)).

**`$let` values see the enclosing scope, not each other.** `{vars: {a: 1, b:
"$$a"}}` is refused; nest a second `$let` to build on the first. **A `$let`
variable is evaluated when the body first reads it** ([ADR-219](decisions.md)),
once, and not at all if nothing reads it: `{$let: {vars: {r: {$divide: [1,
"$n"]}}, in: {$cond: [{$eq: ["$n", 0]}, 0, "$$r"]}}}` answers `0` over `n: 0`.
When two variables would fail, the error is the one the body read first, not the
one written first. A `$lookup` `let` is not lazy: it evaluates every variable it
declares, for each input document. **A user
variable name** starts with a lowercase letter and continues with letters,
digits and underscores — MongoDB's rule, which also keeps user names from ever
colliding with the uppercase system ones.

### Arrays

**Null in, null out; a non-array refuses.** Every array operator returns null
when its array is null or the field is missing, so a sparse collection does not
fail the pipeline, and errors when it is any other type, so `{$size: "$name"}`
on a string is a 400 rather than a silent null. `$isArray` is the exception and
answers `false` for anything that is not an array.

**That is the rule on purpose** ([ADR-217](decisions.md#adr-217--the-refusals-and-differences-ruled-to-stay-are-recorded-as-a-set)). A null or missing argument to
`$size`, `$in`, `$range`, `$slice`, `$arrayElemAt`, `$indexOfArray`, the set
operators and `$anyElementTrue` / `$allElementsTrue` answers null, because a
sparse collection is the norm here. `{$addFields: {n: {$size: "$tags"}}}` over a
document with no `tags` gives `n: null`, not a failed request. A pipeline that needs to catch bad data tests for
the bad value, for example `{$match: {$expr: {$eq:
[{$isArray: "$tags"}, false]}}}`, which finds the documents whose `tags` is
not an array, a missing one included. (See [Deviations](deviations.md).)

**`$arrayElemAt` counts from the end when negative** (`-1` is the last element)
and is null when the index is out of range on either side. **`$first`** and
**`$last`** are `$arrayElemAt` at `0` and `-1`; on an empty array they are null.
**Null here is the rule, on purpose**
([ADR-217](decisions.md#adr-217--the-refusals-and-differences-ruled-to-stay-are-recorded-as-a-set)): an expression always yields a value, so `{$addFields: {head:
{$first: "$lines"}}}` over `lines: []` writes `head: null` and the field is
*present*. A later `$match` on `{head: {$exists:
true}}` keeps the document here, and the same holds for an index out of range
and for a field path that resolves to nothing.
**A fractional index is refused**, not truncated.

**A whole-number argument may be any number of whole value**: an integer, a
double such as `2.0`, or a decimal such as `{"$numberDecimal": "2"}` or
`"2.00"`. That holds for `$arrayElemAt`'s index, `$slice`'s position and count,
`$range`, `$filter`'s `limit`, `$indexOfArray`'s bounds, `$limit`, `$skip` and
`$vectorSearch`'s `k`; a decimal with a fraction is refused as a double with
one is. `$substr` truncates a fractional start or length, the double's and the
decimal's alike. A projection's flag and a sort direction read a decimal as
the number it is too. Until 0.51.0 each refused a decimal, `Decimal128("1")`
included. A `$vectorSearch` query vector still takes only doubles and
integers: a decimal component would have to be rounded to a 32-bit float
([Deviations](deviations.md)).

**`$slice` has two shapes.** `[array, n]` takes the first `n`, or the last
`|n|` when `n` is negative. `[array, position, n]` takes `n` from `position`,
counted from the end when negative, and `n` must be positive there. A window
past either end is empty rather than an error.

**`$in`** is `[value, array]` — the value first, the opposite order from the
filter's `$in` — and tests membership in the canonical order, so `5` is in
`[5.0]`. **`$indexOfArray`** is `[array, value, start?, end?]` and answers `-1`
when nothing matches. **`$concatArrays`** is null if any input is, as `$concat`
is for strings.

**`$filter` takes an optional `limit`**, stops once it has that many matches,
and treats a null limit as no limit. Zero or a negative limit is refused.

**A field path through an array is the array of what it found.** `$items.sku`
over `items: [{sku: "a"}, {sku: "b"}]` is `["a", "b"]`, so `{$size:
"$items.sku"}` is 2 and `{$in: ["a", "$items.sku"]}` is true — MongoDB's rule,
applied in every expression context: `$project`, `$addFields`, a `$group` key
(which then buckets by the whole array), an accumulator argument, `$expr`, and
a path into `$$ROOT` or a variable. An element that is not a document, or that
lacks the field, is skipped rather than filled with null, and an array none of
whose elements had it is `[]`. **Each array crossed adds one level, and nothing
is flattened further:** `$a.b` over `a: [{b: [1, 2]}, {b: 3}]` is `[[1, 2], 3]`
— the last segment returns each `b` as it is — and `$a.b.c` over `a: [{b: [{c:
1}, {c: 2}]}]` is `[[1, 2]]`. `$reduce` with `$concatArrays` flattens a level
when that is what you want. **A numeric segment is a field name here, never an
index:** `$items.0.sku` reads the field called `0` of each element and finds
nothing; `{$arrayElemAt: ["$items", 0]}` is the element. A
[projection](query-language.md#sort-and-projection) path — `$project`'s flags,
and `find`'s — reads it the same way, as a field name in each element. The
filter language and a sort key read `items.0` both ways, and an update path
writes by position; that is where an expression path and a filter path
disagree — and `$unwind`, `$sort` and `$lookup`'s `localField` and
`foreignField` name a field rather than compute one, so they do not fan out.
**`$unwind` also has to write each expanded element back to its path**, not
only read it, and a path crossing an array by a named segment has no single
place to write to — `$unwind: "$a.b"` over `a: [{b: [1, 2]}, {b: 3}]` is
refused, `400`, on every such document, regardless of what `b` holds at any
element (see [`$unwind`](#unwind) and [ADR-130](decisions.md)).

**`$range`** produces at most 100,000 integers — the same ceiling as the
pipeline, for the same reason: `{$range: [0, 1000000000]}` is a memory
exhaustion written as an expression, and it is refused before anything is
allocated. Its elements are 64-bit integers, as every integer result here is
, so `$type` of an element is `"long"`
([ADR-217](decisions.md#adr-217--the-refusals-and-differences-ruled-to-stay-are-recorded-as-a-set)).

### Sorting and zipping arrays

```json
{ "$project": {
    "ranked": { "$sortArray": { "input": "$players", "sortBy": { "points": -1, "name": 1 } } },
    "pairs":  { "$zip": { "inputs": [ "$keys", "$values" ] } }
} }
```

Over `{keys: ["a", "b", "c"], values: [1, 2]}` the `pairs` are `[["a", 1], ["b", 2]]`.

| Operator | Specification | Result |
|---|---|---|
| `$sortArray` | `{input: <array>, sortBy: 1 \| -1 \| {path: 1 \| -1, ...}}` | the array in order |
| `$zip` | `{inputs: [<array>, ...], useLongestLength: <bool>, defaults: [<expr>, ...]}` | an array of arrays, element `i` holding each input's element `i` |

**`$sortArray` orders by the `$sort` stage's order** ([ADR-238](decisions.md)), so a
sorted array and a sorted collection agree: null, then numbers (a `Decimal128`
by its exact value), strings, documents, arrays, booleans. `sortBy: 1` or `-1`
compares the elements whole, a document element included. A sort document
orders document elements by those keys, dotted paths allowed, the first key
deciding and the next breaking its ties; an element that is not a document, or
lacks a key, reads as null for it. The sort is **stable**: elements that compare
equal keep their input order, in both directions. `input` is an expression; null
or missing gives null and any other non-array is a `400`. `sortBy` is written
out, not computed, and a missing `input` or `sortBy`, another key, a number
other than `1` or `-1`, an empty sort document and a direction other than `1`
or `-1` are each a `400` naming `$sortArray`.

**`$zip` stops at the shortest input** by default. With
`useLongestLength: true` it runs to the longest, and an input that has run out
contributes `null`, or its entry of `defaults` (one expression per input, in
order). `defaults` without `useLongestLength: true`, or of another length than
`inputs`, is a `400`, as is a `useLongestLength` that is not a boolean. `inputs`
is an array written out, not computed, and must not be empty. If any input is
null or missing the result is null; a non-array input is a `400` even beside a
null. A result is at most 1,000,000 values (rows times inputs), counted before
anything is built, so a larger one is a `400` naming the limit.

Both read variables as their siblings do (`{$map: {input: "$pairs", in:
{$zip: {inputs: ["$$this.k", "$$this.v"]}}}}`) and work wherever an expression
does. Where they differ from the reference system is in
[Deviations](deviations.md). To order by something an element does not hold,
or to pair by a computed rule, `$map` over a `$range` of indexes with
`$arrayElemAt`, or `$unwind` with `$sort` and `$group`, still works.

### Sets

```json
{ "$project": { "all": { "$setUnion": [ "$tags", "$wants" ] } } }
```

Over `{tags: ["vip", "eu"], wants: ["eu", "newsletter"]}` that is
`["vip", "eu", "newsletter"]`.

| Operator | Arguments | Result |
|---|---|---|
| `$setUnion` | one or more arrays | every member of any of them |
| `$setIntersection` | one or more arrays | the members of the first that every other holds |
| `$setDifference` | two arrays | the members of the first the second does not hold |
| `$setEquals` | two or more arrays | `true` when all have the same members |
| `$setIsSubset` | two arrays | `true` when every member of the first is in the second |
| `$anyElementTrue` | one array | `true` when any element is true |
| `$allElementsTrue` | one array | `true` when no element is false |

**A member is what `$eq` calls equal.** Each array is read as the set of its
distinct members, and two values are one member exactly when `$eq` and `$in`
would call them equal — the canonical order's equality. So `1`, `1.0` and
`{"$numberLong": "1"}` are one member, every `NaN` is one, `0.0` and `-0.0` are
one, and `null` and `undefined` are one. An array or a document element is a
member as a whole and is compared whole, never flattened: `[1, 2]` and
`[1.0, 2]` are one member, and two documents are one only with their keys in
the same order, as `find` compares them. These are the members `$addToSet`
keeps too ([ADR-186](decisions.md#adr-186--a-sets-members-are-identified-the-way-group-identifies-a-bucket)).

**Results come back first-seen, without duplicates.** A set result lists the
members in the order they first appear, reading the arguments left to right
and each array from its start, and the element kept is that first appearance,
untouched: `{$setUnion: [[3, 1, 3], [1.0, 2]]}` is `[3, 1, 2]`, its `1` still
the integer. `$setIntersection` and `$setDifference` keep the first array's
order. The order is fixed so the same input always gives the same output; a
pipeline that needs another order sorts after.

**Null in, null out; a non-array refuses wherever it stands.** A null or
missing argument makes the result null, the boolean operators included. A
non-array is a `400` even beside a null — a set has no first argument that
could hide the others, so the refusal does not depend on where the bad value
is written.

**A `Decimal128` in a set input is a member by its value**, as `$eq` compares
it: `[Decimal128("5")]` is not a subset of `[1]` and is a subset of `[5.0]`, and
`Decimal128("1.0")` and `Decimal128("1.00")` are one member. Until 0.51.0 a
decimal anywhere in a set input was refused, because the canonical order ranked
one equal to every other number ([ADR-207](decisions.md#adr-207--a-set-operators-member-is-what-eq-calls-equal-its-result-is-first-seen-and-what-it-cannot-compare-it-refuses)).

**`$anyElementTrue` and `$allElementsTrue` read truthiness** as `$and` and
`$or` do: `false`, `null`, `0` and missing are false, and everything else is
true, the empty array included, and an array element is not looked inside.
An empty array is `false` for `$anyElementTrue` and `true` for
`$allElementsTrue`. A stored `Decimal128` element is read like any other
number, false when it is a zero. The argument is one
array, so a literal is written inside the argument list:
`{"$anyElementTrue": [[true, false]]}`, not `{"$anyElementTrue": [true,
false]}`, which is two arguments and refused.

**No cap of their own.** None of these builds more than its inputs already
hold, so the only expression-level size limit, `$range`'s, does not apply;
the pipeline's document ceiling still does.

### Documents as pairs

```json
{ "$addFields": {
    "pairs":   { "$objectToArray": "$attrs" },
    "renamed": { "$arrayToObject": { "$map": { "input": { "$objectToArray": "$attrs" },
                   "in": { "k": { "$toUpper": "$$this.k" }, "v": "$$this.v" } } } } } }
```

**`$objectToArray`** turns a document into `[{"k": name, "v": value}, ...]`,
in the document's field order. Only the top level is turned over; a value that
is a document stays one. An empty document is `[]`; null or missing is null;
anything else is a `400`.

**`$arrayToObject`** is the reverse, and reads two forms: `[["name", value],
...]` and `[{"k": "name", "v": value}, ...]` (the `k` and `v` in either
order). The first element decides the form and every element must use it.
Each of these is a `400` naming the element by its index:

- a form mixed with the other (`element 0 is a [key, value] array, element 1
  is a {k, v} document`);
- an array pair with other than two elements, or a document pair with a field
  besides `k` and `v`, or without both;
- an element that is neither, `null` included;
- a key that is not a string;
- a key holding a NUL, which a BSON field name cannot.

**Any other string is a key, taken as written**: an empty key, a dotted one
(`"a.b"`) and one starting with `$` are accepted, as an object expression,
`$literal` and a stored document accept them, so `{"$arrayToObject":
{"$objectToArray": "$o"}}` gives back `o` whatever its keys. A field path
cannot read such a field (`"$a.b"` is a path, not a name), but
`$objectToArray` can.

**A repeated key keeps its first place and takes its last value**:
`[["a", 1], ["b", 2], ["a", 3]]` is `{"a": 3, "b": 2}`. An empty array is
`{}`; null or missing is null.

### Type conversion

```json
{ "$convert": { "input": "$qty", "to": "int", "onError": 0, "onNull": 0 } }
{ "$toInt": "$qty" }
```

`to` is a type name or its numeric BSON code, as `$type` spells them:
`"double"` (1), `"string"` (2), `"objectId"` (7), `"bool"` (8), `"date"` (9),
`"int"` (16), `"long"` (18). The `$toX` shorthands are the same operator with
the target fixed and no fallbacks. Both exist for two situations that come up
as soon as data arrives from more than one source:

- **A `$lookup` whose keys differ in type across collections** — an order's
  `customerId` stored as text, the customer's `_id` as a number. Convert in an
  `$addFields` before the join: `{ "$addFields": { "cid": { "$toLong":
  "$customerId" } } }`, then `localField: "cid"`.
- **A `$group` by a date that was stored as a string.** `{ "$group": { "_id":
  { "$month": { "$toDate": "$placed" } } } }` — the date operators need a date,
  and `$toDate` makes one out of ISO 8601 text.

**Null in, null out, unless `onNull` says otherwise.** A missing field is null.
**A value with no conversion is a 400, unless `onError` gives a value** — the
error names both types, so `$toInt` of a document says so rather than yielding
null. Neither fallback is evaluated unless it is needed. `onError` covers the
*conversion*, not the `input` expression: `{ "$convert": { "input": { "$divide":
[1, 0] }, ... } }` still fails, because hiding a broken expression behind a
fallback would make a typo look like data.

What converts to what:

| To | From |
|---|---|
| `double` | any number; bool (`0`/`1`); date (epoch milliseconds); string, parsed strictly — `"1.5"`, `"-1e3"`; not `"1.5kg"` |
| `int`, `long` | any number, **truncated toward zero** and refused when out of range — `$toInt` of 2^40 is an error, not a wrap; bool; string as a base-10 integer — `"42"`, not `"1.5"`; date to `long` only (epoch milliseconds never fit an int) |
| `string` | number (a whole double prints as `"2"`, not `"2.0"`, and a double never prints in exponent form); bool; date as ISO 8601 with milliseconds, `"2026-08-12T13:45:07.250Z"`; ObjectId as 24 hex characters |
| `bool` | a number is `false` when zero, a stored `Decimal128` zero of any sign or exponent included; **everything else present is `true`** — including `""` and `"false"`, which is MongoDB's rule and a trap worth knowing |
| `date` | a number as epoch milliseconds (a double is truncated); a string in RFC 3339 / ISO 8601 with an offset or `Z`, or the looser forms `"2026-08-12"`, `"2026-08-12 13:45:07"` and a missing zone, all read as UTC; an ObjectId's creation time |
| `objectId` | a string of 24 hex characters |

Everything not in the table — an array to a number, a document to a date — is
an error.

**Four rules of the table, on purpose**
([ADR-217](decisions.md#adr-217--the-refusals-and-differences-ruled-to-stay-are-recorded-as-a-set), [Deviations](deviations.md)):

- **An `int` converts to a `date`** (as epoch milliseconds, like a `long` or a
  double): `{$toDate: 1500}` is `1970-01-01T00:00:01.500Z`.
- **`to` is a constant**: a type name or a numeric code. A field path or
  other expression there is a `400` at parse, before any document is read.
- **A string converts to a date in these spellings and no others**: RFC 3339
  with `Z` or a `±hh:mm` offset; a date alone (midnight UTC); a space for the
  `T`; and a time with no zone, read as UTC. `+0500`, `13:45` with no seconds,
  month names and `20260812` are errors.
- **A double converts to a string in plain decimal digits, never exponent
  form**: `1e21` is `"1000000000000000000000"` and `1e-7` is `"0.0000001"`.
  The engine's JSON output prints doubles the same way.

**`decimal` (`Decimal128`)** is a target ([Decimal128 arithmetic](#decimal128-arithmetic)):

- An integer, a boolean and a date (its epoch milliseconds) convert exactly.
- A **double** converts to its exact binary value rounded once to 34 digits,
  half to even (IEEE 754 `convertFormat`): `0.1` is
  `0.1000000000000000055511151231257827`, because that is the double 0.1, not
  the decimal one tenth. To get one tenth, send `{"$numberDecimal": "0.1"}` or
  convert the string `"0.1"`. `2.5` is `2.5`, `100.0` is `100`, `-0.0` is `-0`.
- A **string** is read by the strict parser and is exact or an error: `"1.50"`
  is `1.50` with its two places, and `""`, `"."`, `" 1"`, `"1e"` and a value that
  would need rounding (a nonzero 35th digit, or below `1E-6176`) are conversion
  errors, which `onError` answers.
- A `Decimal128` is itself, the same bytes. A `null` or missing input answers
  `onNull`. Anything else is a conversion error.

A `Decimal128` converts to `double` (the nearest, half to even; `1E+400` is
`Infinity`, `1E-400` is `0.0`), to `long` and `int` (truncated toward zero from
the exact value; `NaN`, an infinity and a value outside the range are
conversion errors), to `string` (BSON's text form, which keeps the cohort:
`2.30`, `1E+3`, `-0`), to `bool` (a zero of any sign or exponent is `false`)
and to `date` (the exact value truncated toward zero to whole milliseconds;
`NaN`, an infinity and a value outside `i64` are conversion errors). To an
`ObjectId` it is a conversion error.

---

## Four behaviours worth knowing

**Field references are `"$field"`; a bare string is a literal.** `{$sum: "$qty"}`
sums the field, `{$sum: "qty"}` sums the constant string. This matches MongoDB,
and the alternative — guessing which was meant — is worse.

**An integer sum stays an integer.** Widening every total to a double would lose
precision above 2^53 and change what `$type` reports, the same reasoning that
keeps documents' integer types intact ([ADR-002](decisions.md)).

**`$avg` skips non-numeric and missing values rather than counting them as
zero.** A field present on half the documents would otherwise halve the mean,
silently. A `Decimal128` is a number and is counted and summed, exactly
([Decimal128 arithmetic](#decimal128-arithmetic)).

**`$group` buckets by value, not by type.** `5`, `5.0` and `5i64` share a
bucket, because they share an index entry everywhere else in this database. The
grouping key goes through the same encoder the indexes use.

---

## Decimal128 arithmetic

`$add`, `$subtract`, `$multiply` and `$divide`, the `$sum` and `$avg`
accumulators, and `$inc` and `$mul` in an update ([Query language](query-language.md#decimal128-arithmetic-in-updates))
do arithmetic on a `Decimal128` ([ADR-252](decisions.md)). It is IEEE 754
decimal128 arithmetic under one fixed context: 34 digits, the format's exponent
range, round half to even, and IEEE's preferred exponents. There is no setting
to change it. It is not a full implementation of the standard, which also asks
for a selectable rounding direction and exposed flags; kimmydb has neither.

**If a decimal is an operand, the result is a decimal.** With no decimal
operand, every result is what it always was.

- **An integer enters exactly.** `Int32` and `Int64` count as themselves, at
  exponent 0.
- **A double enters as its exact binary value, rounded once to 34 digits, half to
  even** (IEEE 754 `convertFormat`), which is also what `$toDecimal` gives. The
  double `0.1` is `0.1000000000000000055511151231257827`; the double's binary
  error is in the digits, where it can be seen. To compute with one tenth, send
  `{"$numberDecimal": "0.1"}`. This rule never reverses an order the comparator
  gives, which the shortest round-trip text of a double (`0.3`) and MongoDB's 15
  digits both can. `{$add: ["$bal", 0.01]}` over `Decimal128("10.00")` shows
  `10.01000000000000000020816681711722`.
- A `null`, `undefined` or missing operand makes the result `null`, as before. A
  string is `400`, `$add needs numbers, found a string`.

**A sum, a quotient and a mean are exact over their operands as converted, and rounded once. A product is a chain of rounded steps.**

- `$add` of any number of operands is one exact sum, and `$subtract` the exact
  sum of `a` and `-b`: `{$add: [1E+34, 0.5, 0.5, -1E+34]}` is `1.0` in every
  order, where adding left to right gives `0E+1`.
- `$multiply` of any number of operands **multiplies left to right and rounds
  each step**, to 34 digits half to even, as MongoDB does and as IEEE 754 defines a
  sequence of operations. It is linear in the number of operands, with no limit
  beyond the request size, and **it depends on the order** where an intermediate
  overflows or rounds: `[1E+5000, 1E+5000, 1E-5000]` is `Infinity` (the first
  step overflows), `[1E+5000, 1E-5000, 1E+5000]` is `1E+5000`, and three factors
  of 34 digits can differ in the last digit from the exact product rounded once.
  An integer is exact and a double enters by the rule above at its position.
- `$divide` rounds the exact quotient once. `$avg` divides the **exact** sum by
  the count and rounds once; it is not `$sum` followed by `$divide`, which
  rounds twice.
- **`$sum` and `$avg` over a group with a decimal give the same bytes under any
  document order, plan and member.** Integers and doubles in such a group are
  converted as above and joined to the exact sum. A group with no decimal sums as
  it always did: `Int64` with a widening to double on overflow, or a double.
- Values that are not numbers are skipped, as ever, and an array or a document is
  not read inside.

**The result keeps IEEE's preferred quantum**, so a money field keeps its scale:

| | |
|---|---|
| `1.10 + 1.20` | `2.30` |
| `1.5 + 0.00` | `1.50` |
| `10.00 - 0.50` | `9.50` |
| `1.5 × 1.0` | `1.50` |
| `2.30 / 2` | `1.15` |
| `$sum` of `1.10`, `1.20`, `3` | `5.30` (the least exponent of all the terms; an integer's is 0) |
| `$avg` of `1.10`, `1.20` | `1.15` |

Equal values still compare, group and key as one, so `2.30` and `2.3` are one
`$group` bucket, one `$addToSet` member and one unique-index entry. The bytes
differ, the JSON text differs (`"2.30"` against `"2.3"`), and an update that
changes only which member is stored counts in `modified`
([Query language](query-language.md#decimal128-arithmetic-in-updates)). `$group`'s `_id`, `$min`,
`$max` and `$first` report the first member seen, as they always did, so which
of `2.3` and `2.30` they show depends on document order; `$sum` and `$avg` do not.

**`$round` and `$trunc` quantize** ([Math](#math)): an inexact result shows all 34
digits unless the pipeline cuts it, so `$avg` of `1.00`, `2.00`, `2.00` is
`1.666666666666666666666666666666667`, `{$round: [<that>, 2]}` is `1.67`, and
`1 / 3` is `0.3333333333333333333333333333333333`. `$abs`, `$ceil`, `$floor`,
`$pow`, `$sqrt`, `$exp`, `$ln` and `$log10` take a decimal too and give a decimal,
rounded once.

**Special values follow IEEE 754.** Every NaN result is the one quiet NaN, with
no payload (a signalling or payload NaN in gives the plain NaN out, so a `$sum`
cannot depend on which NaN came first). `Infinity - Infinity`, `Infinity × 0` and
`Infinity / Infinity` are `NaN`; `Infinity + 5` is `Infinity`; `5 / Infinity` is
`-0E-6176` or `0E-6176`. The sum of two zeros is `-0` only if both are. **Any
zero divisor is `400`, `$divide by zero`**, whatever its spelling (`0`, `-0`,
`0E-100`, `0.0`) and whatever the dividend: IEEE would give an infinity or a
NaN, and kimmydb refuses every zero divisor for every number.

**Overflow is `Infinity` in an expression or an accumulator**
(`9.999…E+6144 × 10`), as double arithmetic is, and **refused in `$inc` and
`$mul`**, as an `Int64` overflow is. **Underflow is gradual** and not refused: a
result below `1E-6143` becomes a subnormal, or a zero with exponent `-6176`.

Two things that look like bugs and are not:

- `{$eq: [{$add: ["$bal", {"$numberDecimal": "0.1"}]}, 10.1]}` is `false` when
  `bal` is `10.00`: the sum `10.10` is a decimal, and the double `10.1` is not
  10.1 but `10.0999999999999996447286321199499070644378662109375`.
- `$toDecimal(d)` equals the double `d` only when the double's exact binary
  expansion has at most 34 significant digits (`2.5` does, `0.1` does not).

**Cost.** In a release build a two-operand operation takes about a quarter of a
microsecond for money-sized values (under 20 µs at the extremes of the exponent
range), a `$sum` over 100,000 decimal prices costs about 1.3 times the same over
`Int64`, and a `$multiply` of 100,000 operands takes about 15 ms (a chain of
rounded steps is linear).

---

## `$lookup`

Two forms. The first joins on one key and is a single pass; the second runs a
sub-pipeline per input document and is a nested loop. Reach for the first
whenever the join *is* an equality.

```json
[ { "$lookup": { "from": "customers", "localField": "customerId",
                 "foreignField": "_id", "as": "customer" } } ]
```

`as` is **always an array**, empty when nothing matched. A field whose type
depends on whether anything matched would force every caller to handle two
shapes.

**It is authorized against the collection it names.** A caller with `read` on
`orders` and nothing else is refused — with the same uniform 403 as any other
denial, so a pipeline cannot be used to probe which collections exist. Without
that check a join would be a privilege escalation shaped like a query, routing
around the single authorization point ([ADR-024](decisions.md)).

**The foreign collection is scanned once**, not once per input document. A
per-document join is O(n·m), which on any real pair of collections is the
difference between a query and an outage.

**A document joins under every value its path yields, and under every element
of an array it holds** ([ADR-210](decisions.md#adr-210--a-lookup-key-joins-on-every-value-its-path-yields-and-on-every-element-of-an-array)).
`localField: "items.sku"` over `items: [{sku: "a"}, {sku: "b"}]` joins on `"a"`
and on `"b"`, and `as` holds the union of what each matched; `localField:
"productIds"` over `productIds: [10, 20]` joins on `10` and on `20` (and on the
list itself, for a foreign key that is that list). The foreign side is read the
same way, so a foreign document whose `foreignField` crosses an array, or holds
one, is a candidate under each element, and the two always agree about what a
key is. A field is read as a filter reads it: `{tags: "a"}` matches a document
whose `tags` is `"a"`, is `["a", "b"]`, or is any array holding `"a"`, and a join
on `tags` matches under the same three. Two documents join when a value of one
equals a value of the other, where a value is what the path resolves to and,
if that is an array, each of its elements, **one level down**: an array inside an
array is an element and is not opened. The fields are field paths, not
expressions (`items.sku` is every element's `sku`, and a numeric segment is both
an index and a field name). Four rules decide the rest:

- **A foreign document is attached once.** Met through two elements of one
  input document, or through two of its own, it appears once in `as`, in the
  order the foreign collection is scanned, not the order the elements name
  them in. The reference behaviour does not promise an order here.
- **Nothing at the path joins nothing.** An input document without the field
  attaches nothing, and a foreign document without it is never a candidate; an
  explicit `null` joins an explicit `null`. A stored `Decimal128` joins by its
  value, like any number: `Decimal128("5.00")` joins `5`, and `[Decimal128("5.0"),
  5]` joins under the one key `5`. (Until 0.51.0 a decimal could not be a key
  and joined nothing.)
- **The ceiling counts what is held and what is attached.** The foreign
  collection is still scanned once, and the documents any key of the input needs
  are held in memory, each once, up to the pipeline's ceiling of 100,000. The
  documents a stage attaches in all, summed over the input, are counted against
  the same ceiling before any is copied: a join that fans out can attach one
  foreign document to every element of every input document. A pipeline that fit
  before can be refused now, naming `$lookup` and the count, where it used to
  answer with fewer documents attached than it meant.
- **A join on one key per document is held to the attached ceiling too.** Input
  documents that each match many foreign documents were never bounded in total.

To get one row per element instead, `$unwind` the array first and join each row,
which is a stage more and says what it means: the stage then joins each
element's key alone and `as` holds that element's matches.

**A key is a value, so a missing key is not `null` here.** An input document
that lacks `localField` gets an empty `as` and joins nothing, and a foreign
document that lacks `foreignField` is never a candidate — an explicit `null`
on both sides joins, an absent field on either does not. The [filter rule
that `null` matches a missing
field](query-language.md#1-null-matches-missing-fields) does not reach here:
that rule is about selecting documents, and a join is about matching two
stored values to each other.

### The `let` / `pipeline` form

```json
[ { "$lookup": {
      "from": "lines",
      "let": { "oid": "$_id", "min": "$minQty" },
      "pipeline": [
        { "$match": { "kind": "line" } },
        { "$addFields": { "mine": { "$eq": ["$order", "$$oid"] },
                          "over": { "$gte": ["$qty", "$$min"] } } },
        { "$match": { "mine": true } },
        { "$project": { "_id": 1, "over": 1 } } ],
      "as": "lines" } } ]
```

`let` evaluates each expression **against the input document** and binds the
result under that name; `pipeline` then runs **over the foreign collection**
with those names in scope, and whatever comes out is the `as` array for that
document. Inside the sub-pipeline `$field` and `$$ROOT` are the *foreign*
document, the `let` names are the only way to reach the local one, and every
stage takes them — `$project`, `$addFields`, `$group`, `$replaceRoot` and a
nested `$lookup`'s own `let` included. `let` may be omitted for an uncorrelated
join, and a `$lookup` may not carry both `localField`/`foreignField` and
`pipeline` (or `let`): that is a `400`, kept on purpose ([ADR-217](decisions.md#adr-217--the-refusals-and-differences-ruled-to-stay-are-recorded-as-a-set)). Join
on the key, then reshape the attached array with `$filter` or `$map` in the
following `$addFields`: `{"$addFields": {"unpaid": {"$filter": {"input":
"$unpaid", "as": "i", "cond": {"$eq": ["$$i.paid", false]}}}}}` after an
equality `$lookup` into `unpaid`. The equality join attaches every match
before the filter drops any, so the ceiling below counts what the join
attached. The refusal is revisited if a caller meets that ceiling.

**A correlated equality is a join; anything else is a nested loop**
([ADR-222](decisions.md)). The sub-pipeline may do anything at all with the
variables, so in general there is no one key to index the foreign side by, and
the stage runs once per input document over the foreign collection, which is
O(local × foreign). The common case is recognised and is not that: when the
leading `$match` holds a clause

```json
{ "$expr": { "$eq": [ <an expression of the foreign document>, <an expression of the let variables> ] } }
```

(either argument first, any expressions, at the top of the `$match` or inside
`$and`, however deeply nested), the stage files the foreign collection in memory
by the first side, once, and looks each input document up by the second. The
first input document is run as a nested loop, so a `$lookup` with a single input
document pays for no table; the table is built for the second. What the join
returns is what the nested loop returns: the same documents, in the foreign
collection's scan order. The cost goes from O(local × foreign) to O(local +
foreign) plus what is attached: 10,000 inputs against 100,000 foreign documents
took 104 s as a nested loop and 86 ms as the join. The other clauses of the
`$match` stay: the ones that name no variable are applied once to the foreign
collection, before the loop, as a `$match` with no variable always was, and
the rest (further `$eq` terms, `$gt`, anything under `$or`) are checked on the
documents the key finds.

The equality is `$expr`'s `$eq`, not the filter's and not the `localField` form's:
numbers are equal across types (`5`, `5.0`), `null` and a missing field are the
same value (so a foreign document without the field joins an input whose variable
is `null`, where the `localField` form joins nothing), `NaN` equals `NaN`, an array
is compared whole (never opened) and a document field by field in order, and a
`Decimal128` is equal to the numbers of its value (`0.1` the decimal is not `0.1`
the double). Every value is filed by its key, which is equal for two values
exactly when `$eq` calls them equal, a decimal included. Any input document whose
evaluation raises runs as the nested loop, which raises exactly what the loop
would, or nothing. A join never raises an error the loop would not.

What does not join runs as the nested loop, bounded only by the 100,000 ceilings
below: an `$or` or `$nor` with a variable in it, a comparison other than `$eq`
(`$gt`, `$ne`), a variable read inside an `$elemMatch` body, an `$eq` whose two
sides both read the document and a variable, a `$match` that is not first. For
an equality on one field, the `localField`/`foreignField` form is still the
simplest; the `$expr` form is for a join that has more to say (a key that is
computed, an extra range, a filter on the foreign side) and keeps the pipeline.
A nested `$lookup` inside a sub-pipeline is run for each outer input, so its table
is built again for each. An input document that falls back to the nested loop
costs one scan of the foreign collection.

**The ceiling applies throughout.** The foreign collection, each sub-pipeline
stage's output and the total of everything attached across all input documents
are each held to the 100,000-document limit below, so the nested loop cannot
be used to occupy a node's memory by degrees.

**Where `$match` meets `let`.** A `$match` in the sub-pipeline reads the `let`
names (this `$lookup`'s and any enclosing one's) through `$expr`, and only
there: `{"$match": {"$expr": {"$eq": ["$order", "$$oid"]}}}`, at any depth of the
filter, an `$elemMatch` body included, where the element is the document and the
names are bound beside it. The filter language itself has no variables, so a
`$$oid` as a *value* of a clause is refused: any string value beginning with `$$`,
anywhere in a sub-pipeline `$match` outside an `$expr` (a plain equality, inside
`$in` or `$elemMatch`, under `$and`/`$or`/`$nor`, a `$regex` pattern) is a 400
naming it, whether or not the `$lookup` has a `let`. The scope is the
sub-pipeline: a top-level `$match`, and a `find` filter, read `"$$oid"` as the
literal string a stored document may hold and bind no names. There is no
plain-value escape inside a sub-pipeline `$match`, and the register records
that as a deliberate difference from MongoDB ([ADR-217](decisions.md#adr-217--the-refusals-and-differences-ruled-to-stay-are-recorded-as-a-set)). **A stored string
that begins with `$$` is matched there through `$expr` and `$literal`**, which
the expression parser reads as a string and never as a reference:
`{"$match": {"$expr": {"$eq": ["$code", {"$literal": "$$promo"}]}}}`. That
comparison is `$expr`'s `$eq`, not the filter's. A name that is not in scope is
refused, naming it. Variables still bind by need ([ADR-219](decisions.md)): a
name nothing reads is never evaluated, except in a `$lookup`'s own `let`.

**No cross-collection snapshot.** A `$lookup` sees the foreign collection as of
when the stage runs. There are no multi-document transactions in a leaderless
store ([ADR-006](decisions.md)), so there is no consistent snapshot to take —
inherent, not an omission.

---

## `$vectorSearch`

Search a collection by meaning and keep working on the hits. The stage takes the
fields of the [`vector_search` endpoint](vectors.md) and no others: `query` (text,
embedded by the collection's provider) or `vector` (required under `byo`), `k`,
`per_document` and `filter`.

```json
POST /v1/db/shop/coll/products/aggregate
{
  "pipeline": [
    { "$vectorSearch": { "query": "lightweight running footwear", "k": 10 } },
    { "$match": { "price": { "$lt": 100 } } },
    { "$group": { "_id": "$brand", "n": { "$sum": 1 } } }
  ]
}
```

```json
{ "documents": [ { "_id": "acme", "n": 1 } ], "count": 1 }
```

- **It is the source, so it is first.** Anywhere else, and inside a `$lookup`
  sub-pipeline, is a `400` that says so.
- **What enters the pipeline.** Each hit is its source document with two fields
  added, `_score` and `_chunk`, in rank order. If the stored document has a
  field of either name, the stage's value replaces it. With `per_document` above
  1 the same source document enters once per matching chunk, each time with its
  own `_chunk`; the default is one chunk per document. `k` (default 10, at most 1,000, as
  on the endpoint) bounds how many enter; the [pipeline limit](#the-memory-limit)
  is checked against them too. A document deleted since it was ranked is dropped,
  as on the endpoint.
- **Authorization.** The stage needs `read` on the collection, which carries
  `search`. A caller granted only `search` can use the endpoint but is refused
  by `aggregate`, because a pipeline reads documents. Every refusal is the same
  uniform `403`, naming no action.
- **Embedding.** The same checks and messages as the endpoint: `query` needs a
  server-side provider, and under `byo` the stage needs a `vector` of the
  collection's width.
- **The names of other systems are refused, not aliased.** `queryVector` should
  be `vector` and `limit` should be `k`; `numCandidates` and `index` have no
  equivalent, because KimmyDB has no candidate knob and one vector index per
  collection. Each is a `400` saying so. See [ADR-216](decisions.md).
- **`explain`.** `explain: true` plans the pipeline and reports the vector search as its source; see [Performance](#performance).

---

## Performance

**A leading `$match` is planned exactly as `find` is.** When the first stage is
`$match` — or the first several are, which run as one conjunction — its filter
goes through the same planner and the same access paths: a primary-key lookup
for `_id`, an index for an equality, range or `$in` on an indexed field, a
collection scan otherwise. An indexed leading `$match` reads its candidates and
nothing else, so a pipeline over a large collection costs what its `$match`
admits, not what the collection holds.

```json
[ { "$match": { "city": "London", "placed": { "$gte": "2026-08-01" } } },
  { "$group": { "_id": "$sku", "n": { "$sum": 1 } } } ]
```

With an index on `[city, placed]`, the group sees London's August orders and
the rest of the collection is never touched.

**Only the leading `$match` is pushed down.** A `$match` after `$project`,
`$unwind`, `$group` or any other stage reads the documents *that stage
produced*, and moving it to the source would change what it matches — so it
stays where it was written and runs as an ordinary in-memory filter over what
reaches it. The rule is the same one MongoDB's optimizer follows, and it is
what keeps a pipeline's meaning independent of whether an index exists.

**`explain: true` plans a pipeline and runs nothing** ([ADR-221](decisions.md)).
Send `{"pipeline": [...], "explain": true}`: the answer is `{"explain": {...}}`
with `"executed": false`, **no documents and no count**. It is the only `explain`
that does not run what it explains, and so the only one with no
`documentsExamined`, `documentsMatched` or `indexEntriesRead`: nothing was
examined or read.

```json
{ "explain": { "executed": false,
    "source": { "strategy": "index", "index": "city_1", "indexFieldsUsed": 1,
                "stages": 1, "maxDocuments": 100000 },
    "stages": [ { "stage": "$group", "runs": "inMemory",
                  "detail": "groups everything that reaches it in memory; blocking, ..." } ] } }
```

- **`source`** is the plan `find` would make for the leading `$match` (the same
  planner: `strategy`, `index`, `indexFieldsUsed`, `probes`), with `stages` saying
  how many leading stages it took and `maxDocuments` the ceiling a run enforces.
  A range plan that used both ends carries a `note`: the scan re-checks the index
  when it runs and falls back to a collection scan if it has become multikey since.
- **`stages`** has one entry for each later stage: it runs in memory, and says what
  it does. No counts and no timings: nothing ran.
- **`$lookup`** says what it will do: the `localField`/`foreignField` form scans
  the foreign collection once and files it in memory by the key
  (`foreignScanHashed`; no index on the foreign field is used), and the
  `let`/`pipeline` form scans it once and runs its sub-pipeline for each input
  document (`foreignScanNestedLoop`), reading nothing when no document reaches it.
  When the leading `$match` holds a correlated `$eq` ([ADR-222](decisions.md)) the
  strategy is `foreignScanHashJoin`, with `joinKey` (`foreign` and `local`, as
  `$field`, `$$name` or `an expression`), `residualClauses` (how many other clauses
  that read a `let` variable are checked on what the key finds) and `fallback` (the
  run-time conditions under which it runs as the nested loop instead). The
  sub-pipeline's own stages are listed, marked `perInputDocument`. A leading `$match`
  that reads no variable is applied once, in memory, and is never planned. One that
  reads a variable is two entries when part of it does not: `part: independent`
  (`perInputDocument: false`, applied once) and `part: variable` (`true`, per input
  document, or answered from the hash table); with nothing independent it is one
  entry.
- **`$vectorSearch`** is the source: `strategy: vectorIndex`, the collection's
  vector configuration and `queryUsable`, whether a run would take the query as
  sent. It calls no embedding provider and reads no vector, so it cannot say
  whether any is stored.
- **It needs what a run needs**: `read` on the collection and on every `$lookup`
  collection, nested ones included (so it can ask for more than a run that no
  document reaches), and `search` for `$vectorSearch`. Pipelines that fail on data
  when run still plan. There is no `$out` or `$merge`: nothing is written.

---

## The memory limit

`$group`, `$sort` and `$count` are **blocking**: none of them can emit anything
until it has consumed everything — a sort has no first element until the last
is seen, a group has no totals until the last member arrives, and a count has
no number until the last document has gone past. All three work on the whole
materialised input, and `$count` emits a single document however large that
input is. `$unwind` and `$lookup` can *grow* their input, which is the other
way a stage can cost more than it was given. `find` is bounded by `MAX_LIMIT`,
but a pipeline's input is a whole collection — so without a ceiling one request
could take all the memory on a node.

Every stage checks its output against a cap of **100,000 documents**, and
exceeding it is an error naming the stage. **The ceiling applies to what the
leading `$match` admits**, not to the collection: a pipeline over a million
documents runs when its first `$match` selects fewer than 100,000 of them, and
a pipeline with no leading `$match` over that collection is refused at the
source, as it always was. The scan stops as soon as the ceiling is passed
rather than materialising everything to refuse it.

```json
{ "error": "bad_request",
  "message": "$group produced 148230 documents, over the pipeline limit of 100000.
              Narrow the pipeline with an earlier $match" }
```

**It refuses rather than truncating.** A `$group` over 90% of the input looks
exactly like a `$group` over all of it — the caller has no way to detect the
difference, so a partial answer would be worse than none. It also does not spill
to disk: a pipeline that cannot run should say so immediately rather than
becoming slow in a way that is harder to diagnose than a refusal.

`$unwind` is checked *while* it expands, not after, because a handful of
documents holding large arrays can exceed the cap long before the stage ends.

---

## Not supported

| | Why |
|---|---|
| System variables other than `$$ROOT`, `$$CURRENT`, `$$NOW` and `$$REMOVE` — `$$DESCEND`, `$$PRUNE`, `$$KEEP` | Not built. Refused with a message saying so, rather than as an unknown name |
| `$lookup` with both `localField`/`foreignField` and `pipeline` | Refused, by decision ([ADR-217](decisions.md#adr-217--the-refusals-and-differences-ruled-to-stay-are-recorded-as-a-set)). Join on the key, then `$filter`/`$map` the attached array in the next stage |
| `$mod` with a `Decimal128` | Refused, `400`, `$mod needs numbers, found a Decimal128`. Date arithmetic with one is refused too (`$add needs numbers or one date, found a Decimal128`). |
| `$merge` with `on` other than `_id`, `whenMatched`/`whenNotMatched: "fail"`, a pipeline `whenMatched`, `let`; `$out` into a collection that holds documents, `$out` with `timeseries` | Refused, `400`, each naming what and why. [`$merge`](#merge), [`$out`](#out), and the [deviations register](deviations.md) |
| `$vectorSearch` anywhere but first, or inside a `$lookup` sub-pipeline | `400`, naming the rule: it is the pipeline's source, in place of the collection scan |
| `$vectorSearch` fields from other systems — `queryVector`, `numCandidates`, `limit`, `index` | Refused, each naming the KimmyDB spelling (`vector`, `k`) or saying there is no such knob: KimmyDB has one vector index per collection ([ADR-216](decisions.md)) |
| Index use by a `$match` that is not first | Deliberate — see [Performance](#performance). Only the leading `$match` reads through the planner; a later one filters what reaches it |

---

## From MCP

The `aggregate` tool takes the same pipeline and runs through the same executor
as this route, so an agent cannot reach anything the REST API would refuse it —
including through `$lookup`, which is asserted at both edges.

---

## Next

- [HTTP API](http-api.md) — the endpoint reference
- [Query Language](query-language.md) — the `$match` and `$project` languages
- [Decisions](decisions.md) — ADR-024 on why both edges share one executor,
  ADR-105 on the expression scope
