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
| `$match` | The same filter language as `find` — every operator it has, `$expr` and `$mod` included. **Planned like `find` when it is the first stage** |
| `$project` | The same [projection language](query-language.md#sort-and-projection) as `find`, a path through an array included, **plus computed fields**. Below |
| `$addFields`, `$set` | Add computed fields, keeping everything else. Two names for one stage |
| `$replaceRoot` | `{$replaceRoot: {newRoot: <expression>}}` — the computed document becomes the document |
| `$sort` | The same [sort language](query-language.md#sort-and-projection), down to which element a key through an array reads. [Blocking](#the-memory-limit) |
| `$skip`, `$limit` | Non-negative whole numbers |
| `$unwind` | One output document per array element. Below |
| `$group` | [Blocking](#the-memory-limit). **One row per distinct key**, so an empty input produces no rows at all. Accumulators below |
| `$count` | `{$count: "name"}` — a document holding the count, **always**: over an empty input it is one document holding `0`, not no document. [Blocking](#the-memory-limit) |
| `$lookup` | Join another collection, by one key or by a sub-pipeline. **Authorized separately** |
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
| `$sum` | **Ignored** — a string, a bool, an array, a document contributes nothing. A field holding `42` on some documents and `"42"` on others sums only the first kind | Ignored | `0` |
| `$avg` | Ignored, in the numerator **and** the count — a field present on half the documents gives the mean of that half, not half the mean | Ignored | `null` |
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
keeping the first of each in input order: `5`, `5.0` and `{"$numberLong": "5"}`
are one member, every `NaN` is one, `0.0` and `-0.0` are one, and `null` and
`undefined` are one. Two documents are one member only with their keys in the
same order, and two `Decimal128`s only with the same bytes; a `Decimal128` is
never the same member as a number. A value holding a `Decimal128` anywhere,
nested or in an array, cannot be key-encoded and is identified by its
rendering, so inside it `1` and `1.0` are not one member; `$group` does the
same.

---

## Expressions

Anywhere a value is computed — a `$project` field, `$addFields`, `$replaceRoot`,
a `$group` key, an accumulator's argument — takes an **expression**, not just a
field path.

| Group | Operators |
|---|---|
| Arithmetic | `$add` `$subtract` `$multiply` `$divide` `$mod` |
| Strings | `$concat` `$toUpper` `$toLower` `$substr` (`$substrCP`) `$split` `$strLenCP` |
| Conditional | `$cond` `$ifNull` `$switch` |
| Comparison | `$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$cmp` |
| Boolean | `$and` `$or` `$not` |
| Dates | `$year` `$month` `$dayOfMonth` `$hour` `$minute` `$second` `$dateToString` |
| Arrays | `$size` `$arrayElemAt` `$first` `$last` `$slice` `$concatArrays` `$in` `$indexOfArray` `$isArray` `$reverseArray` `$range` |
| Sets | `$setUnion` `$setIntersection` `$setDifference` `$setEquals` `$setIsSubset` `$anyElementTrue` `$allElementsTrue` |
| Documents | `$objectToArray` `$arrayToObject` |
| Iteration | `$filter` `$map` `$reduce` — each binds a variable per element |
| Variables | `$$ROOT` `$$CURRENT` `$let` |
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
`400` naming `condition`, not a silently unfiltered array.

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
about a value, a bug in the server, is never held back this way. `$let` still
evaluates every variable before its body, whether the body reads it or not.

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

### Variables

An expression evaluates in a **scope**: the document, plus whatever the
constructs around it have bound. A variable is written `$$name`, and
`$$name.path` reads a field out of its value.

| Variable | Bound by | Value |
|---|---|---|
| `$$ROOT` | always | the document the expression is evaluated against |
| `$$CURRENT` | always | the same document — nothing here rebinds it |
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
`$$NOW`, `$$REMOVE`, `$$DESCEND` and the rest — are refused with a message that
says they are unsupported rather than misspelled.

**`$let` values see the enclosing scope, not each other.** `{vars: {a: 1, b:
"$$a"}}` is refused; nest a second `$let` to build on the first. **A user
variable name** starts with a lowercase letter and continues with letters,
digits and underscores — MongoDB's rule, which also keeps user names from ever
colliding with the uppercase system ones.

### Arrays

**Null in, null out; a non-array refuses.** Every array operator returns null
when its array is null or the field is missing, so a sparse collection does not
fail the pipeline, and errors when it is any other type, so `{$size: "$name"}`
on a string is a 400 rather than a silent null. `$isArray` is the exception and
answers `false` for anything that is not an array.

**`$arrayElemAt` counts from the end when negative** (`-1` is the last element)
and is null when the index is out of range on either side. **`$first`** and
**`$last`** are `$arrayElemAt` at `0` and `-1`; on an empty array they are null.
**A fractional index is refused**, not truncated.

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
allocated. Its elements are 64-bit integers, as every integer result here is.

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

**A `Decimal128` anywhere in a set input is refused.** The canonical order
ranks a `Decimal128` equal to every other number, so `[Decimal128("5")]` would
be a subset of `[1]`. Rather than answer that, the operator refuses, `400`,
naming the argument: `$setIsSubset cannot compare a Decimal128 (argument 1
holds one)`. Nested inside an element counts. Beside a null nothing is
compared, and the result is null.

**`$anyElementTrue` and `$allElementsTrue` read truthiness** as `$and` and
`$or` do: `false`, `null`, `0` and missing are false, and everything else is
true, the empty array included, and an array element is not looked inside.
An empty array is `false` for `$anyElementTrue` and `true` for
`$allElementsTrue`. A stored `Decimal128` element is read like any other
number, false when it is a zero; unlike the set operators these compare
nothing, so there is nothing to refuse. The argument is one
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
| `string` | number (a whole double prints as `"2"`, not `"2.0"`); bool; date as ISO 8601 with milliseconds, `"2026-08-12T13:45:07.250Z"`; ObjectId as 24 hex characters |
| `bool` | a number is `false` when zero, a stored `Decimal128` zero of any sign or exponent included; **everything else present is `true`** — including `""` and `"false"`, which is MongoDB's rule and a trap worth knowing |
| `date` | a number as epoch milliseconds (a double is truncated); a string in RFC 3339 / ISO 8601 with an offset or `Z`, or the looser forms `"2026-08-12"`, `"2026-08-12 13:45:07"` and a missing zone, all read as UTC; an ObjectId's creation time |
| `objectId` | a string of 24 hex characters |

Everything not in the table — an array to a number, a document to a date — is
an error. **`decimal` (`Decimal128`) is refused at parse**: it has no exact key
encoding here ([ADR-005](decisions.md)), so a value converted to it could be
neither indexed nor grouped, and producing one would only move the refusal
somewhere less obvious. Convert to `double` or `long` instead.

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
silently.

**`$group` buckets by value, not by type.** `5`, `5.0` and `5i64` share a
bucket, because they share an index entry everywhere else in this database. The
grouping key goes through the same encoder the indexes use.

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
  explicit `null` joins an explicit `null`. A stored `Decimal128` cannot be a
  key and joins nothing, and inside an array the rest of the array still joins:
  `[Decimal128, 5]` joins on `5`.
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
`pipeline`: join on the key, then reshape the attached array with `$filter` or
`$map` in the following `$addFields`.

**This form is O(local × foreign).** The sub-pipeline may do anything at all
with the variables, so there is no one key to index the foreign side by; it is
run once per input document, over a copy of the foreign collection. Two things
keep that tolerable: the foreign collection is read from storage **once** and
held in memory for the stage, and a **leading `$match`** is applied once,
before the loop, because a filter has no access to the variables. Put one
first whenever there is a constant condition — it shrinks what every iteration
copies. A join that is an equality on one field belongs in the
`localField`/`foreignField` form, which is a single pass however large either
side is; the pipeline form is for the joins that form cannot express — a
range, a computed key, a sub-pipeline that groups or reshapes before
attaching.

**The ceiling applies throughout.** The foreign collection, each sub-pipeline
stage's output and the total of everything attached across all input documents
are each held to the 100,000-document limit below, so the nested loop cannot
be used to occupy a node's memory by degrees.

**Where `$match` meets `let`.** A `$match` inside the sub-pipeline is the
ordinary filter language, and the filter language has no variables, so a
`$$oid` in one is refused: any string value beginning with `$$`, anywhere in
a sub-pipeline `$match` — a plain equality, inside `$in` or `$elemMatch`,
under `$and`/`$or`/`$nor`, a `$regex` pattern — is a 400 naming it, whether
or not the `$lookup` has a `let`. The one exception is the subtree under
`$expr`, which the expression parser owns and checks by its own rule. The
scope is the sub-pipeline: a top-level `$match`, and a `find` filter, read
`"$$oid"` as the literal string a stored document may hold. There is no
literal escape inside a sub-pipeline `$match` — a stored string that begins
with `$$` cannot be matched there — and the register records that as a
deliberate difference from MongoDB. Correlate in a computed field and
`$match` on that, as the example does. `$expr` in a filter
— which is the natural place for a correlation,
`{$match: {$expr: {$eq: ["$order", "$$oid"]}}}` — is a separate addition to
the filter language and, once the two compose, will be the direct way to
write it.

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
- **No `explain`.** `aggregate` has none; see [Performance](#performance).

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

**`aggregate` has no `explain`.** To see which access path a leading `$match`
gets, send the same filter to `find` with `"explain": true` — it is the same
planner reading the same indexes, so the answer is the same.

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
| `$zip`, `$sortArray` | Not built, by decision: both are refused at parse as **unknown operators**. `$map` over a `$range` of indexes, with `$arrayElemAt`, reads several arrays by position, and an `$unwind`, `$sort` and `$group` with `$push` orders an array's elements |
| System variables other than `$$ROOT` and `$$CURRENT` — `$$NOW`, `$$REMOVE`, `$$DESCEND`, `$$PRUNE`, `$$KEEP` | Not built. Refused with a message saying so, rather than as an unknown name |
| `$lookup` with both `localField`/`foreignField` and `pipeline` | Refused. Join on the key, then `$filter`/`$map` the attached array in the next stage |
| `$convert` to `decimal` (or code `19`) | `Decimal128` has no exact key encoding ([ADR-005](decisions.md)); refused at parse, naming `double` and `long` as the alternatives |
| A `Decimal128` literal — bare, under `$literal`, or inside a document or array written in an expression | The same reason from the other side: the canonical order ranks a Decimal128 equal to every other number, so a comparison against one would hold for every number and a value computed from one could be neither indexed nor grouped. Refused at parse — `a Decimal128 literal is not supported in an expression` — wherever an expression is parsed, `$expr` included. A `$sort` stage refuses a document holding one at a sort path the way `find`'s sort does ([Query language](query-language.md#sort-and-projection)); a *stored* Decimal128 read through a field path is not refused, and `$type` finds it, except by the set operators, which refuse one in their input rather than compare it ([ADR-207](decisions.md), [Sets](#sets)); where truth is read, a `Decimal128` zero is false like any zero ([What reads as false](#behaviours-worth-knowing)) |
| `$toDecimal` | Never built as an operator at all, so it is refused at parse as an **unknown operator** — `unsupported operator "$toDecimal": not an expression operator` — rather than with the pointer `$convert` gives. The reason is the row above; the message does not say so |
| `$facet`, `$bucket`, `$graphLookup`, `$merge`, `$out` | Not built. An unknown stage is refused with a message listing what is supported |
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
