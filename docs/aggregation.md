# Aggregation

[← Documentation index](README.md)

Grouping, reshaping and joining, done in the database rather than in the client.

```
POST /v1/db/{db}/coll/{coll}/aggregate
{ "pipeline": [ { "$match": … }, { "$group": … } ] }
```

A pipeline is an array of stages applied in order. Each stage takes the previous
stage's output, so ordering is the main thing that decides what a pipeline
costs: **put `$match` first**, so every later stage sees less.

---

## Stages

| Stage | Notes |
|---|---|
| `$match` | The same filter language as `find` — all 18 operators, `$expr` included |
| `$project` | The same projection language as `find`, **plus computed fields** |
| `$addFields`, `$set` | Add computed fields, keeping everything else. Two names for one stage |
| `$replaceRoot` | `{$replaceRoot: {newRoot: <expression>}}` — the computed document becomes the document |
| `$sort` | The same sort language. Blocking |
| `$skip`, `$limit` | Non-negative whole numbers |
| `$unwind` | One output document per array element |
| `$group` | Blocking. Accumulators below |
| `$count` | `{$count: "name"}` — a document holding the count |
| `$lookup` | Join another collection, by one key or by a sub-pipeline. **Authorized separately** |

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
| Iteration | `$filter` `$map` `$reduce` — each binds a variable per element |
| Variables | `$$ROOT` `$$CURRENT` `$let` |
| Escape | `$literal` |

```json
[ { "$addFields": {
      "value": { "$multiply": ["$qty", "$price"] },
      "label": { "$toUpper": "$city" },
      "band":  { "$cond": [ { "$gte": ["$qty", 10] }, "bulk", "single" ] },
      "month": { "$dateToString": { "date": "$placed", "format": "%Y-%m" } } } },
  { "$group": { "_id": "$month", "revenue": { "$sum": "$value" } } } ]
```

### How a value is read

- `"$field"` is a **field path**; a bare string is a literal.
- `"$$name"` is a **variable** — see below — and `"$$name.path"` reads into its
  value.
- A document whose **first key starts with `$`** is an **operator**, and it may
  not carry any other key.
- **Any other document is computed** — its values are expressions. This is what
  makes a compound `$group` key work.
- `{$literal: x}` yields `x` untouched, which is how you produce the *string*
  `"$total"` or a document with a `$`-prefixed key.

### Behaviours worth knowing

**`$cond` and `$ifNull` do not evaluate the branch they do not take.** A guard
like `{$cond: [{$gt: ["$n", 0]}, {$divide: [100, "$n"]}, null]}` works, rather
than failing on exactly the inputs it exists to protect.

**Null propagates; a type violation refuses.** `{$add: ["$typo", 1]}` is null
because a missing field is null. `{$add: ["text", 1]}` is a 400. Returning null
for both would make a typo and a type error indistinguishable in the output.

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

**Reach into array elements with `$map`, not a dotted path.** A field path
that crosses an array — `$items.sku` — yields the *first* matching value here,
not an array of them as it does in MongoDB (a long-standing behaviour of the
expression layer's path resolution, now more visible). `{$map: {input:
"$items", in: "$$this.sku"}}` is the array of skus.

**`$range`** produces at most 100,000 integers — the same ceiling as the
pipeline, for the same reason: `{$range: [0, 1000000000]}` is a memory
exhaustion written as an expression, and it is refused before anything is
allocated. Its elements are 64-bit integers, as every integer result here is.

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
`$$oid` in one is refused. Correlate in a computed field and `$match` on that,
as the example does. `$expr` in a filter — which is the natural place for a
correlation, `{$match: {$expr: {$eq: ["$order", "$$oid"]}}}` — is a separate
addition to the filter language and, once the two compose, will be the direct
way to write it.

**No cross-collection snapshot.** A `$lookup` sees the foreign collection as of
when the stage runs. There are no multi-document transactions in a leaderless
store ([ADR-006](decisions.md)), so there is no consistent snapshot to take —
inherent, not an omission.

---

## The memory limit

`$group` and `$sort` are **blocking**: neither can emit anything until it has
consumed everything. `$unwind` and `$lookup` can *grow* their input. `find` is
bounded by `MAX_LIMIT`, but a pipeline's input is a whole collection — so
without a ceiling one request could take all the memory on a node.

Every stage checks its output against a cap of **100,000 documents**, and
exceeding it is an error naming the stage:

```json
{ "error": "bad_request",
  "message": "$group produced 148230 documents, over the pipeline limit of 100000.
              Narrow the pipeline with an earlier $match, …" }
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
| Set operators (`$setUnion`, `$setIntersection`, `$setDifference`, `$setEquals`, `$allElementsTrue`, `$anyElementTrue`) | Not built. The array operators above cover membership and iteration; the set family is a further pass over the same scope |
| `$zip`, `$objectToArray`, `$arrayToObject`, `$sortArray` | Not built |
| System variables other than `$$ROOT` and `$$CURRENT` — `$$NOW`, `$$REMOVE`, `$$DESCEND`, `$$PRUNE`, `$$KEEP` | Not built. Refused with a message saying so, rather than as an unknown name |
| `$lookup` with both `localField`/`foreignField` and `pipeline` | Refused. Join on the key, then `$filter`/`$map` the attached array in the next stage |
| Type conversion (`$convert`, `$toInt`, `$toString`, `$toDate`) | Not built |
| `$facet`, `$bucket`, `$graphLookup`, `$merge`, `$out` | Not built. An unknown stage is refused with a message listing what is supported |
| `$vectorSearch` as a stage | Vector search is its own endpoint — see [Vectors](vectors.md) |
| Index-aware `$match` | A pipeline reads the collection; the planner is not consulted. A selective `$match` still helps, by shrinking what later stages see |

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
