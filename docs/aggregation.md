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
| `$match` | The same filter language as `find` — all 17 operators |
| `$project` | The same projection language as `find`, **plus computed fields** |
| `$addFields`, `$set` | Add computed fields, keeping everything else. Two names for one stage |
| `$replaceRoot` | `{$replaceRoot: {newRoot: <expression>}}` — the computed document becomes the document |
| `$sort` | The same sort language. Blocking |
| `$skip`, `$limit` | Non-negative whole numbers |
| `$unwind` | One output document per array element |
| `$group` | Blocking. Accumulators below |
| `$count` | `{$count: "name"}` — a document holding the count |
| `$lookup` | Join another collection. **Authorized separately** |

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
| Array and set operators (`$size`, `$arrayElemAt`, `$slice`, `$filter`, `$map`, `$reduce`) | Deliberately out of the first expression pass. `$map`/`$filter`/`$reduce` also need `$$this`-style variable binding, which is an evaluation *scope* rather than another operator |
| Variable expressions — `$$ROOT`, `$$this`, `$let` | Same reason. Refused explicitly rather than read as a field named `$ROOT`, which would silently yield null |
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
- [Decisions](decisions.md) — ADR-024 on why both edges share one executor
