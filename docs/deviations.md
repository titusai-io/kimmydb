# Deviations Register

[← Documentation index](README.md)

Where the implementation differs from what was planned or requested, why, and
what it would take to close.

Individual notes live near the code they affect, but scattered notes are not
reviewable — this is the single place to see the whole picture. **Every entry
here is a debt, not a decision that has been quietly retired.**

Status meanings:

- 🔴 **Open drift** — differs from an explicit request, not yet agreed
- 🟡 **Agreed deferral** — differs from the plan, explicitly accepted
- 🟢 **Superseded** — the plan changed for a recorded reason

---

## 🟡 A compound index over two array fields indexes nothing for that document, where MongoDB refuses the write

**Raised 2026-09-06, with ADR-139.** MongoDB refuses a write that would put
arrays at two of a compound index's paths ("cannot index parallel arrays"),
and refuses to build such an index over existing documents. KimmyDB used to
do the same, per document, at write time. It no longer refuses either: the
document is stored and filed under the index's *unkeyed* run, which every
scan of that index reads beside its ranges and rechecks like any other
candidate, so every query still finds it and none finds it wrongly. The same
holds for a document that would produce more than 1,000 keys and for one
holding a `Decimal128` at an indexed path. A unique index still refuses such
a document on a local write, because a unique index must be able to key
every document it covers.

**Why.** A refusal that depends on an index is a constraint, and a leaderless
store cannot hold a constraint across members without coordination
(ADR-020). A member that had not yet received a definition legally accepted
a document the definition's holder could then neither apply nor skip, and
the holder's replication stopped for the life of the process (ADR-139). The
refusal bought a smaller index and an early error for a badly shaped
document; it cost a cluster its convergence. MongoDB can afford the refusal
because every write goes through one primary and an index build commits on
a quorum; a store that accepts writes on every member cannot.

**What closes it.** Nothing needs to: a client that wants the refusal back
polices document shape itself, or splits the compound index into single-field
indexes, which key every shape. The `unkeyed` field on the index listing and
`unkeyedCandidates` on `explain` say when it matters.

---

## 🟡 A `$lookup` join key that crosses an array reads the first element, where MongoDB joins on every one

**Raised 2026-09-04, by the 2026-09 test round, which asked which value
joins and found no doc that answered.** `localField` and `foreignField` are
field paths and the join needs one key per document, so a path whose
non-terminal segment is an array — `items.sku` over `items: [{sku: "ef-9"},
{sku: "gh-3"}]` — resolves to every element's value and then keeps the
**first**. MongoDB fans the key out and attaches the union of every element's
matches. So a stage that reads as "attach the product of every line" attaches
the first line's product, and says nothing about the rest. A field that
merely *holds* an array is not this case: `localField: "tags"` joins on the
whole array as one value, which agrees with MongoDB and is what makes
`["a","b"]` find a foreign document whose key is that same array.

Not fanning out is the right default here and stays: it is the same rule
`$sort` and `$unwind` follow for a path that names a field rather than
computes one, and a join that silently multiplied its input by an array's
length would be a `$unwind` nobody wrote. What was wrong was that nothing
said which of the two it does — ADR-116's sentence about "the single value at
the path" explains why the stage does not fan out, not which value it reads
when there are several. `docs/aggregation.md` now says, and points at
`$unwind` as the way to write the fanning join explicitly.

**To close** — if it is ever worth closing — the fanning form would have to
be opted into rather than made the default, because the ceiling and the cost
both change with it. Nobody has asked.

---

## 🟡 A filter accepts a `$type` alias and a `$regex` flag it has never heard of, and matches nothing with either

**Raised 2026-09-04 — the `$type` half by the 2026-09 test round, the
`$options` half while closing it.** `{"$type": 999}` is a
`400` naming the unknown code. `{"$type": "nosuchtype"}` — and `{"$type":
"Int"}`, and `{"$type": "boolean"}` — is a `200` with no matches, because
the argument is taken as a type *name* and no stored value ever reports that
name. MongoDB refuses an unknown alias, and the two halves here disagree with
each other as well as with it: the same mistake is loud through one spelling
of the argument and silent through the other. An empty array, `{"$type":
[]}`, is accepted the same way — it lists no type, so it matches nothing.

It is the failure [ADR-121](decisions.md) closed for request fields and
[ADR-124](decisions.md) for query parameters, still standing inside the
filter: an empty result that is indistinguishable from a real one. It is
narrower than either, because a filter's keys are the caller's own field
names and only an *operator's argument* can be checked — but `$type`'s
argument is exactly that, a closed vocabulary this database defines, and
`$mod`'s pair, `$size`'s integer and a sort direction are all refused when
they are wrong already.

**The same shape is in `$options`.** `compile_regex` reads the flag string
character by character, sets `i`, `m`, `s` and `x`, and drops everything else
— so `"I"` is not `"i"`, the pattern compiles case-sensitively, and the
caller gets an empty result with nothing said. Two words in this language's
vocabulary that a filter accepts without checking, and they fail the same
way.

**To close:** validate the alias at parse and refuse an unknown one, as the
numeric arm already does — the same error, reached by the other spelling.
The table to validate against is `type_name_of`'s, the set of names a stored
value can actually report, **not** `type_name_for_code`'s: the latter omits
`symbol` and `dbPointer`, which have no numeric code but are real types that
`$type` matches today. Refusing them would be a regression dressed as a fix.
`$options` closes the same way, against its own four flags. Both are
behaviour changes and tightenings, so they belong to a `0.MINOR` with a
changelog line, not to a documentation pass; `docs/query-language.md` states
the current behaviour, the alias list and the flag list in the meantime, so a
reader can at least check a spelling against something.

---

## 🟡 Type conversion is a strict superset of MongoDB's table, and `decimal` is outside it

**Raised 2026-08-30, with `$convert` and the `$toX` shorthands.** The
conversion pairs follow MongoDB's table, and every pipeline MongoDB accepts
means the same thing here. Four places are more lenient or differently
spelled, and one target is missing:

- **`int` → `date` is accepted.** MongoDB refuses it — its table admits
  `long`, `double` and `decimal` to `date` but not `int`, for no reason the
  documentation states — so `{$toDate: 1500}` is an error there and
  `1970-01-01T00:00:01.500Z` here. An Int32 is a small Int64; refusing one and
  not the other would be a rule nobody could predict.
- **`to` is a constant.** MongoDB lets it be an expression, evaluated per
  document. Nothing here needs a per-row target type, and a constant is what
  lets the unsupported `decimal` be refused before a document is read.
- **A string to a date accepts RFC 3339 and three looser spellings** — a bare
  date, a space between date and time, a missing zone read as UTC. MongoDB's
  parser accepts those and more (month names, `%Y%m%d`, a `timezone` argument
  applied to a zoneless string). A string neither accepts is an error in both.
- **A double to a string is Rust's shortest round-trip rendering.** It agrees
  with MongoDB on every ordinary value — `2` for `2.0`, `1.5`, `NaN`,
  `Infinity` — and disagrees on magnitudes MongoDB prints in exponent form:
  `1e21` is `"1000000000000000000000"` here and `"1e+21"` there. The engine's
  own JSON edge prints doubles the same way, so a converted string and a
  returned double read alike.
- **`decimal` is refused.** `Decimal128` has no exact key encoding
  ([ADR-005](decisions.md)); a value converted to it could be neither indexed
  nor grouped. `$convert` to `decimal` or `19` is a `400` naming `double` and
  `long` as the alternatives. **`$toDecimal` is a `400` too, but not that
  one**: the shorthand was never built as an operator, so it is refused as an
  unknown expression operator and the message points at nothing. Both
  refusals are right and only one of them explains itself; giving `$toDecimal`
  its own arm of the refusal is the small change that would close it. A
  `Decimal128` *input* is likewise unconvertible.

**`$mod` and `$pullAll` have no entry.** Both follow MongoDB — `$mod`
truncates doubles toward zero, refuses a zero divisor, and refuses a `NaN` or
unrepresentable operand at parse as MongoDB 5.1 and later do; `$pullAll` on a
non-array field is an error, as it is there. **The leading `$match` pushdown is not a deviation either**: it changes
what a pipeline costs and what the 100,000-document ceiling is measured
against, not what it returns, and the rule — only a *leading* `$match` may be
planned — is the one MongoDB's own optimizer follows.

**Closing the first would mean refusing something harmless**, so it is
recorded rather than scheduled. The third and fourth close by widening the
date parser and adding an exponent threshold to the double renderer, if a
ported pipeline ever needs either.

---

## 🟡 The `$` positional operator is not implemented; `$[<identifier>]` and `$[]` are

**Raised 2026-08-30, with ADR-104.** MongoDB has three ways to address array
elements in an update path: `$[<identifier>]` (the elements an `arrayFilters`
entry selects), `$[]` (every element) and `$` (the first element the *query*
matched). The first two are implemented; `$` is refused at parse time with a
message that points at the first.

**Why not `$`.** Its meaning depends on which element satisfied the filter,
and the matcher answers a `bool`: `filter::matches` tests a path's values with
*any* semantics and never records the index it stopped at. Reporting one would
mean threading a position out of every comparison, a rule for which position
wins when several clauses touch arrays (MongoDB's own rule — the last array
field in the query — is a documented source of surprise), and carrying the
value from the match into the update inside the write transaction.
`$[<identifier>]` needs none of that, because its filter is evaluated per
element at apply time, and it says the same thing more precisely:
`{"items.sku": "gasket"}` with `items.$.shipped` is `items.$[line].shipped`
with `[{"line.sku": "gasket"}]`, and the second reaches every gasket line
rather than the first.

**What differs for a caller.** An update ported from MongoDB that uses `$` is
a `400` rather than a write; the message names the replacement. Two smaller
points, both on the strict side: a filter document that names no identifier —
`{"$and": []}` — is refused rather than read as always-true, and only field
conditions and `$and`/`$or`/`$nor` are accepted at the top of a filter
document. A positional update that selects no element writes the document
back unchanged, so it counts in `modified` exactly as the entry below says a
no-op `$set` does.

**Closing it** means a second evaluation mode for `Filter::Field` that
returns the matching element's index, plumbed from `ModifySpec::matches` into
`update::apply`. Not scheduled: nothing `$` can express is out of reach.

---

## 🟢 The violations report over-stated after a rewrite (was a documented limit in ADR-087)

**Was.** `GET …/violations` derived "still standing" from "every named
document still exists". [ADR-087](decisions.md) recorded the gap in so many
words — a rewrite that changes the colliding value *also* resolves the
collision, but the route did not notice, because it did not re-evaluate index
keys — and argued that re-evaluation would mean re-running index maintenance
in order to read. The recipe in `indexes.md` told an application to *delete or
rewrite*, and only one of the two cleared the report.

**Now.** Closed 2026-08-30. The pass already read every named document to
know it exists; it now also computes that document's keys under the index as
currently defined, with the same function the write path uses, and keeps a
member only if some other member shares one of its keys. A group left with
fewer than two members is not reported, and an index that is gone or no longer
unique contributes none. Deletion and rewrite are one case. Nothing is stored
or written, retention still bounds the pass, and the ADR carries a dated note
correcting its cost argument.

**Kept, and documented.** `merged` names the recorded arrival, so after that
document's own value is rewritten it can name an `_id` outside the group's
`ids`. Also unchanged: a collision is recorded once per merge, so three
documents arriving one after another produce a two-member record and then a
three-member one, and both are reported while both still collide — the
surviving-set deduplication collapses them once they shrink to the same group,
but does not collapse a subset into its superset.

---

## 🟢 `describe` said nothing about durability (hardening item 4.5)

**Was.** [ADR-088](decisions.md) put the node's durability class on
`GET /v1/version` as `durability` and nowhere else. A client that follows the
documented sequence — list, describe, then write — learned everything about a
collection except what an acknowledged write to it means, and the MCP tool
that forwards `describe` had no way to say it either.

**Now.** Closed 2026-08-30. `describe` carries `nodeDurability`, the same
value from the same source, and the MCP `describe_collection` tool forwards
it as it forwards the rest of the document. Named for what it is: a fact about
the node that answered, not a per-collection setting. Specified, and the
conformance test drives it.

---

## 🟡 Array expression operators are lenient about null where MongoDB errors

**Raised 2026-08-30, with the expression scope (ADR-105).** The array operators
follow the expression layer's standing rule — *null propagates, a type violation
refuses* — and in four places that is a strict superset of MongoDB, which
errors instead:

| Operator | Here | MongoDB |
|---|---|---|
| `$size` on null or a missing field | null | error |
| `$in` with a null array | null | error |
| `$range` with a null bound or step | null | error |
| `$slice`, `$arrayElemAt`, `$indexOfArray` with a null count, index or bound | null | error |

A pipeline MongoDB accepts means the same thing here; a pipeline that errors
there may succeed here with a null in the row. Chosen because a sparse
collection is the norm in this database and `{$size: "$tags"}` on a document
without `tags` failing the whole request is the wrong trade; recorded because
someone porting a pipeline that *relies* on the error to catch bad data will
not get it.

**Three smaller divergences, same entry.** `$arrayElemAt` out of range and
`$first`/`$last` on an empty array are **null** rather than MongoDB's *missing*
— an expression here always yields a value, and a projected field is therefore
present with null rather than absent. `$range` produces `Int64` elements where
MongoDB produces `Int32`, as every integer result of the expression layer does
(see the module notes on numbers). And `$filter`, `$map`, `$reduce` and `$let`
**refuse a key they do not know** (`condition` for `cond`), where MongoDB
ignores it; that is stricter, and only turns a silently wrong pipeline into an
error. `$unwind`'s document form, `$lookup`'s both forms, `$replaceRoot`,
`$switch` (and each of its branches), and `$dateToString` refuse an unknown
key the same way (ADR-129) — the same closure, not a new one. Whether MongoDB
itself ignores or refuses an unknown key on each of *those* five is not
verified here, so no claim is made about it either way; the "where MongoDB
ignores it" clause above is scoped to the original four.

**`$lookup` refuses the combined form.** MongoDB 5.0 accepts
`localField`/`foreignField` *and* `pipeline` in one stage — the "concise
correlated subquery". Here it is a 400 that points at the equivalent: join on
the key with the equality form, then `$filter` or `$map` the attached array in
the following stage, which keeps the single pass. Closing this would mean
combining the indexed pass with the per-document loop; not planned until
someone needs it.

**Closing the null leniency** would mean a per-operator strictness flag in the
evaluator for the sake of matching an error, at the cost of the one rule the
expression layer has been able to state in a sentence. Not planned.

---

## 🟡 `$expr` treats an evaluation error as no match, and is accepted under `$elemMatch`

**Raised 2026-08-30, while adding `$expr` to the filter language (ADR-106).**
Two places where the filter's `$expr` is looser than MongoDB's, both by
design and both small.

**A type violation inside the expression is a document that does not match,
not a failed request.** `{$expr: {$gt: [{$add: ["$name", 1]}, 0]}}` over a
collection where one document's `name` is a string: MongoDB fails the whole
query on reaching that document; here the document is skipped and the rest of
the result is returned. `filter::matches` answers a `bool` for every caller —
the scan, `$elemMatch`, the executor's residual re-check after an index probe
— and the regex arm already resolves the same tension the same way: an
unusable pattern matches nothing rather than taking down the request. Parse-
time errors (an unknown operator, a wrong arity, a `$$name` nothing binds) are
still a `400`, so the leniency is confined to failures that depend on the data.
`$$ROOT` and `$$CURRENT` were parse-time errors here too until the expression
scope landed (ADR-105); they now name the document under consideration, which
inside a document-form `$elemMatch` is the element. A pipeline's
`$addFields` with the same expression still refuses, as it did before; the
difference is that a filter *selects* and a stage *derives*, and a
derivation that cannot be computed has no honest value to write.

**`$expr` is accepted inside a document-form `$elemMatch`.** MongoDB refuses
`{lines: {$elemMatch: {$expr: …}}}` outright. Here the element is a document
and `$elemMatch`'s body is an ordinary filter over it, so the expression reads
the element's fields — `{$elemMatch: {$expr: {$gt: ["$qty", "$min"]}}}`
compares two fields of one element, which MongoDB needs `$map` and
`$anyElementTrue` for and this database does not have. A strict superset: a
filter MongoDB accepts means the same thing here.

**`$expr` is *not* accepted in an `arrayFilters` entry, where MongoDB takes
it.** An entry takes field conditions on its identifier and `$and`/`$or`/`$nor`
to group them, and refuses every other `$`-operator — the rule predates `$expr`
joining the filter language (ADR-106 landed after ADR-104) and was left alone
rather than widened as a side effect of the two meeting. `{"line.qty": {"$gt":
5}}` covers what an array filter is usually for; comparing two fields *of the
same element* is what it cannot express.

**To close:** thread a `Result` through `filter::matches` and its callers so
an evaluation error can surface as a `400`, at which point the first item
becomes a choice rather than a constraint. The second is a feature, and would
only be withdrawn if the operator set gained the pipeline-side spelling. The
third is `update::strip_identifier` letting `$expr` through to the filter
parser: `filter::matches_element` already answers one, against the element.

---

## 🟡 A `$$` string in a `$lookup` sub-pipeline `$match` is refused, where MongoDB reads it as a literal

**Raised 2026-09-02, fixing a documented refusal that did not happen.**
`docs/aggregation.md` had said a `$$oid` in a sub-pipeline `$match` was
refused. It was not: the filter language has no variables, so
`{$match: {_id: "$$oid"}}` was the five-character string, matched nothing,
and the join came back as an empty array on every input with a 200. The
refusal now exists, and it is stricter than MongoDB.

**The rule.** Inside a `$lookup` sub-pipeline — with or without a `let`, at
any nesting depth — any string value beginning with `$$`, at any depth of a
`$match` (a plain equality, `$eq`/`$ne`/`$gt`, `$in`/`$nin`/`$all`, `$not`,
`$elemMatch`, the arrays under `$and`/`$or`/`$nor`, a `$regex` pattern), is
a 400 naming the variable and the idiom that works: bind it in an
`$addFields` stage and `$match` on the computed field. The subtree under
`$expr` is exempt; the expression parser owns variables there and refuses an
unbound one by its own rule.

**What that costs.** MongoDB does not substitute variables in a sub-pipeline
`$match` either — `{_id: "$$oid"}` matches nothing there too — but it accepts
the string as a literal, so a stored value that begins with `$$` *can* be
matched from inside a sub-pipeline. Here it cannot: **there is no literal
escape inside a sub-pipeline `$match`.** A top-level `$match` and a `find`
filter keep the literal reading, so such a document is still reachable; it is
only from within a `$lookup` pipeline that it is not.

**Why the stricter side.** The two readings of `"$$oid"` inside a
sub-pipeline are a `let` name the author expected substituted and a literal
that happens to look like one, and the first is overwhelmingly the one
written. Under the lenient reading the first produces an empty join with no
error — a result indistinguishable from a correct one, which is the worse
failure. Closing this would mean an escape syntax for a literal `$$` string
in a sub-pipeline filter, which the filter language has nowhere else; not
planned until someone stores such a value and needs to join on it.

---

## 🟡 Update operators are not checked for conflicting paths, except `$setOnInsert`

**Raised 2026-08-30, while adding `$setOnInsert` and the `$push` modifiers.**
MongoDB rejects any update in which two operators write the same path, or one
writes inside the other — `{$set: {a: 1}, $inc: {a: 1}}` fails with *"Updating
the path 'a' would create a conflict at 'a'"*. Here the operators apply in the
order their keys arrive on the wire and the last one wins: `{"$set": {"a": 1},
"$inc": {"a": 5}}` on `a: 0` leaves `6`, and `{"$inc": {"a": 5}, "$set": {"a":
1}}` leaves `1`. That is what the parser has done since the update language
existed and what its tests pin — but before the release that records ADR-120
it was not what a client saw. Every request body was decoded into a JSON map
that sorted its keys, so the operators ran in alphabetical order whatever the
body said — `$inc` before `$set` — and the two updates above both left `1`.
The boundary now keeps the order it is given (ADR-120). "The order written"
means the order the bytes arrive in: a client whose JSON encoder does not
preserve insertion order — a language whose maps are unordered, or a library
that sorts keys on output — gets whichever order its encoder produced, so a
caller who depends on the last operator winning should serialise the update
deliberately rather than trust a map.

**`$setOnInsert` is the exception, and it is checked.** An update that sets a
path on insert and also `$set`s, `$inc`s, `$unset`s or `$rename`s onto it (or a
prefix or extension of it) means one thing when it inserts and another when it
matches, and which of the two ran would depend on operator order in a document
whose key order is an accident of the client's JSON encoder. That case is
refused at parse time with the same shape of message as MongoDB's. The check is
confined to pairs involving `$setOnInsert` because extending it to every pair
would turn an update that works today into a `400` on upgrade — a behaviour
break, which a patch release does not carry.

**Closing it** is a one-line widening of `reject_set_on_insert_conflicts` to
all operator pairs, plus the tests that pin ordered application today, and it
belongs in a `0.MINOR` bump that says so in the changelog. Until then an update
that names one path twice is applied in order, not refused, and a client that
relies on MongoDB refusing it will not be told.

Two smaller choices from the same change, recorded here so they are visible
rather than because either is a debt:

- **`$push`'s `$sort` on elements that are not documents.** With a
  `{field: direction}` sort, an element that is not a document sorts as though
  every named field were missing — `null`, at the low end — rather than being
  an error. MongoDB does the same. A whole-element sort (`1` / `-1`) uses the
  canonical cross-type order, so mixed arrays sort by type group first.
- **A document argument to `$push` with any `$`-prefixed key is modifiers.**
  MongoDB decides by the *first* key. Deciding by *any* key means
  `{"$each": [1], "x": 2}` and `{"x": 2, "$each": [1]}` are both refused as an
  unrecognized clause, where MongoDB would push the second literally. Nothing
  that starts with `$` is a value anyone meant to store, so the stricter
  reading only ever turns a silent misfiling into an error.

---

## 🟡 `modified` counts documents written, not documents changed

**Raised 2026-08-21, found by sweeping the CLI against a running cluster.**
`update` reports `modified` for every document the write path put back, whether
or not the stored document ended up different. A `$set` to the value a field
already holds counts:

```
$ kimmy update dev.mod '{"g":"x"}' '{"$set":{"g":"x"}}' --multi
{"matched":2,"modified":2}
```

Not a miscount. `docs.rs` returns `WriteOutcome { matched: existed, modified:
existed, .. }`, so the field means *a document was there and we wrote over it* —
which is exactly what it reports. It is consistent across every path that
returns it: `$unset` of a field that was never present counts, and a `PUT`
replacing a document with a byte-identical body counts.

**MongoDB's `nModified` means the other thing.** It excludes documents whose
values were already what the update asked for, so the two answers diverge on
precisely the case a caller is most likely to be testing for — "did this change
anything?" A client ported from MongoDB gets a number that looks right, is
wrong, and never says so. That is what makes this worth an entry rather than a
footnote: silent disagreement on a field both systems spell the same way.

Nothing on the retry path depends on it. `modified` is reported, not consumed:
no retry class reads it, and the value cannot make a write happen twice.

**Closing it means comparing documents, not counting differently.** The write
path would have to hold the pre-image and compare it with the result to know
whether anything moved — a serialize-and-compare per document, on the write
path, to make a reported number more precise. Deferred rather than done because
the cost lands on every update in order to improve an answer that no part of the
system acts on.

**The workaround is to ask the question directly.** `matched` already says how
many documents the filter found, and a `count` with a filter describing the
desired state says how many are already that way — which is the honest way to
learn whether an update would change anything, in either database.

Also noted in [the HTTP API](http-api.md#count-update-delete-by-filter),
[Query language](query-language.md) and [`openapi.yaml`](openapi.yaml), which
are where someone writing an update will meet it — rather than in
[Compatibility](compatibility.md), which is about this project's own `/v1`
promise and not about MongoDB.

**`openapi.yaml` was the one that got out of step**, and it stated the opposite
until 2026-08-26: *"Matched but unchanged documents count in `matched` and not
here."* Found by someone writing a fourth client (.NET) from the specification,
which is the only document a client author is obliged to read — so the register
was right about the behaviour everywhere except in the contract. Same shape as
the `409` on a second collection create, below: a false sentence about an
*outcome* survives because the specification's coverage assertion checks that
every *operation* is exercised, not every documented outcome. The contract test
now pins `modified == matched` for a no-op `$set`, so the sentence and the
server cannot drift apart again in silence.

---

## 🟢 An authenticated node without a JWT secret signed tokens with a source-visible constant

**Was.** `Config::validate` required `auth.jwt_secret` only under
`cluster.enabled` (`crates/kimmyd/src/config.rs`). A **single** authenticated
node started without one was accepted, and `node::run` then fell back to a
literal signing key — `"insecure-no-auth-unused-signing-key"` — that ships in
the source. The comment above the fallback claimed it was reached only with auth
off; the validation did not hold that claim, and nothing tested the single-node
case.

**Why it mattered.** The fallback is a public constant, so anyone could forge a
`root` token (HS256 over `{sub, iat, exp, grants, tv}`) and skip login
entirely — and `docker run -p 7878:7878` binds `0.0.0.0`, so the node is
reachable from the LAN. The documented run paths all set `KIMMY_JWT_SECRET`,
which is the only reason this stayed hidden. **Confirmed on a real node before
the fix**: a hand-forged token returned `200` from `/v1/auth/whoami` as `root`
with `[{db:*, collection:*, actions:[admin]}]` and created a collection; both
`whoami` and the write became `401` once a real secret was set.

**Now.** The JWT-secret requirement moved out of the `cluster.enabled` block: an
auth-on node with no configured secret is **refused at startup**, single node or
not, with an error that names the hazard. The constant is reachable only through
`node::signing_key`, which returns the throwaway key for the auth-off path and
an **error** otherwise — a second enforcement point rather than an assertion,
because `debug_assert!` is compiled out of the `--release` build the Dockerfile
ships. The length rule moved to `Config::validate` too (sharing
`kimmy_auth::MIN_SECRET_LEN`), so `check-config` refuses what the server refuses
instead of blessing a configuration that dies after bootstrapping the superuser.
Covered by `auth_requires_a_jwt_secret` and
`auth_on_with_no_secret_refuses_rather_than_signing_with_the_constant`, and
listed in [Operations](operations.md#refused-at-startup).

**The first attempt at the second half was itself the register's recurring
mistake**, caught in review before merge: it was a `debug_assert`, with a comment
claiming it meant a future weakening of the validation "trips a test". It could
not — assertions are compiled out of release, and no test drove that path at all.
A claim about a mechanism, with no mechanism.

**Alternative considered:** generating a random secret when one is unset.
Rejected — it would invalidate every live token on restart and give each node of
a cluster a different key, turning one silent failure into two. Requiring the
operator to supply the key is the same shape as the existing root-password and
`cluster_secret` refusals.

**The lesson is the register's recurring one:** a claim in a comment that the
code does not enforce. The question that finds these is "what fails if this stops
being true?" — here, nothing did, which is why a forged-token drive was the check
that mattered.

## 🟢 The reachability check was sized on a fixture whose shape did not generalize

**Was.** PR #81 added a build-time check that a finished HNSW graph can find
its own data, which closed a real defect: about one build in 250 orphaned 10–24%
of a collection, silently. Its threshold — three sampled misses — was sized
against a **400-vector, 16-dimensional** fixture, and both of those turned out to
matter.

**The problem.** A missed probe is two events added together: a search that ran
out of budget, and a point the graph cannot reach. An ordinary search explores a
fixed amount, so the first grows with collection size and width while the second
does not. At 384 dimensions — the realistic embedding width — misses ran to a
median of 8 in a sample of 128 at 4,000 vectors, against a threshold of 3.

**Seven builds in ten at that size were discarded, rebuilt twice, and then
reported to the operator as losing data.** Three times the build cost, and a
`searches on this collection may be incomplete` warning, on a healthy graph.
It was found by `scripts/bench-baseline.py check` during unrelated work:
`hnsw_build/4000` at 3.00×, `hnsw_build/2000` at 2.03×, exactly
`MAX_BUILD_ATTEMPTS + 1`.

**Now.** The probe counts *unreachable* points rather than missed searches — a
miss is re-probed with the budget removed, and only a point still missing then
counts. Threshold 8, sized from both sides of the gap rather than one:
599 healthy builds scored 0–5 (median 1), and the one catastrophic build
observed scored 22. [ADR-061](decisions.md) has the reasoning and the
alternatives.

**A false explanation was corrected with it.** The old comment said a healthy
miss was a vector among near-duplicates being edged out — approximation working
as designed. Measured, that is wrong: with the budget removed those points are
still not found. **Every graph this builds orphans between 0.8% and 3.0% of the
collection**, and the check has always been separating routine orphaning from
catastrophic orphaning rather than noise from signal.

**The lesson is the one #81 recorded twice and this makes three:** a constant
sized without knowing the shape of what it bounds. The new one ships with the
measurement that produced it — two `#[ignore]`d tests that re-derive both
distributions — so the next person can check rather than trust.

## 🟡 Routine HNSW orphaning of up to 3% is accepted and not reported

**Raised 2026-08-15 by [ADR-061](decisions.md).** Measuring the reachability
threshold established that a healthy graph at these parameters leaves 0.8%–3.0%
of a collection unreachable from the entry point. Those documents are returned
by no approximate vector search, at any `k`. Exact search is unaffected, and
collections under the approximate-index threshold use it.

**Not a build defect and not fixable by rebuilding** — it is a property of
`MAX_CONNECTIONS` and `EF_CONSTRUCTION`, so every draw has it. Closing it means
changing those with recall measurements to hand, which trades build time and
memory for it.

**Recorded because it was previously believed to be zero**, and described in a
comment as approximation rather than loss. The number is now known rather than
waiting to be discovered.

---

## 🟢 HNSW implemented (was an open drift)

**Requested.** During planning the choice was *"HNSW from the start"*. It was
then deferred three times across sessions, each time to land something else
end to end — no single decision, which is exactly how this kind of drift
happens.

**Now built.** `HnswIndex` over `hnsw_rs`, with recall measured against the
exact path rather than assumed: the tests assert ≥ 90% recall at k=10 and that
the nearest neighbour agrees with an exact scan exactly.

**Two findings from building it:**

- The crate the roadmap originally named, `hnswlib-rs`, **requires nightly
  Rust** — its `corenn-kernels` dependency uses `#![feature(f16)]`. Naming it
  without checking was my error. `hnsw_rs` builds on stable.
- **The `Dot` metric has no approximate index.** `anndists::DistDot` computes
  `1 - dot` and *asserts the result is non-negative*, which only holds for
  unit-length vectors; a real embedding would abort the process. Dot-product
  collections use the exact scan. Normalizing on the way in would make it work
  but would silently change what a dot-product search means.

---

## 🟢 HNSW is wired into search (was an open drift)

**Was.** The index existed and was tested, but `vector_search` always ran the
exact scan — nothing chose the approximate path. Wiring it needed a
cache-and-invalidate policy that no component owned.

**Now.** `kimmy_vector::IndexCache` owns that decision, held in `AppState` so
one graph is shared across requests. `access()` returns `Approximate` or
`Exact`, and both search endpoints dispatch through it.

The policy, and why each part of it is what it is:

| Question | Answer | Because |
|---|---|---|
| When is a graph worth building? | ≥ 500 vectors | **Measured.** Originally 2000 on the assumption that a scan wins below some size; it never does. 500 is where one build repays itself in ~12 queries ([Benchmarks](benchmarks.md)) |
| How is staleness detected? | A per-collection generation counter, bumped on every vector write and delete | Counting is O(n); a count also cannot see a delete-then-add that leaves the total unchanged |
| When does a stale graph rebuild? | After 30s | Rebuilding per write would rebuild continuously under load, and each rebuild is O(n log n) |
| What happens if a build fails? | Fall back to the exact scan | An optimisation that cannot be built must not fail the query |

**Why bounded staleness is safe here, when a stale secondary index is not.**
The graph only supplies *candidates*. Scores are recomputed from the currently
stored vector, and a candidate whose record no longer exists is skipped. So a
deleted document cannot surface and an updated one scores by its new vector —
the only effect of staleness is that a document written in the last 30 seconds
may not be found yet. That is bounded recall loss on new data, never incorrect
data. `the_approximate_path_agrees_with_the_exact_one` asserts the two paths
return the same nearest neighbour with a byte-identical score.

**Still open:** no on-disk snapshot persistence — see below.

---

## 🟢 Vector indexes survive a restart (was a 🟡 deferral since M2)

**Was.** The cache was in-memory only: the first search of a large collection
after a restart paid a full O(n log n) build, served by the exact scan until
then. Slower, never wrong — which is why it waited from M2 to M8.

**Now.** Every successful build is persisted beside the database file
(`<data_dir>/hnsw/<collection-id>/`, staged and renamed so a crash mid-save
leaves the previous snapshot rather than a torn one), and a process's first
look at a collection loads the snapshot before paying the build. Validity
across a restart cannot use the generation counter — it is in-memory and
resets — so the check is the vector *count* the snapshot covered: equal
counts adopt it as fresh; unequal counts serve it once (bounded recall loss,
instantly) and rebuild on the next access. The corner accepted, on purpose: a
delete-and-add while the node was down leaves the count equal, and that
snapshot serves as fresh until the next vector write — the same class of
bound as the 30-second staleness window, on a longer clock. A restore does
not carry snapshots and rebuilds, which is always correct.

**Found on the way, by testing the failure path:** `hnsw_rs` *panics* on a
corrupt graph file — an `unwrap` on its magic check — so the load runs under
`catch_unwind`, and a torn file on disk costs a rebuild rather than the
process. The same species as `DistDot`'s assert in M2: the library's failure
behaviour was checked rather than trusted, and it needed containing.

---

## 🟢 `byo` now has an ingest route (was an open drift)

**Was.** `byo` — "the client supplies the vectors" — is the default provider,
and there was **no endpoint for supplying them**. Search on a `byo` collection
returned nothing, always. The only working path was writing raw records into
the shadow collection using the internal `VectorRecord` serde shape: the `#`
chunk separator in `_id`, the externally-tagged `DocId`, the internal HLC
shape. An implementation detail acting as a public contract.

**Now.** `PUT /v1/db/{db}/coll/{coll}/docs/{id}/vectors` taking
`[{chunk, vector, text}]`, with `GET` and `DELETE` alongside it. Replace-all
per document, matching what the embedding worker does — the only semantics that
stops a shortened document from leaving orphan chunks. The server supplies
`source` and `source_hlc` from the document it already holds, so no internal
shape crosses the boundary and staleness detection keeps working.

Requires `write` on the collection, and the document must exist: without it
there is no HLC for staleness to compare against.

**And the failure is no longer silent.** Searching a collection with no vectors
stored at all now returns `409 no_vectors` naming the remedy, rather than an
empty result that reads as "nothing matched". That was the more damaging half:
M3 exposed `vector_search` to agents, which would retry a query forever against
a collection that could never answer it.

**Shape chosen by the maintainer** rather than unilaterally, since it is
public API.

---

## 🟢 A malformed shadow document no longer breaks search (was a live bug)

**Was.** A shadow collection is an ordinary collection, so anyone with write
access could insert a document into one. Any document that did not decode as a
`VectorRecord` made `for_each_vector` fail — turning **every subsequent search
on that collection into a 500**. One insert could brick search.

**Now.** Undecodable documents are skipped and logged at `warn`. Search stays
available; the operator still sees the problem. Same principle as the index
skipping records that no longer exist.

**Found by** the same manual verification as the entry above.

---

## 🟢 Index ranges use both bounds on scalar-only indexes (was the register's last 🔴)

**Was.** A real bug, found by property testing: `{a: [2, 0]}` matches
`{$gte: 1, $lte: 1}` because *different array elements* satisfy each bound.
Intersecting both into one key range excluded the document. The fix used one
bound only — correct, but `{qty: {$gte: 5, $lte: 9}}` scanned the index from 5
upward rather than stopping at 9, and this register carried it as its only 🔴
from M1 to M7.

**Now.** Multikey-ness is tracked per index, as MongoDB does: a `multikey`
flag on the definition, set in the same transaction as the index entries the
first time any document contributes more than one key — by write, by backfill,
or by applying a peer's write. Scalar-only indexes get both bounds; multikey
ones keep the one-bound superset. One-way, because clearing it safely is a
full scan for the sake of a planner hint. [Indexes](indexes.md).

**Two things the closing surfaced, both fixed in the same change:**

- A plan that intersected both bounds is only sound for the snapshot whose
  flag approved it — so the scan **re-validates the flag in its own snapshot**
  and falls back to a collection scan if a write flipped it in between. A
  stale `false` proving nothing about the present is the same lesson the
  register's native-toolchain entry teaches about claims generally.
- **Index maintenance trusted the caller's index list**, so a write through a
  `CollectionMeta` handle fetched before an index existed silently skipped
  that index — no entries, no unique check. Found by this branch's own tests
  tripping over it. Maintenance now re-reads the definitions inside the
  write's transaction.

---

## 🟢 `$in` uses the index (was a 🟡 deferral)

**Was.** `$in` needs a *union* of point lookups rather than a single range,
so it fell back to a collection scan — the most common operator to do so.

**Now.** An `IndexPlan` carries a list of `(lower, upper)` ranges rather than
one: a single entry for a contiguous range, one per distinct value for `$in`.
The executor scans each and unions the candidates, deduplicating by document
key — one document can appear under two probes when an array holds two listed
values. Values are deduplicated on the *encoded* key, so `[5, 5.0]` probes
once, exactly as the index collapses them. Probes are equalities, so they are
sound on multikey indexes with no flag interaction, and an empty `$in` plans
an empty union — zero probes for a filter nothing can match. Visible in
`explain` as `"strategy": "indexUnion"` with a `"probes"` count.

---

## 🟢 Ranges on descending index fields are planned (was a 🟡 deferral)

**Was.** The planner fell back to the equality prefix. Inverted encoding swaps
which end each bound belongs to, and getting it backwards produces a range that
is too **narrow** — silently wrong. Deliberately slow rather than possibly
wrong, until the swap had its own tests.

**Now.** The swap is implemented: a descending component's inverted bytes
reverse order, so the value-space lower bound caps the key-space *top*. Held
down three ways — encoded-key assertions on the planner (the key for a value
inside the range falls inside the plan's bounds, outside falls outside),
equivalence and selectivity tests against a real engine including the compound
ascending-prefix-descending-range shape, and the equivalence property test,
which already generated descending indexes and two-sided ranges and began
exercising this path the moment the planner stopped refusing it. Multikey
descending indexes use one bound, exactly as ascending ones do — the multikey
rule is about the data, not the direction.

---

## 🟡 The "no C toolchain" property has not held since M2 — now enforced

**Claimed.** ADR-001 chose redb over RocksDB and ADR-016 chose `rust_crypto`
over `aws_lc_rs` to keep the build free of a native toolchain. This register
repeated that as a live property, and the `local-embeddings` gating below cites
it as a reason.

**Actually.** `kimmy-vector` depends on `reqwest` with `rustls-tls`,
**non-optionally**, for the remote embedding providers — so
`reqwest → hyper-rustls → rustls → ring` has been in every default build since
M2. `ring` ships C and assembly and builds with `cc`.

**How it was found.** Planning TLS, by running `cargo tree -i ring` instead of
trusting this document. Two milestones of a stated property being false is
exactly the drift this register exists to catch, and it did not catch it — a
claim was recorded once and never re-checked against the build.

**Decided.** Accept the cost and correct the record, rather than gating
`reqwest` behind a feature. The individual choices in ADR-001 and ADR-016 were
still right; only the claim about the whole build was wrong. Recorded 🔴 rather
than 🟢 because what closes it is not code — it is *checking*, and there is no
mechanism that would catch the next such drift.

**The rule that replaces it.** Do not add a *second* native crypto stack.
`ring` is already paid for; `aws-lc-rs` would add CMake for the same primitives.
That is what selected the TLS provider in [ADR-039](decisions.md).

**Closed as far as it can be.** `scripts/check-native-deps.sh` runs in CI and
fails when the default build gains a package matching a native-toolchain
indicator — `cc`, `cmake`, `bindgen`, `pkg-config`, `*-sys`, `*-src` — that is
not on `scripts/allowed-native-deps.txt`. The allowlist currently holds one
entry, `cc`, with the reason beside it.

Downgraded from 🔴 to 🟡 rather than 🟢 because the *claim* was wrong for two
milestones and nothing brought it back to true; what changed is that the next
such drift now fails a build instead of sitting in prose. Adding native code is
still allowed — it just has to be a deliberate line in a diff.

**Worth noting about the check itself:** its first version exited 1 for the
wrong reason. With `set -e`, an allowlist holding only comments made `grep`
return non-zero and killed the script *before it printed anything* — a passing
failure, which is precisely the class of bug this register exists to catch. It
was found by running the failure path rather than only the happy one.

---

## 🟡 Local embeddings are feature-gated, not the default

**Planned.** `fastembed` local ONNX as the zero-config default provider.

**Built.** Behind a `local-embeddings` cargo feature, off by default.

**Why.** Its dependencies pull native ONNX Runtime *and* OpenSSL, and roughly
triple the image. Raised and agreed before building.

**One of the original reasons no longer holds.** This was justified partly by
preserving a pure-Rust build, which the entry above shows was already untrue
when it was written. The decision still stands on the other reason: ONNX Runtime
is hundreds of megabytes of native binaries and a separate runtime, which is a
different order of cost from a crate that builds some C with `cc`. Downgraded
from 🟢 to 🟡 because the stated rationale was partly wrong, not the outcome.

**Consequence.** Out of the box, embedding needs a remote provider or
client-supplied vectors. `--features local-embeddings` restores it.

---

## 🟢 Unique indexes are single-node only by default

Uniqueness is a global invariant and provably not maintainable without
coordination. `local` enforcement is the default; `coordinated` is reserved and
refused until M4. Raised and agreed. See [ADR-020](decisions.md).

---

## 🟢 Bulk insert exists (was a deferral the register never wrote down)

**Was.** `POST /docs` took one document per request, so loading N documents
cost N durable commits. Carried as known debt since M1 and named in the M8
plan — but, found while closing it, **the register never held an entry for
it**. The handoff's debt table pointed at a 🟡 in this file that was not here.
Recorded now on the way out, because the gap in the register is the more
useful lesson: a debt tracked only in a document that gets rewritten each
branch is a debt that can quietly stop being tracked.

**Now.** `POST /v1/db/{db}/coll/{coll}/bulk` takes an array and writes it in
one transaction, capped at 1000 documents ([ADR-048](decisions.md)). A document
inside a batch of 1000 costs about 1/175th of one inserted alone — 291
documents/sec becoming ~51,300 — because the durable commit was very nearly the
whole cost, which is what M8 task 3's flat concurrent-writer curve predicted.

**Consequence.** Bulk insert is atomic and *says* it is atomic, which is a
stronger promise than `update` and `delete` make. That asymmetry is now a
documented part of the surface rather than an accident: see the row below.

---

## 🟢 A renewed certificate no longer needs a restart (was a 🟡 deferral since M5)

**Was.** TLS certificates were read once, before the socket was bound
([ADR-039](decisions.md)), so rotating one meant restarting the node.

**Now.** SIGHUP, or a change to either file noticed within 60 seconds, swaps
the certificate on a running node ([ADR-049](decisions.md)). Both triggers
exist because neither covers both deployments: there is no convenient way to
signal PID 1 of a Kubernetes pod, and a poll alone leaves an operator who has
just replaced a file waiting out the interval.

**Almost none of this was new machinery.** `axum-server` already held the
certificate behind a handle the acceptor reads per handshake and already
exposed a reload; what was missing was something to call it. The branch is
therefore about triggers, and the ADR is mostly about why there are two.

**The startup rule and the reload rule now differ, on purpose.** A bad
certificate at startup is still fatal — there is nothing to fall back to. A bad
certificate at reload is refused and the one already serving stays, because
there is. Verified on a live node: a truncated certificate and a
certificate-without-its-key each left the node serving and healthy, counted a
failure, and the following trigger completed the rotation.

**To close, still open:** a failed reload is visible as
`kimmy_tls_reloads_total{outcome="failed"}`, but a certificate **nobody ever
tried to rotate** reports nothing, because no reload was attempted. That wants
`kimmy_tls_cert_expiry_seconds` parsed from `notAfter` — the metric actually
worth alerting on. Left out here because it needs `x509-parser` as a new
runtime dependency, and a branch about triggers should not also be a branch
about dependencies. See the row below.

---

## 🟢 SRV discovery resolves (was a 🟡 deferral since M4)

**Was.** `dns-srv:` parsed and was tested, and then returned an error when
asked to resolve. The standard library resolves *names*, not record types, and
SRV's whole purpose is the port inside the record — so this needed a resolver
crate, and the open question was never difficulty but which dependency.

**Now.** `hickory-resolver`, with `default-features = false` and exactly
`system-config` + `tokio` ([ADR-050](decisions.md)). The feature list is the
decision: every transport beyond plain DNS — DoT, DoH, QUIC, h3 — and DNSSEC
each ship a `-ring` and an `-aws-lc-rs` flavour, and the aws-lc-rs ones would
add CMake for primitives `ring` already provides. `check-native-deps.sh` still
reports `cc` alone, and the default build still has no `aws-lc-rs` and no
`openssl` — checked, not asserted, since the last prose claim about this was
false for two milestones before anyone noticed.

**Verified on two live nodes on ports 7911 and 7922**, neither the 7900
default, seeded only by `dns-srv:` against a real DNS server. That arrangement
is the point: with both on the default port, an implementation that resolved
the names and ignored the ports would have looked identical.

**Found while building it:** the resolver reports an empty answer as an
**error**, and the discovery loop warns on every error once per tick. A name
with no SRV records yet is what a cluster looks like before its first node
registers — so the obvious mapping would have logged a warning every few
seconds forever while nothing was wrong. `NoRecordsFound` is translated to an
empty set, with a test on it.

---

## 🟢 Webhook ownership follows the node, not the address (was a 🟡 deferral since M6)

**Was.** Rendezvous hashing took the `SocketAddr` SWIM publishes, so
re-addressing a node reshuffled its subscriptions. The register accepted that
as "the same disruption as that node leaving and another joining."

**That justification was wrong, and measuring it is what showed it.** Over
100,000 subscriptions across three members: a node genuinely leaving moves
**25.0%**, while re-addressing one node moves **50.8%** — twice as much,
because it is a departure and an arrival at once. The old address's share
scatters, then the new address claims a fresh share from everyone. For a node
that never went anywhere, which in Kubernetes is what a rescheduled pod is.

**Now.** The node id travels inside the SWIM identity foca already gossips, and
ownership hashes that ([ADR-051](decisions.md)). The id lives in the database
file, so it survives restarts and moves with a restore. Driven live: a node
moved from port 8203 to 8303 with the same data directory, and **all twelve
subscriptions kept their owner** while the cluster reformed and replication
continued.

**Found while building it — the M8 plan's expectation was wrong.** The plan
said a mixed-version cluster "produces duplicates, which at-least-once
tolerates". foca encodes identities with postcard, which is not
self-describing, so the added field is a wire break: a new node **rejects** an
old identity outright. A mixed-version cluster does not double-deliver, it
fails to form membership. That makes this a stop-the-cluster upgrade, like
ADR-040's, and it is recorded in [Operations](operations.md) beside it.

---

## 🟢 Tokens can be revoked (was 🟡 "not planned")

**Was.** Deleting a user, disabling one, or narrowing a role did nothing until
the token expired — up to an hour. The register listed rotating
`KIMMY_JWT_SECRET` as the only remedy, which logs out the entire cluster.

**Now.** The user record carries a token version and the token carries the one
it was issued under; a mismatch is a 401 ([ADR-052](decisions.md)). Deleting or
disabling a user needs no version at all — the absent or disabled record is the
refusal. Changing a password or grants bumps it.

**The grants half is the one that mattered.** Grants ride inside the token, so
a permission that was *taken away* kept working for the rest of the token's
hour. That was the actual exposure, rather than the untidiness of a deleted
account lingering.

**Verification is still a pure function.** `TokenIssuer::verify` does no I/O:
the version check lives in the `Auth` extractor, which already holds state, and
reads through a per-node cache that an oplog consumer keeps honest. Reading the
user record per request would have put a storage read on a path that had none.

**Found while building it — the consumer alone fails quietly.** The integration
tests build a router without spawning background tasks, and happily kept
honouring revoked tokens until the admin routes were changed to evict
synchronously as well. Correctness that depends on remembering to spawn a task
is the same shape as the M6 webhook bug. Now a single node is correct with no
background task, and a missing consumer delays *cluster-wide* revocation rather
than disabling revocation.

**Measured on two live nodes:** the node taking the change refuses immediately,
the other within about two seconds.

---

## 🟢 M8 closed with a mutation pass over the whole milestone diff

**227 mutants, 47 escapes, 32 closed.** `cargo-mutants --in-diff` over
everything M8 changed, run per crate so each mutant scores against a fast
relevant suite. Seventeen new tests killed 31; one restructuring removed
another by construction. The full account, including why the remaining 15 are
left alive, is in [Testing](testing.md).

**The M7 lesson held again: escapes cluster in new callers, not new logic.**
`backfill_from_entry` — task 5's addition — inherited the retry classification
written for the streaming path, and nothing exercised it. Forced one way a
permanent failure retries forever and the whole scan stalls on one document;
forced the other, a transient blip silently drops one. The fake provider could
only fail *retryably*, so neither direction was reachable.

**Worst coverage was `kimmy-core`: 8 escapes out of 9.** All of them in the
provider config added by task 6. The dialects were verified against documented
shapes with fixtures over in `kimmy-vector`, and that left the type beside them
— wire tags, default key variables, validation for Cohere and Gemini — with no
tests at all. It now scores 9 of 9.

**Two escapes were closed by changing the code rather than by adding a test.**
`should_reload` was extracted out of the certificate-reload loop so the
decision could be tested at all, and the `!` at its call site was removed by
inverting the condition — a mutant that cannot be written is better than one a
test has to chase.

**Four were proven equivalent rather than chased**, per the standing rule.
`hlc > held` → `>=` in `lag_behind_ms` contributes `wall_ms − wall_ms` = 0 when
the two are equal, which cannot change a `max()` that already defaults to 0;
and the node-id tiebreak in `win_addr_conflict` is *deliberately* arbitrary, so
flipping its direction preserves the only property that matters — that every
node computes the same answer.

---

## 🟢 SWIM membership is authenticated (was an open drift, found by driving a cluster)

**Was.** Only the TCP replication handshake verified `cluster_secret`;
`membership.rs` never referenced it. A node holding a *different* secret joined
a real cluster's SWIM member set. Replication rejected it correctly and it
could read nothing — so it looked contained.

**It was not contained.** Webhook ownership is rendezvous-hashed over the live
member set, so the impostor became an ownership candidate, won roughly
`1/(N+1)` of subscriptions, and delivered none of them. Measured on a
three-node cluster with twelve subscriptions: **eight delivered, four delivered
nothing**, with every real node believing it had correctly stood down. The same
failure shape as the M8 task-1 bug, reached from the other side.

**The realistic trigger is a rolling `cluster_secret` rotation**, not an
attacker — which is what makes it likely rather than theoretical.

**Now.** Every membership datagram carries `HMAC-SHA256(cluster_secret,
payload)` as a 32-byte prefix, verified in constant time before foca sees it
([ADR-053](decisions.md)). Rejections are counted and logged at powers of two,
so a misconfigured peer says so once rather than every probe interval.

**Found by driving a five-node cluster, not by the suite.** Every automated
test builds its cluster with one shared secret, so nothing ever presented a
wrong one. There is now an integration test that does.

**A wire break, and the third of its kind** — nodes must be upgraded together,
like ADR-040 and ADR-051. Recorded in [Operations](operations.md).

**Not fixed here: the datagrams are still readable.** Authentication was the
missing property and the one the failure needed; encryption needs a key
schedule, nonces and replay handling, and would not have changed the outcome.
Membership topology remains visible to anyone on the path — see the row in
[Security](security.md).

---

## 🟢 The HTTP reference lists every route again

**Was.** `docs/http-api.md` presents a table titled **Endpoints** that reads as
complete. It was missing **six of twenty-eight routes**: `aggregate`, `vector`
(configure), `vector_search`, `hybrid_search`, and both `webhooks` routes. Each
was documented on its own page, so nothing looked wrong — a reader of the API
reference simply could not learn they existed.

**Found the same way**, by reaching for `/vector_search` while driving a
cluster and not finding it in the reference.

**Now.** All twenty-eight are listed, each linking to its own page for detail.
And the claim is **checked rather than asserted**:
`every_route_is_in_the_http_reference` parses the router's own source and fails
naming any route absent from the document — the lesson M8 spent twelve tasks
relearning.

**Corrected while there:** the same document's coverage table said node-to-node
replication was "still plaintext", twenty lines above a section explaining that
replication runs over TLS always. It had been wrong since ADR-040.

---

## 🟢 A converged cluster stops re-syncing, and the lag gauge stops lying

**Was.** The version vector advances only when an oplog entry is appended, but
`behind` and `lag_behind_ms` both read it as "what I have seen". Two things a
node processes correctly append nothing: a document that **loses
last-writer-wins**, and an entry the sender holds but never ships — a
`UniqueViolation`, which is logged locally so it reaches change streams but is
excluded from `entries_for_peer` by design ([ADR-029](decisions.md)).

Either leaves a permanent hole, and `behind` then re-requests everything from
that point on every round, forever.

**The universal trigger is the bootstrap user.** Every node creates its own
`__users` collection and inserts `root` locally. Each peer's `root` insert
loses last-writer-wins on arrival — so **every cluster ever run** has had a
permanent hole from the moment it formed.

**Measured on an idle, fully converged three-node cluster:** 60 merge rounds in
20 seconds with no user data at all, each re-fetching and re-applying the same
entries. The re-sent range grows with everything written after the hole, so the
cost grows with the database. After the fix: **zero**.

**The visible symptom was the gauge.** After concurrent updates to one
document, `kimmy_replication_lag_seconds` pinned at **1204 seconds** on all
five nodes of a converged idle cluster and stayed there — clearing only when a
winning write from each origin finally advanced past the discarded stamps.
Operators are told to alert on this metric and that zero is the caught-up
steady state, so it was both a permanent false alarm and a real signal nobody
could distinguish from one.

The gauge stayed silent in the universal case only because `lag_behind_ms`
ignores an origin it has never seen — the "fifty-year lie" rule. So the loop
ran everywhere while the metric read zero.

**Now.** A second durable vector records what has been **processed**, and
`behind`/`lag` read that; the oplog-derived vector keeps its meaning — what
this node can *serve* — and is still what a peer receives
([ADR-054](decisions.md)).

**Found by driving a cluster, and only after fixing my own test.** The earlier
evaluation reported this area clean because its conflict test used `PUT`
without `?upsert=true` and therefore wrote nothing at all. A test that asserts
five nodes agree can pass because they agree on an error.

**Corrected during the work:** the first draft of ADR-054 claimed replicated
DDL was one of the unlogged paths. It is not — `apply_ddl` appends the
originating entry deliberately, to advance the vector. Reading the code rather
than trusting the draft is what found the real universal trigger.

---

## 🟢 A collection can be paged without `skip` (M9 task 5)

**Two decisions settled before code.** The token is **opaque**, following the
`ResumeToken` convention next door — a client treats it as a blob, so the
encoding can change without breaking anyone. And a cursor pages in **`_id`
order only**, because that is the order both access paths already produce.

**The implementation is smaller than the feature**, which is the point. A
cursor is the encoded document key of the page's last row, and nothing else.
`keyenc` is order-preserving, so byte order *is* `_id` order — "the next page"
becomes a range bound the storage engine already knows how to take. No new
comparison logic, no sorting, no server state, and portability between nodes
falls out because the token is a pure function of the `_id`.

The honest numbers, paging 1,000,000 documents at 100 per page: `skip` visits
~5×10⁹ documents, a cursor ~10⁶.

**Arbitrary sort keys were deliberately excluded**, and the reason is worth
keeping. A token *can* bound the start of a sorted scan — but index candidates
come back in document-key order (`scan_range_in` ends with `out.sort()`), so a
sorted page still materialises and sorts whatever remains. That is roughly 2×
better than `skip` rather than a fix, and shipping it would have promised
efficient sorted paging while delivering a constant factor. Making sorted
paging genuinely cheap needs index-ordered scans, which would also make every
sorted `find` cheaper — it is the obvious next thing, and it is not in M9.

**A design fault was found by writing the test.** The first draft returned
`nextCursor` only when a cursor had been *sent*, so the test had to invent a
magic first-page constant to start a walk — something no client could have
discovered. `nextCursor` is now offered whenever the page filled and the query
is one a cursor can continue, so a client's first request needs no cursor and
no flag. The test that needed a magic constant was the design review.

---

## 🟢 Partial indexes exist, and sparse falls out of them (M9 task 4)

**Two decisions settled before code.** Partial only — a sparse index is
`{field: {$exists: true}}`, which is where MongoDB has been steering for years,
so there is one mechanism rather than two overlapping ones. And the partial
filter language is **deliberately bounded**: `$exists: true`, equality, the four
comparisons against a literal, conjunction. Nothing else.

**The bound is the safety property, not a shortcut.** A partial index may answer
only a query provably contained by its filter, and general implication between
filters is not decidable — so a general language forces a best-effort
containment check whose mistakes return a *subset* with nothing to indicate it.
That is the multikey failure again, and it is the one this codebase is least
able to notice. Restricting the language makes containment a decision. The
refusal lands at index creation, where an operator can act on it, rather than at
query time where the symptom would be a plan that quietly stopped applying.

**Two things were verified rather than assumed**, both of which would have been
silent bugs:

- **`bson::Document` round-trips losslessly through the collection metadata's
  JSON**, as canonical Extended JSON — checked with a date and an integer above
  2^53 before choosing to store the filter as a document. The standing rule is
  that a type crossing a format boundary needs a *chosen* representation, and
  `NodeId` has cost a replication outage by inheriting one.
- **A comparison never matches an absent field**, because `condition_matches`
  evaluates over resolved values that are empty when the path is missing. That
  is what makes `$gt`/`$lt` safe to treat as proving existence. The exception is
  **`{a: null}`, which matches an explicit null *and* a missing field** — so it
  contributes nothing to containment. Answering it from a presence-filtered
  index would drop exactly the documents it is meant to find.

**`impl Eq for Bson`** exists in `bson` despite the `f64`, which is what lets
`IndexMeta` keep its derives with a `Document` inside. Checked, because the
obvious assumption was the opposite.

---

## 🟢 `update` and `delete` use the planner (was a 🟡 raised one branch earlier)

**Raised while building `find_and_modify`, fixed here.** `exec::update` and
`exec::delete` collected their targets with `for_each_doc` — a straight
collection scan — so **an index never sped up a filtered update or delete**,
however selective the filter was. Both now go through `collect_matching`, the
same helper `find` uses, which brings the index plan, the `$in` union with its
per-document deduplication, and the multikey both-bounds re-check with them.

Measured on 200 documents, ten of which match:

| | Documents examined |
|---|---|
| Before | 200 |
| After, index applies | 10 |
| After, no index | 200 (unchanged, and it must be) |

**`explain` came with it**, on both routes, mirroring `find`. That is what
turns "an index applies here now" from a sentence into something a test
asserts and an operator can check — the standing lesson from ADR-016, where a
claim nothing verified stayed false for two milestones.

**"Mirroring `find`" was true of the output shape only, and not of the
behaviour — until [ADR-131](decisions.md).** `explain: true` on `update` and
`delete` still performed the write it described; `find`'s `explain` never
writes anything, because `find` never does. A `multi: true` `delete` with
`explain: true` deleted every document in the collection, which a 2026-09
test round found. ADR-131 closes the gap by having `update` and `delete`
run the same read-only scan `find` uses when `explain: true` is set, so the
sentence above is now true of the behaviour as well as the shape.

**A second, quieter bug went with it.** `update` counted `modified` by
incrementing per target rather than reading the write's own answer, so a
document deleted between the read pass and the write was still reported as
modified. It now counts `WriteOutcome::modified`. `delete` already did the
equivalent, which is why only one of the two was wrong.

**What deliberately did not change:** both still apply document by document and
can stop partway. That is the atomicity limitation recorded below, and it is a
separate question from which access path finds the targets.

---

## 🟢 `findAndModify` exists (M9 task 3)

**Three decisions settled before code**, and the second is the substance.

**One `/find_and_modify` route** carrying Mongo's flags, rather than three
`find_one_and_*` routes or an option on `/update`: every existing route is named
for a server operation, and `findAndModify` is the server command all three
driver methods map onto — which matters for the first-party clients M10 builds.

**The match happens inside the write transaction.** `update` collects targets in
a read pass and writes them one at a time, so two callers claiming the same job
both succeed. redb has a single writer, so a match found *inside* the write
cannot be taken between the match and the commit: atomic by construction, with
no retry loop and no way to report "nothing matched" while something did. The
cost is that the writer is held for the match; an unindexed filter over a large
collection blocks every other write for the length of the scan.

**Sort is supported**, so FIFO job queues work — which is most of why the
operation exists. It materialises every match inside the transaction to do it.

**`MAX_CANDIDATES` bounds the damage those two combine into.** Ten thousand,
matching `find`'s `MAX_LIMIT` and the ~8 ms scan it was chosen from — as a
**refusal**, not a truncation, because choosing from a prefix would return a
document the sort did not pick and no caller could tell.

**The crate boundary held.** `kimmy-query` is a dev-only dependency of
`kimmy-storage` on purpose, so filtering, ordering and update operators could
not move into the engine. They arrive as `ModifySpec` — pure functions over
documents that the engine calls inside its transaction — which is the same shape
as the guard `delete_guarded` took for TTL, one step further.

---

## 🟢 Documents can expire (M9 task 2)

**Three decisions were settled before any code**, and each is recorded where it
is enforced rather than only here.

**The trigger is a TTL index**, not a collection setting or a magic document
field, because the pass has to *find* expired documents every tick: a collection
scan is ~8 s per pass at ten million documents whether or not anything expired,
against ~1.66 µs per candidate through an index range scan. The index is both
the policy and the mechanism.

**One node expires a given collection**, by rendezvous hashing over live
members — the webhook ownership machinery, reused. Every node expiring
independently is convergent but produces N deletes per document, of which N-1
are superseded entries that still cost oplog, replication and change-stream
bandwidth. The cost is that expiry stalls for a collection whose owner is
partitioned; TTL is best-effort by nature and that was judged the better trade.

**An expiry is an ordinary `OpKind::Delete`.** A dedicated op kind would be a
**stop-the-cluster upgrade**: `op_kind_from_tag` (`codec.rs`) rejects an unknown
tag as *corruption*, so an old node would treat a new node's expiry as a corrupt
oplog rather than tolerating it — the same class as ADR-040 and ADR-051. The
audit value did not justify it, and MongoDB does not distinguish either.

**Two things came out of building it.**

**The obvious middle path on the third decision was a data-loss bug.** Marking
an expiry by putting a payload in the delete's body looks harmless, and
`apply_remote` branches on exactly that: a delete carrying a body is decoded as
a **live document**. Every "marked" expiry would have resurrected itself on
every node. Found by reading the apply path before offering the option, not
after taking it.

**The scan and the delete cannot share a transaction**, so a document refreshed
in between — a session heartbeat, which is the main reason to have a TTL at
all — would be deleted while live. `delete_guarded` re-reads inside the write
transaction and declines. The ordinary delete shares that body rather than
having a second one beside it, for the same reason `insert` and `insert_many`
share `insert_in_txn`.

**`kimmy_ttl_expired_total` was added** so "one document, one delete" is
measured rather than asserted: the cluster harness sums it across three nodes
and requires exactly 1. Correctness alone cannot distinguish the two designs,
because N deletes converge.

---

## 🟢 The pipeline can derive (was the largest gap against MongoDB)

**M9 task 1.** Expressions were a field path or a literal, so a pipeline could
filter, group and join but could not *compute*. `kimmy-query/src/expr.rs` is now
a recursive tree over the agreed operator list — arithmetic, strings,
conditionals, comparison, boolean and date parts — plugged into `$project`,
`$group` keys, accumulator arguments and the new `$addFields`/`$set` and
`$replaceRoot` stages.

**Three things came out of building it that were not in the plan.**

**A latent `$sum` bug, found by reading the code the decision touched.**
`finish()` carried an `all_int` flag and a comment citing ADR-002 about not
losing precision above 2^53 — while accumulating in `f64` and casting back, which
loses precision above 2^53. A sum of `2^53 + 1` and `1` returned
`9007199254740992`. Fixed by accumulating in `i64` and widening only on a double
operand or an overflow; both the unit test and a pipeline-level test pin it.
**The comment was right and the code was wrong, and nothing failed when they
disagreed** — the same shape as the "no C toolchain" claim (ADR-016).

**A deliberate behaviour change.** A document-valued expression now *computes*
its values, so `{$group: {_id: {c: "$city"}}}` groups by city. Previously that
was a constant document and **every input landed in one bucket** — a wrong answer
rather than a refusal. `$literal` is the escape hatch and exists because the rule
requires one: without it there is no way to produce the string `"$city"`.

**`$literal` was not on the agreed list** and was added anyway, because the list
is incomplete without it. Noted rather than slipped in.

**Deliberately excluded**, and recorded in the table below: arrays and sets,
variable binding (`$$ROOT`, `$map`, `$filter`, `$reduce`, `$let`), and type
conversion. `$$`-prefixed strings are **refused** rather than parsed as a field
named `$ROOT`, which would silently evaluate to null.

---

## 🟢 The client story — settled on 2026-08-12 by ADR-055

**Raised and postponed on 2026-08-11, decided on 2026-08-12.** The protocol is
the HTTP/JSON and WebSocket API KimmyDB already serves, promoted to a
specified and versioned contract with first-party clients for Rust, Python and
Go. The MongoDB wire protocol, gRPC and GraphQL are all rejected as
client-facing protocols. M10 is the milestone that carries it out.

**The framing in the original entry was wrong, and worth preserving as the
lesson.** It read "there is no wire protocol", which conceded a premise that
was never true — HTTP framing, Extended JSON v2 encoding, bearer-token
authentication, a typed error envelope and WebSocket streaming are a complete
protocol, and `kimmy-cli` is 464 lines of working client against it. What was
actually missing was that **nothing specified, versioned or tested it**. That
is a documentation gap wearing an architecture gap's clothes, and describing it
correctly is what made the decision small. Full reasoning, including why each
alternative was rejected, is in [ADR-055](decisions.md).

**Left open on purpose:** a narrow, read-mostly wire shim presenting as a
*standalone* server, purely so Compass and `mongosh` can inspect a database.
Not rejected — a smaller, separate decision, to be made after M10.

---

## 🟢 The protocol is specified, and the specification is checked (M10 task 1)

**Was.** The protocol existed and nothing wrote it down. `docs/http-api.md` said
so itself: "a *reference*, not yet a *specification* — nothing here is
versioned or machine-checked against the routes, so it can drift."

**Now.** `docs/openapi.yaml`, hand-written, with `crates/kimmy-api/tests/openapi.rs`
as the gate. The approach was the task's reserved decision and the maintainer
chose hand-written over generated; the reasoning, including why generation is
weaker *here* specifically, is [ADR-056](decisions.md).

**The test checks two different things**, because a specification can be wrong
two ways. Inventory — every registered route is described and every described
operation is registered — and behaviour: every documented operation is driven
against a real server and its response validated against the declared schema.
It ends by asserting every documented operation was exercised, so an entry
nothing executes cannot be added.

**Three findings, all from the behavioural half:**

- **`PUT /docs/{id}` returned booleans where every sibling route returns
  counts.** `{"matched": true, "modified": true}` against `/update`'s
  `{"matched": 3, "modified": 2}` — one field name carrying two types on one
  protocol, because the route serialized `WriteOutcome`'s three bools straight
  to the wire. Nothing had stated the type, so nothing disagreed, and the route
  had **no integration test at all**. `docs/handoff.md` described it as
  `{"matched": 0}`, which was wrong in a way nothing could contradict.
  Normalized to counts by decision — the cheapest possible moment, since no
  client exists yet and nothing in-tree reads the fields. `upserted` stays a
  boolean because it genuinely is one, and the route now has a test naming both
  behaviours.
- **`GET /v1/users` returns names, not user objects.** The specification's
  first draft said objects, written from the handler's shape rather than the
  store's return type. Caught on the first run.
- **The M8 inventory test had a hole.** `every_route_is_in_the_http_reference`
  matched `.route("` at the start of a line, which skips the three
  registrations rustfmt breaks across lines — `/docs/{id}`, `/docs/{id}/vectors`
  and `/vector`. It had been green while never checking the busiest route on
  the API. There is now one scanner, in the new test, and it checks the prose
  reference as well.

**The pattern is the milestone's own thesis, one task in:** every claim that
mattered and had no mechanism behind it was false. The two documents describing
this protocol both described it wrongly, and it took driving it to notice.

---

## 🟢 The error codes are a closed set, and say what to do about them (M10 task 2)

**Was.** Codes were `&'static str` literals written at call sites across five
modules. Nothing enumerated them, so both documents that tried to list them
were wrong, and nothing said what a client should *do* about any of them.

**Now.** `ErrorCode` is an enum; the wire string and the retry class both come
from exhaustive matches on it, so a new code does not compile until both are
answered. The specification carries the full set and the class of each, checked
against the enum by the contract test. [ADR-057](decisions.md) has the
reasoning, including why the retry class is three-valued.

**`no` / `wait` / `elsewhere`, not a boolean**, because every node accepts
writes and holds a full copy — so "ask a different node" is a real answer, and
the right one for `internal`, `misconfigured` and `snapshot`, which are
conditions of the node rather than of the request. A boolean would tell a
client that `internal` is retryable and have it retry the machine that just
failed.

**The class travels in the envelope**, so a client that acts on `retry` handles
a code released after it was written. That is what has to be true for "adding a
code" to be additive under task 3's compatibility policy.

**Two omissions found while enumerating**, both of the kind that only a list
compared against the code can find:

- **`no_vectors` was in neither document.** A 409 returned when searching a
  collection whose vectors were never ingested — deliberately a refusal rather
  than an empty result, because an empty result is indistinguishable from
  "nothing matched", which is how a `byo` collection can look like it works
  while returning nothing forever. It is in `vectors.rs`, and both lists had
  been written by reading `error.rs`.
- **422 was documented in the prose reference and specified nowhere.** A body
  that is valid JSON of the wrong shape is a different failure from a body that
  is not JSON, and seventeen operations can return it. All seventeen now
  declare it.

**And four more, every one of them found by driving a real node** rather than by
the suite — which had passed the whole time:

- **Sixteen routes were outside the taxonomy.** Axum's body rejection is bare
  text with no code, and the M5 mapping that fixes it is only reached by a
  handler taking `Result<Json<T>, JsonRejection>`. **One handler of nineteen
  did.** Every other typed body answered `422 text/plain`. The mapping now
  lives in an extractor — `json::JsonBody<T>` — which cannot be used without
  it. The conformance test had exercised a wrong-shaped body only against
  `/bulk`, the one route that was right.
- **`/watch` refused non-upgrade requests with no envelope at all**, for the
  same reason and with the same fix. It was the single refusal on the API a
  client could not branch on.
- **The specification said the OpenAI provider's tag was `openai`.** It is
  **`open_ai`** — `openai` is the display name from `ProviderConfig::name()`,
  which is what the spec was written from. `docs/vectors.md` had it right all
  along, and the project had already been caught by this exact distinction once
  before.
- **`no` is not a string in YAML 1.1.** `enum: [no, wait, elsewhere]` reads as
  `[False, "wait", "elsewhere"]` in PyYAML while Rust's reader gives the
  string, so **two readers of the specification disagreed about a value on the
  wire**. Quoted now, with the reason beside it. This is the whole argument for
  the live drive being a second, independently written reader: nothing inside
  the Rust test could have seen it.

---

## 🟢 `/v1` has a written promise, and a node says what it can do (M10 task 3)

**Was.** The path said `v1` and nothing said what that meant. No document
stated what could change under a client, and **nothing exposed a version over
HTTP at all** — the build version was in the startup log and in the MCP
handshake, so a client could not ask what it was talking to.

**Now.** [Compatibility](compatibility.md) is the policy: `/v1` does not break,
additive changes ship in it, anything breaking mints `/v2` served alongside for
a stated window. `GET /v1/version` reports the protocol, the build, the node
that answered, and a **capability list**. [ADR-058](decisions.md) has the
reasoning, including why date-versioned requests were rejected.

**Capabilities rather than a version number**, because nodes are upgraded one
at a time and a client that fails over between nodes can reach an older node
right after a newer one. A number answers "can I use this feature" only if the client also carries
a table mapping versions to features — the table this replaces, and the table
that goes stale in every client independently. `Capability` is an enum, checked
against the specification like the error codes.

**Four claims became mechanism**, which is what separates this from the last
time a compatibility property lived in prose:

- Every versioned route is under `/v1/`, and the prefix agrees with the
  server's reported protocol *and* the specification's `info.version`.
- The advertised capability set is exactly the documented one, and every
  capability has an explanation rather than only a name.
- **No response schema forbids unknown properties.** Without this, "a new
  response field is additive" is false for any client that validates — it would
  break on the next field added, silently, and only for them.
- The default build must *not* advertise `local-embeddings`, which is what
  proves the list is answered per build rather than asserted.

**Two things stay prose, and are marked as such**: the six-month `/v2` window,
which is a promise about calendar time, and "changing what a route means is
breaking", which nothing that reads shapes can detect.

---

## 🟢 A token can be refreshed without re-sending credentials (M10 task 4)

**Was.** `/v1/auth/login` was the only way to get a token, and tokens expire —
so a client library's only options were to hold the password and re-send it
every hour, or to hand every application an hourly outage.

**Now.** `POST /v1/auth/refresh` exchanges a valid token for a fresh one.
**Sliding re-issue, not a second credential**: nothing new to store, no second
lifetime to reason about, nothing kept server-side.
[ADR-059](decisions.md) has the reasoning, including why a stored, rotating
refresh token fits a leaderless store badly — rotation is a compare-and-set on
a replicated record, and two concurrent rotations resolve by last-writer-wins,
discarding a credential a client is holding.

**The security half is a property of the route's shape.** Refresh takes the
`Auth` extractor, so the token goes through the same ADR-052 check every route
applies and a revoked session is refused *before the handler runs*. That is the
difference between "cannot launder a revocation" and "does not, as long as
nobody deletes a line".

**Three things it deliberately does not do**, each written down rather than
discovered later:

- **It does not recall the old token**, which keeps working until it expires. A
  stateless token cannot be recalled; ending a session early is what the
  version bump is for.
- **It does not survive a grant change.** Grants live in the token, so changing
  them bumps the version and refresh is refused along with everything else —
  the cost of grants being carried rather than looked up.
- **It offers no grace for an expired token**, so `exp` means one thing on
  every route. A client idle past the lifetime logs in again, which is a thing
  a library may ask of an application where doing it hourly is not.

**Login and refresh both report `expiresIn`**, so a client schedules renewal
without decoding a token it is told to treat as opaque. An added response field
is additive under the policy written one task earlier, which is the first time
that promise was spent.

---

## 🟢 A client can discover the cluster (M10 task 5)

**Was.** A client was handed one address and could not learn the others. It
could not fail over, and the one capability a MongoDB driver's SDAM would have
given free was missing — while being *easier* here, because every node accepts
writes and there is no primary to find.

**Now.** `GET /v1/topology`. **Addresses** come from a replicated registry each
node writes itself into; **liveness** comes from SWIM. [ADR-060](decisions.md)
has the reasoning.

**Reading addresses from SWIM was not available.** `Member` carries the
*gossip* address and is postcard-encoded, so adding a client address to it is a
stop-the-cluster upgrade — the fourth in three milestones — and would put
client-facing configuration inside the cluster's internal wire. Deriving a
client address from a gossip address is a guess that breaks on separate
interfaces, TLS termination and container networking.

**Both known traps were inherited deliberately rather than met by accident:**

- `Members` holds **peers only**, so the answering node is added explicitly. A
  list derived from membership alone would tell a client the cluster does not
  include the node it is talking to — the shape of the bug that silently
  undelivered every clustered webhook.
- The member set must contain **only authenticated peers** (ADR-053), and that
  invariant now protects more than ownership: an unauthenticated peer in the
  set would be advertised to clients as a node to send credentials to.

**Verified on real nodes, not only in-process.** A cluster-harness test boots
three nodes and requires every one to list all three as `live` with its real
address — including a node whose seed list never named it, which only
replication can explain — then checks a token from one node works at every
advertised address, then kills a node and requires it to be reported `unknown`
rather than vanishing. The in-process tests prove the assembly; only the
harness proves the assembled thing, which is the distinction M8 task 1 was
built on.

**`status` is `live` or `unknown`, never `down`**, because a node whose gossip
is partitioned while its HTTP works is a real state here and hiding it removes
an option exactly when a client wants one.

---

## 🟡 A decommissioned node stays in the topology

**Raised 2026-08-13, with M10 task 5.** A node's registry record outlives the
node: a machine that is removed from the cluster keeps appearing in
`/v1/topology` as `unknown` forever.

**Not solvable by age.** Records are written at startup and only when their
content changes — deliberately, so an idle cluster does not append to the oplog
every tick — so a record's age measures uptime, and collecting old ones would
delete the longest-running healthy nodes first.

**The workaround needs no new surface**, which is why it is a 🟡 rather than a
task: the registry is an ordinary collection, so removing a decommissioned node
is deleting its document. An explicit route belongs with whatever operational
tooling comes after M10, not inside the client protocol.

**Documents that have to say this, kept in step (2026-08-27):** this entry,
ADR-060's "what is not solved", and `openapi.yaml`'s `Node.status` — which
until 2026-08-27 described `unknown` accurately but said nothing about it
being permanent for a decommissioned node, the one fact a client writing
failover logic needs. The sweep that found the `modified` drift flagged it;
it now says so.

---

## 🟢 Cursors are a protocol promise, and the promise is tested (M10 task 6)

**Was.** M9 built cursors and documented them well, but as *engine* behaviour.
The wire contract carried three claims nothing checked — node portability,
never-repeat-never-miss, and what a full page means — and the specification
said nothing at all about page size.

**Now.** The specification carries the contract a client may rely on, and the
three claims are checked.

**Node portability is verified on real nodes**, which is the one that mattered.
It was argued from the design — a token is a pure function of the `_id`, so any
node computes the same bound — and inherited from change-stream resume tokens,
which *had* been verified on a cluster. A cluster-harness test now walks a
collection changing node on every page and requires the walk to see every
document exactly once, in order. The protocol tells clients to fail over between
nodes ([ADR-060](decisions.md)); paging that broke when they did would be a data
bug reached by following the protocol's own advice.

**Two silent behaviours were specified nowhere**, and both are the kind a
client author discovers in production:

- **A `find` with no `limit` returns 100 documents, not all of them.** The
  prose reference said so; the machine-readable specification a client is
  generated from did not, so a generated client had no way to know. A caller
  treating an unlimited `find` as "the collection" processes a prefix and is
  told nothing. `count` has no cap, and is the honest source for a total.
- **A `limit` over 10,000 is clamped rather than refused.** The request
  succeeds and returns less than was asked for.

**And one that is a real trap:** a final page that is exactly full still
carries a token, because the server cannot know it is the last without looking
further, and looking further is work the caller did not ask for. A client must
end its walk on a short or empty page, not on a token no longer being offered.
Now stated in both documents and tested.

**One property is documented rather than enforced, deliberately.** A token
encodes a position, not a query: sending it with a different filter resumes
that filter after the same key, and the server does not check that the token
came from the query it is used with. Enforcing it would mean putting the query
in the token, which would make it large, node-specific in spirit, and a place
for a client to depend on structure it is told to treat as opaque.

---

## 🟢 Client-facing throughput is measured (M10 task 7)

**Was.** Every figure in [Benchmarks](benchmarks.md) was taken at the storage
engine, so JSON and Extended JSON conversion, per-request token verification,
HTTP framing, TLS and concurrent clients were all outside the published
numbers. There was **no honest answer to "what throughput can a client
expect"** — and that file's retracted-figure note is what happened the last
time one was quoted anyway.

**Now.** `cargo bench -p kimmyd --bench http` spawns the **shipped binary** and
drives it with concurrent clients over a real socket, plaintext and TLS, reads
and writes. Recorded rather than gated, like every benchmark here.

**Four findings**, all of which needed the socket to be visible:

- **TLS is close to free** — within noise at one client, about 10% at
  thirty-two. Whatever the reason to terminate TLS elsewhere, throughput is not
  it.
- **The protocol costs about 0.1 ms per request.** A point read, end to end
  through HTTP, token verification, storage and Extended JSON, has a p50 of
  0.09 ms. That is the number "what does the API cost" was missing.
- **Reads scale with clients (8,001/s → 70,660/s) and writes do not**
  (143/s → 602/s), with p99 rising from 10 ms to 246 ms. redb has one writer;
  concurrency queues rather than parallelizes. The engine benchmarks said this;
  the socket confirms a client experiences it as tail latency.
- **`count` is a collection scan** — 30 requests a second over 10,000
  documents, and it barely scales. A client polling a count asks the server to
  read everything, every time.

---

## 🟢 A write no longer costs twice as much through the daemon as at the engine (was a 🟡 "explained, not yet fixed")

**Raised 2026-08-14 by M10 task 7. Explained 2026-08-15 by M11 task 1. Fixed
by ADR-125, after the cluster form of it was measured.** A
single insert was **7.0 ms** over HTTP against **~3.4 ms** for the same insert
at the engine on the same machine, and the cause was recorded as unknown
because a cause that has not been measured is not a cause.

**The cause.** The daemon spends **two durable commits** on an insert where the
engine spends one. The second is the embedding worker's: `EmbeddingWorker::run`
records its oplog position after *every* entry, including entries it skips in
collections with no vector configuration, and `put_consumer_position` is its own
write transaction and its own fsync. redb has a single writer, so that commit is
a queue position in front of the next write — which is why the penalty was fixed
per request rather than per document: a request waits behind at most one
in-flight commit however many documents it carried.

**Measured, on a node, through `kimmy_commits`** — added for this and now on
`/metrics`: 200 inserts cost **2.00 commits each** as shipped and **1.00** with
the worker not started, at 156/s against 226/s. The HTTP benchmark's `insert
one` cell goes **54 → 236 req/s** (p50 11.00 → 4.05 ms). The webhook
dispatcher, the other named candidate, is **not** implicated: it wakes on a
two-second tick, and removing it changes nothing. The third candidate — fsync
on a runtime worker thread — cannot explain a gap present at one client, where
the runtime has nothing else to schedule; whether it costs anything under
concurrency is unmeasured.

**The larger finding is the amplification behind the batch API.** The client
waits behind one commit, but the worker commits once per *document*: 4,000
documents in 40 bulk requests took 0.39 s and left the node committing for
another **12.3 s**, 4,041 commits in total. Bulk exists so that 100 documents
cost one fsync instead of 100; the worker pays the 100 anyway, deferred. This
holds on every node whether or not any collection uses vectors.

**Was 🟡 because it was explained and not fixed.** The fix — coalescing a
consumer's position writes — trades a crash replaying a few idempotent entries
for an fsync per write, which changes the oplog-consumer contract and was
**reserved for a decision**. It was not to become "record only when there was
work to do": a position that advances only on work is killed by retention.

**Now.** The decision was forced by the cluster form of the same cost,
measured on a three-member cluster running 0.20.0: a 1,000-document bulk
converged on every member in 3–5 s and then every member, the writer
included, committed at a steady ~18/s for about 75 s — one checkpoint per
replicated entry, on top of the one-transaction sync batch of ADR-119. The
worker now holds its position beside its batches and writes it by deadline
(ADR-125): with a batch, at stream end, before a reconfiguration's backfill,
or after at most one second — and the deadline is checked per entry as well
as on the idle timer, so it holds during a drain, not only once the stream
goes quiet. A burst the worker has nothing to do with is one position write
however many entries it holds; a lone entry's position lands within about a
second; a steady trickle costs at most one checkpoint a second. A restart
re-processes up to a second of the stream, which idempotent embedding makes
a handful of reads. The measured numbers in [Benchmarks](benchmarks.md) are
left as taken, with a note.

**Pinned by tests rather than by this entry.**
`kimmy-storage`'s `one_insert_is_one_commit` still fails if the engine's own
cost changes, and `commits_are_counted_at_one_chokepoint` fails if a new
write path stops being counted. `kimmy-vector`'s
`a_burst_of_writes_the_worker_skips_costs_one_position_write_not_one_each`
fails if the worker goes back to one commit per entry;
`a_lone_skipped_entrys_position_is_recorded_within_the_position_wait` and
`a_held_position_is_checkpointed_during_a_long_drain_not_only_after_it` fail
if holding turns into never writing, on a quiet stream or during a backlog;
`a_burst_of_skipped_entries_does_not_delay_the_embeddable_document_behind_it`
fails if the held position ever delays an embed.

---

## 🟢 There is a Rust client, and the CLI is its first consumer (M10 task 8)

**Was.** KimmyDB had a specified protocol and nobody to speak it. `kimmy-cli`
hand-rolled every HTTP call — building URLs, reading statuses, deciding what an
error meant — which meant every one of those decisions existed once, in a tool,
where no other client could benefit from it being right.

**Now.** `kimmy-client`, and the CLI is a consumer of it: nothing in
`kimmy-cli` builds a URL or reads a status code any more.

**It depends on no `kimmy-*` crate, and a test keeps it that way.** That is the
property that makes it useful as a check rather than only as a convenience: it
sees exactly what the Python and Go clients will see, so it cannot quietly rely
on something the specification never promised.

**What it does, and each of them is a server promise from an earlier task:**
holds a token and refreshes it before expiry using `expiresIn` (task 4); fails
over between nodes discovered from `/v1/topology` (task 5); pages with cursors,
ending the walk on an empty page rather than a missing token (task 6); returns
typed errors carrying the retry class (task 2); and resumes change streams from
the last token seen, which is safe only because tokens are portable (M4 and
task 6).

**Retries are conservative on purpose.** A read moves to another node on
`elsewhere`; **a write does not**, because `elsewhere` means *this node* did not
answer, not that the work did not happen — and no status distinguishes an
insert that failed before the commit from one that failed after it. Callers who
know their request is idempotent say so.

**Three things the conversion found**, which is exactly why the roadmap made
the CLI the first consumer:

- **`Client::request` took a `reqwest::Method`**, so every consumer had to
  depend on `reqwest` to name a verb — the HTTP stack in the public API, where
  changing it would be a breaking change for everyone. The crate has its own
  `Method` now.
- **Login did not fail over.** It only tried the first endpoint, so a client
  handed a list whose first address was dead could not authenticate at all —
  the one failure that makes every other endpoint useless. Found by a test that
  put a dead address in front of a live one.
- **The CLI could not create a collection.** On a fresh database the first
  `insert` fails with "collection not found" and the tool offered nowhere to go
  but `curl`. It has `create-collection` now, and `watch` and `topology` while
  there.

---

## 🟢 There is a Python client (M10 task 9)

**Now.** `clients/python`, package `kimmydb`, synchronous, on `httpx` and
`websockets` — both chosen because each has a sync *and* an async API behind
nearly the same surface, so "sync first" costs nothing later. The stdlib was
considered and rejected for one measured reason: `urllib` opens a connection
per request, and against a ~0.1 ms request a handshake per call would dominate
everything the client does.

**It shares no code with the Rust client**, which is the point. Two independent
readers of one specification make a disagreement between them mean something.
The test suites are deliberately the same scenario list.

**The surface is Python, not a translation.** Iteration where a Python caller
expects it, `documents()` for the shape most people actually want, exceptions
rather than returned errors, and `.code` as a plain string rather than an enum
— because codes are additive and an enum would make an unfamiliar one an error
in itself.

**One thing had to fight the idiom.** A change stream connects when it is
asked for, not on the first read. Python's natural shape is a lazy generator,
which would open the socket at the first `next()` — so anything written between
`watch()` and that read would be missed, silently. Found by a test that wrote a
document immediately after opening a stream and then waited for it forever.

**CI runs it** against a real `kimmyd`, like every other client test here: a
mocked server would only assert what the client already believes.

---

## 🟢 A dropped collection now ends the streams watching it (was a 🟡, one day old)

**Was.** A change stream whose collection was dropped received no event, no
close and no error: it waited for changes to something that no longer existed.
And because ids are derived from `(database, name)` ([ADR-031](decisions.md)),
a collection recreated under the same name has the *same id* — so the stream
would silently resume delivering for it, bridging two different collections
with nothing in between. The stall was the visible half; the bridge was the
dangerous one.

**Now.** `InvalidateReason::CollectionDropped`, decided in storage rather than
at the HTTP edge, because storage is where the other two reasons live and where
`finished` is set — an in-process consumer of `Engine::watch` would otherwise
keep a stream it believes is live. **Scoped deliberately**: only a stream
watching *that* collection ends. A `Cluster` or `Database` stream keeps going,
and a test says so, because ending those would take the embedding worker down
with the first dropped collection anywhere.

**Fixing it exposed a second defect that had been invisible.** A replicated
schema change was appended to the receiving node's oplog but **never
published**, while a replicated *document* was — so a drop ended its own
node's watchers immediately and left every other node's waiting for an
unrelated write to wake them. Change streams filter schema entries out, so
"delivered late" and "not delivered" had looked the same for as long as the
asymmetry existed. It stopped looking the same the moment a drop became
something a stream cares about.

**The cluster harness is what found it**, and nothing else could have: a single
node applies its own drop directly, so every in-process test passed. This is
the third time that harness has caught a cluster behaviour that every
transport-free test agreed was fine.

**One representation fixed while there.** The invalidate reason was rendered
onto the wire with `{:?}` — a `Debug` derive, so renaming a variant would have
silently renamed a value clients branch on. It has an `as_str` now, with the
two existing names kept exactly as `Debug` produced them, so nothing already on
the wire changed. That is the same invariant `NodeId` and `CollectionId` each
cost a replication outage to learn.

**Both clients assert the new behaviour**, and the specification documents all
three reasons with what a client should do about each.

**And a third defect, older than both.** Because ids are derived from
`(database, name)`, a collection recreated under the same name reuses its id —
so the oplog still held the dead incarnation's entries and streams still
matched them. `from_start` on a healthy recreated collection replayed a dead
collection's documents and then invalidated immediately, never showing the live
data. A stream now never reads across a drop, and a resume token from before
one is **refused** rather than moved forward silently: the gap between that
token and this collection's first event is exactly what a client must not be
left unaware of.

**All three were found by asking a running node rather than reading the code.**
The first came from a client test that hung; the second from the cluster
harness; the third from a probe written to check a claim in a pull request
description, which turned out to describe behaviour the server did not have.

---

## 🟢 There is a Go client (M10 task 10)

**Now.** `clients/go`, package `kimmydb`, import path
`github.com/titusai-io/kimmydb/clients/go/kimmydb`. **One dependency**, and it
is the WebSocket framing: Go's `net/http` pools connections, so the argument
that ruled out Python's standard library does not apply.

**`coder/websocket` rather than `gorilla/websocket`**, for a reason specific to
this design rather than a preference: it handshakes through an ordinary
`*http.Client`, so a change stream inherits the same client, TLS configuration,
proxy and timeouts as every other request. `gorilla` dials with its own
`Dialer`, which is two configurations that can drift — the split this project
has been bitten by before.

**Idioms, not a translation.** Paging and streaming are range-over-function
iterators, so the error is the second loop variable rather than something to
remember to check; everything takes a `context.Context`, including the change
stream, and cancelling it is how a caller stops watching. `Code` is a plain
string rather than a constant set, for the same reason it is in Python — codes
are additive, and an unfamiliar one must not be an error in itself.

**Three clients now pass the same scenario list**, written three times against
one specification, sharing no code. That is the arrangement task 11 was waiting
for: the conformance suite becomes a matter of running one set of scenarios
three ways rather than inventing them.

**It found nothing new**, which the roadmap predicted: Go was placed third
because it is the least likely to surface a protocol gap the other two missed.
That prediction holding is itself worth recording — the specification and the
two earlier clients had already taken the surprises.

---

## 🟢 Three clients are held to one set of scenarios (M10 task 11)

**Was.** Three clients passed matching scenarios because they were *written* to
match. Nothing enforced it. The drift would have been found by a user.

**Now.** `clients/conformance`: one declared scenario list with the
observations a correct client must produce, a small driver per client, and a
runner that starts a fresh node per scenario and compares. Sixteen scenarios,
three clients, forty-eight runs.

**Two checks, not one.** Coverage — every driver must implement every declared
scenario — and behaviour, which is the half a per-language suite cannot do:
three suites can each have a `failover` test and disagree about what failover
means.

**A driver reports; it never judges.** Three clients that each decided whether
they had passed would be three opinions rather than one oracle and three
answers.

**Verified by breaking a client on purpose.** The Python driver made to stop
one page early produced `documents_seen: expected 250, observed 200` while the
other two passed. A suite that has never gone red is a suite nobody has tested.

---

## 🟢 Collection creation is not idempotent, and the specification said it was

**Found by the conformance suite's first full run**, indirectly: the runner
reused its work directory, so a second run started a node on the first run's
data and every client failed to create a collection that already existed. The
runner bug was real and is fixed — but the error message was the finding.

**`POST /v1/db/{db}/collections` returns `409 conflict` when the collection
exists.** It always has: `create_collection_inner` returns `CollectionExists`.
`docs/openapi.yaml` had said "Created, or already present — creation is
idempotent" since M10 task 1, and both the Python and Go clients repeated the
claim in a comment.

**Nothing caught it because nothing ever created a collection twice.** Every
test in the repository — the contract test, three client suites, every drive
script — creates each collection exactly once. The specification's coverage
assertion checks that every *operation* is exercised, not every documented
*outcome*, so a false sentence about a second call sat unchallenged.

**Now**: the specification says what happens, both clients say it correctly,
the contract test creates a collection twice, and it is a conformance scenario.
Four places, because the claim was in four places.

---

## 🟢 One application, written three times, and run (M10 task 12)

**Now.** `examples/shelf` — a small library catalogue in Rust, Python and Go.
It uses what KimmyDB exists for rather than what every database has: bulk
insert in one commit, cursor paging, an aggregation, semantic search over
client-supplied vectors, and a change stream watching the collection while it
is written to.

**They run in CI**, against a real node, all three in sequence — so the second
and third exercise the "already stocked" path and re-runnability is checked
rather than assumed. An example nobody runs rots into a document that used to
be true.

**The embedding is a toy and says so**: a deterministic bag-of-words hash, the
same algorithm in all three languages, so the pipeline is real without an API
key or a model download. All three produce identical scores, which is a
pleasant way to notice that nothing language-specific leaks into the protocol.

**Why an application rather than three snippets.** The deferred decision that
became M10 named *running KimmyDB on something real* as the trigger for judging
whether the client direction was right. A snippet that inserts one document
does not test that; something that has to find, page and react does.

---

## 🟢 The M10 server-side mutation pass is done (was a 🟡)

**Narrowed on 2026-08-14.** M10 task 12 called for a mutation pass over the
milestone diff and covered `kimmy-client` only, leaving 90 mutants in
`kimmy-api`, `kimmy-storage` and `kimmy-auth` unclassified. The original run was
misconfigured — every mutant re-ran all three crates' suites at a parallelism
that pushed each past the timeout, producing 3 caught and 15 timeouts out of 47
before being abandoned.

The scope splits **76 / 12 / 2**, which sums to exactly the 90 recorded.

| Crate | | |
|---|---|---|
| `kimmy-auth` | ✅ 2 caught | Both first read as misses and **both were scoping artefacts** — see below |
| `kimmy-storage` | ✅ 10 caught, 2 unviable | One real gap: `InvalidateReason::as_str` |
| `kimmy-api` | ✅ 76 tested | 36 caught, 32 unviable, **8 missed** on the first clean run; 6 were real and are fixed, 2 are artefacts |

**`kimmy-api`'s eight misses sorted into three different categories**, which is
the result worth keeping — "missed" is not one thing.

**Category 1, a genuine gap: `capabilities()` could return `vec![]`.** The
contract test compared the wire against *the same function that produced it*,
then asserted `!served.contains("local-embeddings")` — and both hold for an
empty list. A node advertising **no capabilities at all** passed every check.
Since [ADR-058](decisions.md) makes capabilities the thing clients branch on
*instead of* a version number, a node that silently claims to support nothing
is the exact failure the mechanism exists to prevent. The unconditionally
present capabilities are now named and required, and the fixture is asserted
non-empty so the test cannot go vacuous again.

**Category 2, covered only by an `#[ignore]`d test: the four `topology.rs`
mutants.** The cluster harness does catch them — three real nodes, `count == 3`,
exactly one `self` — but harness tests are `#[ignore]`, so they are invisible
to `cargo test --workspace` *and* to every mutation pass. **A mutation run sees
only the tests that run by default**, which means anything whose only coverage
is the harness reads as uncovered forever.

**And a real gap was hiding inside that category.** `register`'s docstring
claims it is *"idempotent, and silent when nothing changed — a node that
restarts twice an hour should not append to a log every other node then
replicates."* **Nothing tested it, and the harness structurally cannot**: it
starts each node once and never restarts one on an unchanged address. The
registry is replicated, so a spurious rewrite is an oplog entry every peer then
carries. Two in-process tests now cover it — `register` is entirely local, and
only the *replication* half ever needed real nodes.

**Category 3, a cross-crate artefact: the two `render` mutants.** `render` is
never called in `kimmy-api`'s own suite, because the contract test checks the
`101` handshake and never reads a frame. It is driven by the Rust, Python and
Go client suites and by the conformance runner. Confirmed by re-running with
`-p kimmy-client` in scope, where both die. Classified, not chased.

**The verification was run twice on purpose.** The first widened-scope run was
confounded — the test files were edited while it was in flight, so it could not
separate "the wider scope caught it" from "the new tests caught it". The clean
re-run at the natural `-p kimmy-api` scope reports **25 caught, 5 unviable, 2
missed**, which is what establishes that the six are closed where it matters
rather than only under a scope nobody uses.

**The `kimmy-storage` finding is the one that mattered.**
`InvalidateReason::as_str` could return `""` or `"xyzzy"` with nothing failing.
That method exists *precisely* so a variant rename cannot silently rename a
value clients branch on — and the strings were pinned only downstream, in the
three client suites, the cluster harness and the conformance scenarios, and
only for `CollectionDropped`. `ConsumerLagged` and `ResumeTokenExpired` were
held by prose in `docs/openapi.yaml` and by nothing that fails. They are pinned
now in the crate that chooses them.

**And a methodological finding worth more than either.** Both `kimmy-auth`
misses were caught once `-p kimmy-api` joined the test scope: `ttl_secs` is
asserted in the consumer's suite, not the owner's. **Per-crate scoping — M10's
own documented lesson, and the thing that makes these runs twenty minutes
instead of nine hours — systematically hides cross-crate killers.** A miss in a
crate whose surface is consumed by another may only mean the test lives one
crate up. Verify with a widened scope before believing a gap is real. A local
test was still added, because a crate's public accessor should not depend on a
consumer to pin it.

**Contention had to be learned twice.** Running the `kimmy-api` pass beside an
ordinary `cargo test --workspace` stretched that suite from ~2 minutes past 10.
It does not only ruin the mutation run — it ruins whatever shares the machine.
On an idle box the same 76 mutants finished in 31 minutes with **zero
timeouts**, against the original run's 15 timeouts out of 47. Run these alone.

---

## 🟢 The client's `retry: wait` path is tested — and was broken (was a 🟡)

**Closed on 2026-08-14, and the untested branch turned out to be wrong.** The
🟡 recorded a missing test. Writing it found that `Client::send` did not do
what the comment directly above it said, which is the failure mode this project
keeps meeting: a claim with nothing that fails when it stops being true.

The comment read *"The same node, after the delay it named."* The code slept
and then `break`, and that `break` left the inner `loop` — advancing the outer
`for endpoint`. So `wait` moved to the **next node**, exactly like `elsewhere`
and only slower; with a single endpoint it slept and then returned the error,
so the wait bought a delayed failure and nothing else. That is precisely the
outcome the three-valued taxonomy (ADR-057) exists to prevent: `wait` means
*this* node will serve the request shortly, so failing over abandons the one
node that said how long to wait.

`wait` now retries the same endpoint, **bounded to one wait per endpoint**, and
a node that refuses twice is left for the next one.

**Four tests, and the second endpoint is what makes three of them tests at
all.** With one node, "gave up" and "failed over and found nowhere to go"
produce the same error, so the mutation pass could rewrite the guards either
way unnoticed. Adding a second stub separated them — including the sharpest
case, an **unsafe write** that must not be repeated to a peer that might commit
it a second time. All nine mutants in the changed region are now caught, from
zero.

The harness is `Stalling` in `crates/kimmy-client/tests/client.rs`: a stub that
answers `429` a fixed number of times and counts its hits. It is not
`kimmy_api::router` because a real node's rate limiter bounds *login*, so no
arrangement of one makes an ordinary request answer `rate_limited` on demand —
which is also why this path has no live drive, and the conformance suite's 48
runs stand as the regression instead.

The rest of the residue is classified in [Testing](testing.md): reconnect
backoff arithmetic that needs fault injection to reach, and a handful of
provably equivalent mutants.

---

## 🟡 Sharding is deferred until there is experience

**Raised and explicitly postponed on 2026-08-11**, after the surface was
compared against MongoDB's. Not dropped: a decision the maintainer wants to
make from operational experience rather than from a comparison table. **A
future session should not re-open it unasked.**

Every node holds a full copy, so capacity is bounded by one machine and write
throughput by one redb writer — ~300 documents/sec single-write, ~51,300/sec
batched ([Benchmarks](benchmarks.md)).
**Replicated-not-partitioned is the current position and is considered
correct for now.** Partitioning would be a milestone-sized architectural change
and arguably against the leaderless simplicity that makes the rest of the
design work.

**To close.** Run KimmyDB on something real for a while, then decide. Recorded
here so that the absence of a decision is visible as a decision.

---

## 🟡 Not yet implemented, and known

| Gap | Consequence | Milestone |
|---|---|---|
| Client certificates (mTLS) | Server TLS authenticates the *server* to clients; clients still authenticate with a bearer token only | not planned |
| No certificate expiry metric | A failed reload is counted, but a certificate nobody ever tried to rotate reports nothing. `kimmy_tls_cert_expiry_seconds` needs `x509-parser` as a new runtime dependency (pure Rust — not a second crypto stack, so `check-native-deps.sh` would still pass) | not scheduled |
| Rate limiting beyond login | Only `/v1/auth/login` is limited. Every other route is unbounded — see the entry below | M5 |
| Per-session revocation | Revocation is per user — all of that user's tokens or none. Killing one session while leaving another needs a per-token deny-list, which fails open when an entry has not reached the node handling the request | not planned |
| `$vectorSearch` as a pipeline stage | The pipeline is built, but vector search stays its own endpoint | M5 |
| Set expression operators (`$setUnion` and family), `$zip` and `$objectToArray` | Deliberately outside M9 task 1's agreed operator list. The array operators, variable binding and type conversion it also excluded have all since shipped — the first two on an evaluation scope (ADR-105), the third as `$convert` and the `$toX` shorthands; the set family is a further pass over the same scope | not scheduled |
| Multi-document atomicity | Uneven, on purpose. **Bulk insert is atomic** — one transaction, all or nothing ([ADR-048](decisions.md)). `update` and `delete` still apply document by document and can stop partway, because each match is committed on its own. Note this is about *commits*, not about how the matches are found — that is planned now | by design |
| Benchmarks | The vector index, the write path, batched writes, concurrent writers and the planner are measured ([Benchmarks](benchmarks.md)), against a recorded baseline that is advisory rather than gating | M8 |
| No published protocol specification | The HTTP/WebSocket API is the client contract ([ADR-055](decisions.md)) but nothing specifies or versions it, so every client is hand-written and nothing fails when a route drifts | **M10 task 1** |
| No token refresh | `/v1/auth/login` is the only way to obtain a token and it expires. A long-running client has no option but to re-send credentials, which no driver should ask of an application | **M10 task 4** |
| Clients cannot discover the cluster | SRV discovery serves *nodes finding each other*. A client is given one address and has no way to learn the others or fail over — the one thing a MongoDB driver's SDAM would have provided free | **M10 task 5** |
| Client-facing throughput is unmeasured | Every figure in [Benchmarks](benchmarks.md) is taken at the storage engine in a release build. Nothing measures the socket: JSON and Extended JSON conversion, token verification per request, TLS, and concurrent HTTP clients are all outside the numbers. The single end-to-end figure (500 documents, 0.16 s bulk against 11.6 s individually) came from a **debug** node and is a ratio, not a rate | **M10 task 7** |

---

## 🟢 `find` by `_id` uses the primary key (was a 🟡 since M8)

Found while measuring latency-histogram buckets (ADR-046): `{_id: 500}`
through `find` was a full collection scan — ~7 ms over 10k documents — because
the planner consults *secondary* indexes only. The primary key is not in its
candidate set. `GET /v1/db/{db}/coll/{coll}/docs/{id}` is the O(1) path and
runs p50 ≈ 250 µs on the same data.

**Consequence.** Correct but slow, the register's favourite shape. Any client
that filters on `_id` through `find` — including via the MCP `find` tool,
where an agent has no reason to know a second route is the fast one — paid a
scan.

**Closed on 2026-08-14.** `plan::choose_primary_key` resolves an `_id` equality
or `$in` straight into document keys, which is the same candidate shape an
index scan produces — so the executor's existing "re-apply the full filter to
every candidate" rule carries over untouched. Measured on a release build, 10k
documents, before and after on the same data:

| | `find({_id})` p50 | examined | strategy | unindexed scan (control) |
|---|---|---|---|---|
| before | 7.328 ms | 10,000 | `collectionScan` | 7.242 ms |
| after | **0.540 ms** | **1** | `idLookup` | 7.065 ms |

**13.6× faster, and the control is unchanged** — which is what says the gain is
this path rather than a warmer machine. The remaining 1.77× against
`GET /docs/{id}` (0.306 ms) is real work `find` still does: parse a filter,
plan, re-apply the filter, wrap the result.

**`update`, `delete` and `count` inherit it**, because M9's #60 made all three
go through `collect_matching`. A targeted write on `_id` stops costing a scan
too, and a test asserts it rather than leaving it implied.

**The trap, and why the conversion is not decoration.** A stored document key
is `keyenc::encode(DocId::try_from_bson(id).to_bson())`, and that conversion
**normalizes `Int32` into `Int64`**. Encoding the filter's raw `Bson` instead
would build a different key for `{_id: 5}` depending on how the JSON parsed,
match nothing, and report a stored document as absent — a candidate set that is
*too narrow*, the one error the planner's safety rule forbids. Routing every
value through the same function the write path uses makes probe and stored key
agree by construction.

**And why a value it refuses falls back rather than being dropped.**
`try_from_bson` rejects `Double`, `Decimal128` and null. That is not merely
unhelpful: filter equality is cross-type *within* the numeric group, so
`{_id: 5.0}` genuinely matches a document stored under `Int64(5)`. Probing only
the values that normalize would lose it silently, so a single unusable value
abandons the fast path for the whole filter. There is a test that inserts under
an integer key, queries with a double, and requires both `collectionScan` *and*
the document.

**Deliberately not included:** ranges on `_id` (`$gt`, `$lt`). The documents
table is ordered by encoded key and `keyenc` is order-preserving, so a range
scan is available and would be a real further win — but it is a different shape
of work from a set of probes, and saying so is cheaper than a wrong answer. An
`_id` inside an `$or` is not a lookup either, for the reason `extract` already
encodes: a disjunction constrains nothing that must universally hold.

**Built.** A token bucket per key, on `/v1/auth/login`, keyed on the client
address. Closes the 🔴 that made a password guessable at network speed.

**Deliberately not more than that.** On login a limit is a *security* control:
the route is unauthenticated by necessity, and every attempt runs a full
Argon2id verification — including for a user that does not exist, since
equalising that is what stops timing from revealing whether one does. At the
configured work factor an unthrottled endpoint hands an anonymous caller ~19 MB
and milliseconds of CPU per request.

Everywhere else a limit would be a *capacity* control, and a capacity number
picked without a measurement is a guess of exactly the kind M5's benchmarks
exist to remove. Agreed with the maintainer: build the mechanism
route-agnostic, apply it
where it is a security property, and let the benchmark work decide the rest.

**To close.** `kimmy_api::Limiter` takes an arbitrary key and knows nothing
about login, so another route is a field on `RateLimits`, a config knob, and a
`check_at` call — in the handler when the key depends on the body, or in a
`tower` layer when it is just the caller.

---

## 🟡 Per-username login limiting is off by default

`login_per_user` is implemented and defaults to `0`, which disables it.

**Why it exists.** It is the only defence against a brute force spread across
many source addresses, which per-address limiting cannot see.

**Why it is off.** It introduces a lockout: anyone who can reach the endpoint
can spend a *named* user's budget and keep the legitimate holder out for the
rest of the window. Enabling it trades a remote-guessing risk for a
denial-of-service one, and which of those matters more is a property of a
deployment, not something a default should assume. Turning it on is one config
value, and the behaviour is tested either way.

---

## 🟡 A shared egress address shares a login budget

Per-address limiting keys on the peer address, so callers behind one NAT or one
egress gateway draw on one budget. An address that is over its budget is refused
**even with correct credentials** — the check has to precede authentication or
it would not prevent the Argon2 work it exists to prevent.

**Consequence.** On a shared egress, one client guessing passwords can lock out
its neighbours for the rest of the window. Raise `login_per_ip`, or set
`trusted_proxy_header` so the limiter sees the real client.

**Not closed by** trusting a forwarded header by default — that is client-
supplied data, so trusting it unasked would let anyone defeat the limit by
varying a header, which is worse than having no limiter, because it would look
like one was working.

---

## 🟢 An HNSW build could orphan a tenth of a collection (was a 🟡 "flaky test")

**2026-08-15. The recall test was not flaky. It was correctly reporting silent
data loss, and the first attempt at a fix would have hidden it.**

`recall_against_exact_search_is_high` failed CI at **0.865** against a `>= 0.90`
bar, on a pull request whose diff could not reach `kimmy-vector`. The obvious
reading — a randomised structure makes a hard threshold unreliable — was wrong,
and acting on it would have been the worst available outcome.

**What was actually happening.** Over 1,500 builds the distribution is
**bimodal**: median 0.995, and about **one build in 250** collapsing as low as
**0.545**. In a collapsed build, 19 of 20 queries degrade together — a global
property, not one hard query. Probing every stored vector with *its own exact
value* found the cause: a healthy build fails to self-retrieve ~2 of 400 points,
while a bad one fails **40 to 96 — 10% to 24% of the collection**. Those
documents were unreachable from the graph's entry point, so **no search at any
`k` could ever return them**, until something happened to rebuild the index.

**Everything else was eliminated by measurement, not argument:**

| Candidate | Verdict |
|---|---|
| SIMD divergence between machines | `anndists` builds without `simdeez_f`; scalar path everywhere |
| Core count / rayon scheduling | rayon is only in `parallel_insert`/`parallel_search`; this code calls the sequential ones |
| Misconfigured graph | `Hnsw::new` argument order checked against the crate source |
| Unusually tall graph | failures occur at the **modal** top level, not rare ones |
| Poor connectivity (`keep_pruned`) | improves the bulk (median 0.990 → 1.000), leaves failures untouched |
| Too-narrow search (`ef`) | helps then **plateaus**: 5/800 at ef=50, 2/800 at ef=100, 2/800 at ef=200 |

The `ef` plateau is the tell: more exploration recovers hard-to-reach points but
cannot cross into a disconnected component.

**The fix is at build time, where the defect is.** `HnswIndex::build` asks a
sample of the stored vectors to retrieve themselves and rebuilds a graph that
cannot. Draws are independent, so the failure rate falls off geometrically.
Measured over 1,500 builds with the check in place: **minimum recall 0.9600,
nothing below 0.95, zero failures**, against a floor of 0.5350 before. It never
fails the build — a poor index still answers, with a warning, because taking
vector search down over a quality problem would be worse than serving it.

**Both constants are sized from measured distributions**, because two earlier
attempts at this were wrong for exactly the reason this register keeps
recording — a number chosen without knowing the shape of what it bounds:

- The **sample** is 128, not 32. At 32 the catastrophic builds were caught but
  *mild* orphaning of a few percent slipped through: detection is `1-0.97^n`,
  62% at 32 and 98% at 128.
- The **trigger** is 3 sampled misses, not "any miss". A healthy graph
  legitimately misses ~2 of 400 (median 3, p99 9), so retrying on any miss at
  all rebuilt most collections twice for nothing — **a 3× build-cost regression
  that no recall measurement could see**, since every rebuild is fine and only
  the wasted work differs. The measured cost at 3 is one rebuild for **11.2%**
  of builds, ~1.11× overall.

**The rejected fix is worth recording.** The first attempt averaged recall over
three graphs. It would have smeared a data-loss bug into an acceptable-looking
statistic — and, because a mean of three draws three chances at a bad graph, it
would have failed CI *more* often, not less (~1 in 300 → ~1 in 100). Its
supporting measurement was real but irrelevant: it sampled the bulk of the
distribution and never observed the rare mode that actually breaks the test.

**The test is restored unchanged.** Fixing the defect made the original
assertion honest rather than lenient, which is the outcome to prefer whenever a
test looks flaky: ask what it is reporting before making it quieter.

**Operational note.** The fix prevents new bad builds; it does not repair one
already cached in a running node or persisted as a snapshot. See
[Operations](operations.md) — a collection whose vector search looks
incomplete should have its index rebuilt.

---

## 🟢 Closed

**Reindexing is `POST /vector` again — and the old escape hatch never worked.**
The register carried "changing model or dimension needs a
disable-with-`drop_vectors` and re-enable, which backfills from the oplog."
The second half of that sentence was **false, and had always been false**: the
embedding worker's backfill was its first-ever run starting from the
beginning of the oplog, and a worker whose position has ever advanced is past
old entries forever. Enabling embedding on a collection that already held
documents embedded *nothing* — on any node whose worker had previously run —
and re-enabling after a drop embedded nothing either. A claim in this
register, recorded once and never re-checked against the code: the
native-toolchain lesson, again.

Now the worker treats every `ConfigureVectors` oplog entry as a reindex
trigger: it scans the **collection** — the documents are the durable source;
the oplog may have collected their entries — and re-embeds what the
configuration demands. Idempotency is layered: per document, the HLC
staleness check; per scan, a fingerprint of the configuration written only
after the scan completes, because a configuration change is invisible to
per-document HLCs (configurations do not touch documents). A crash mid-scan
replays the entry; a replay after completion costs a scan and no embedding.
Dimension changes are now legal in place for server-embedded collections —
both search paths already skip width-mismatched records, so old vectors are
invisible while the backfill replaces them — and still refused for `byo`,
whose vectors the server can never regenerate.

**Also found and fixed on the way:** the worker's provider cache had no
eviction, so a collection reconfigured to a new model would have kept
embedding through the *old* provider for the life of the process. The cache
now stores the configuration each provider was built from and rebuilds on
mismatch.

**No webhook was ever delivered in any clustered deployment — found by the
cluster harness on its first run.** The live set SWIM maintains contains
*peers only*: it is populated by foca's `MemberUp` notifications, which never
fire for the node holding it. Rendezvous ownership computed an owner over
that set — so `owner == me` was false on every node, for every subscription,
and every node stood down. Three real nodes, gossip formed, webhook
registered, document inserted: zero deliveries. Single-node worked (an empty
set means "own everything"), which is why the whole M6 suite, every mutation
run, and every live single-node drive passed while ADR-045's central claim —
"when the owner dies, another takes over" — described machinery that had
never once delivered in a cluster.

Fixed in `ownership::owns`: the candidate set is the live peers **plus this
node**, which also dissolves the empty-set special case. The trade, stated:
a node SWIM has declared dead still considers itself a candidate, so a
flapping node can deliver alongside its replacement — a duplicate, which
at-least-once already promises, where the alternative was silence.
`peer_only_views_still_elect_exactly_one_owner` now models what production
actually looks like; the original unit tests all hand-built member sets that
included the owner, which is precisely how the assumption survived.

Two lessons, both already in the register in other clothes: a unit test that
hand-builds its input can encode an assumption the real feed violates — the
fixture lesson again — and a feature verified only in the topology where its
hard path cannot occur has not been verified. The harness exists so the
second one stops recurring.

**The three findings from the M6 code review.** Recorded 2026-08-10, closed in
the M7 warm-up branch:

- **Webhook events carried empty `database` and `collection` fields.**
  `render_without_document` emitted both as `""`, always — for the whole of M6,
  with every test passing, because every test asserted on fields that were
  right and none looked at these two. They are now filled from the
  subscription, the delivery test asserts them **on the wire** rather than on
  `render`'s output, and the docs show them. The lesson is the fixture one
  again, in a new shape: a payload assertion that lists what must be present
  says nothing about what else is being sent.
- **The egress check and the dial resolved DNS separately.**
  `EgressPolicy::check` resolved and checked every address — and then `reqwest`
  resolved *again* to connect, so a rebinding attacker with a zero TTL could
  pass the check and flip the record before the dial. The documented rule
  ("checked before each delivery") was true; it just checked addresses nobody
  was obliged to dial. Closed by `CheckedResolver`: the delivery client's own
  DNS resolver runs `permits_addrs` on what it resolves, so the approved
  addresses are, by construction, the dialled ones. The pre-delivery `check`
  stays — it is what refuses literal addresses, which never reach a resolver.
  Found on the way: the client was built with `unwrap_or_default()`, whose
  fallback is a default client that follows redirects and resolves unchecked —
  a build failure would have silently shed both egress protections. Now the
  dispatcher refuses to start without its client, loudly.
- **Backoff state outlived its subscription.** Failure state was only cleared
  by a successful delivery, which a removed subscription can never have. The
  map is now pruned against the registry each pass.

**The webhook "saturation" risk did not exist, and the real one was its
opposite.** M6 task notes and the roadmap's open questions both asked for a
"per-node delivery cap, so a webhook on a hot collection cannot saturate a
node's outbound connections". That risk was never reachable. `dispatch_once`
delivered inside a `for` loop with one `.await` per subscription, so the node
had **at most one HTTP request in flight at any moment**, no matter how many
subscriptions existed or how hot the collection was. A bound of one cannot be
exceeded, so a cap would have capped nothing.

The premise came from reasoning about the design as described — "the dispatcher
is an oplog consumer that dials out for each owned subscription" — rather than
from the loop as written. Nothing in the code contradicted it out loud, and the
serial `.await` is one character of syntax, so it survived three branches and a
plan review.

What the serial loop *did* cause is the inverse failure, and a worse one: one
endpoint that stopped answering held the entire pass for the full ten-second
`DELIVERY_TIMEOUT`, delaying **every subscription queued behind it in the loop**.
That is exactly the cross-subscription interference the per-subscription
`Backoff` was built to prevent, reappearing one layer up — and it means a
webhook the operator does not control decides when the ones they do control
fire. Backoff hid how bad it was, because a repeatedly-failing endpoint is
skipped after its first failure; the lasting damage is from an endpoint that is
*slow* rather than dead, which never backs off and pays the timeout every pass.

Both are now addressed by the same change: deliveries run concurrently under a
semaphore of `webhooks.max_concurrent_deliveries` (default 8). Concurrency
removes the head-of-line blocking; the bound makes the original request true
rather than vacuous, since there is now something that could saturate. See
[ADR-045](decisions.md) and `a_slow_endpoint_does_not_hold_up_another_subscription`.

**The lesson worth keeping:** a risk asserted in a plan is a hypothesis about
the code, not a fact about it. This one was written down three times and
reviewed each time without anyone reading the loop. Before building a mitigation,
confirm the failure it mitigates can actually occur — the check costs one
reading and would have reframed the whole task.

**Backup and restore.** The only answer was a cold file copy, which meant
stopping the node — and copying `kimmy.redb` from a running one captures a torn
file. Now `GET /v1/admin/backup` takes a consistent snapshot inside a read
transaction while the node serves, and `kimmyd restore --from` writes it into a
fresh data directory. [ADR-041](decisions.md). The residual sharp edge is
inherent: a backup carries the node's identity, so restoring one onto two nodes
gives them the same id.

**TLS between nodes.** Replication frames were plaintext; `cluster_secret`
authenticated peers without hiding what they exchanged. Now TLS, always on,
with the handshake bound to the secret through the TLS exporter rather than to
a certificate — so there is no PKI to run and a man-in-the-middle cannot relay
the handshake. [ADR-040](decisions.md). The residual limit is unchanged and
inherent: a node's identity is still only "holds the cluster secret".

**Collection ids above `i64::MAX` broke replication.** A live bug, not a
deferral, and the most serious thing found since the collection-id fix itself.
Ids are derived by hashing, so they use the whole `u64` range; BSON has no
unsigned 64-bit type; and `CollectionId` used a derived `Serialize`. Any id in
the upper half — **about 48% of collection names** — could not be encoded, so
every oplog entry naming that collection was unsendable and the collection
never replicated. The write succeeded locally; the peer logged one
`malformed frame` warning per round.

Found by running three containers, not by the suite, which used a single
collection name that happens to hash low. Fixed by giving `CollectionId` one
fixed representation (bit-cast to `i64`), matching what `NodeId` already needed
for the same class of bug. On-disk format untouched — ids are persisted by the
hand-rolled codec, not serde. [ADR-031](decisions.md).

**SWIM was silently degraded under the shipped container defaults.** Not a code
bug: `cluster.bind` defaults to a wildcard, and the node correctly refuses to
advertise one, falling back to loopback with a warning ([ADR-037](decisions.md)).
But `docker-compose.yml` — the documented way to run a cluster — never set a
per-node bind, so all three nodes advertised `127.0.0.1` and gossip never
formed. Replication still converged via discovery, which is what made it look
fine. Compose now pins a subnet and a per-node address; Kubernetes uses the
downward API. Verified: both survivors now declare a killed node down within
17 ms of each other, where before nothing was ever declared down.

**TLS for clients.** Was 📋 M5 and the reason "terminate at a proxy" appeared in
every deployment note: tokens and passwords crossed the wire in plaintext
otherwise, including the bootstrap login that sets the first password. The
listener now terminates TLS itself — `axum-server` over `rustls`, on the `ring`
provider already in the build. Set `server.tls.cert_file` and
`server.tls.key_file`. [ADR-039](decisions.md). A proxy is still a fine
deployment; it is no longer the only one. **Node-to-node replication is still
plaintext** — that is a separate piece with its own trust question, and it stays
📋 in the table above.

**Login rate limiting.** Was a 🔴 in [Security](security.md) and a 📋 in this
register: `/v1/auth/login` had no limit, so a password was guessable as fast as
the network allowed, and each guess cost a full Argon2id hash. Now a token
bucket per client address, refusing with `429` and a `Retry-After` **before**
authentication runs. Only failed attempts are recorded, so a client with correct
credentials is never throttled for succeeding. What remains is scope, not the
mechanism — see the three 🟡 entries above.

**Oplog and tombstone GC.** Was the most serious 🟡 in this register —
retention was configured but not enforced, so both tables grew without bound.
Enforced now by a background pass every `storage.gc_interval_secs`. The design
constraint that mattered was not the collection itself but that **the newest
oplog entry must never be collected**: the logical clock resumes from the oplog
tail, so an aged-out tail would reset the clock on restart and make every later
write lose to its own older version, silently. [ADR-028](decisions.md).

---

## 🟢 Deliberate departures in M3

**`kimmy-mcp` depends on `kimmy-api`, not the reverse.** The crate graph
carried a placeholder arrow from M0 pointing the other way. Inverted so both
edges share one executor with the authorization check inside it; the
alternative was duplicating Extended JSON conversion, the query planner path,
and vector search dispatch into a second crate. [ADR-024](decisions.md).

**Tools are not filtered by grant.** A read-only token sees every write tool and
is refused when it calls one. Hiding is not an enforcement boundary, and a
filtered list makes refusals unexplainable. [ADR-025](decisions.md).

**`rmcp`'s `Host` allow-list is off by default.** It is DNS-rebinding
protection for unauthenticated local servers; `/mcp` verifies a bearer token
before the transport runs. The SDK default would have rejected every client
connecting by a real hostname. Operators can re-enable it via
`server.mcp_allowed_hosts`. [ADR-026](decisions.md).

**MCP resources exclude `__kimmy` and `.__vectors`.** A resource is material an
agent attaches to its context, and the user store is a column of password
hashes. Tools still reach them under the ordinary access check, so this is a
default rather than a control. [ADR-027](decisions.md).

**The MCP listing tools exclude them too.** `list_databases` and
`list_collections` omit the same internals resources do — a listing is an
invitation to open what it names — while `find`, `count` and
`describe_collection` reach an internal by name as before. The REST listing
stays complete: the CLI and operators target `orders.__vectors` on purpose.
[ADR-092](decisions.md).

**Sessions are disabled.** Stateless, so a token that expires mid-conversation
stops working rather than riding an already-open session. The cost is that a
long-running agent must re-authenticate.

---

## 🟡 Simplifications inside working features

**Keyword search is term overlap, not BM25.** It exists to give hybrid search a
lexical signal, and RRF only uses the *ordering*, so absolute scores need not
be principled. A real BM25 would rank better on its own. On short documents
the ordering it produces is close to random — nearly every candidate shares a
word or two with the query — and equal-weight fusion then costs hybrid search
recall against plain vector search. The per-request `weights` and
`min_overlap` fields on `hybrid_search` are the mitigation ([ADR-094](decisions.md),
[Vectors](vectors.md)); a BM25 half is deferred until those have been measured.

**Chunking counts characters, not tokens.** A token count depends on the
model's tokenizer, which the storage layer has no business knowing. The default
(2000 chars ≈ 512 tokens) assumes prose and overshoots for dense text — a
2000-character chunk of a transcript came to 1073 tokens on a live cluster and
was refused by a 1024-token provider on every scan. `chunk.max_tokens` bounds
that with a deliberately pessimistic estimate (two bytes per token); it is a
ceiling on an estimate, not a tokenizer, and a model with an unusually dense
tokenization can still be overshot. Set it below the provider's limit.

**No minimum score threshold on search.** k-NN returns the `k` nearest even
when nothing is genuinely similar, so a query against unrelated content still
returns results with near-zero scores. Callers must threshold themselves.

**`skip` is O(n)** even with an index. Deep paging stays expensive.

**Result order without an explicit `sort` is unspecified**, and differs between
an index-backed query and a scan. Matches MongoDB, still a footgun.

---

## 🟢 Replicated writes now reach change streams (was the 🔴 M4 blocker)

**Was.** An applied remote entry keeps its originating stamp, so it entered the
oplog *behind* the local tail and a subscriber past that point never saw it.
Single-node streams were unaffected, which is why it had not bitten.

**Now.** A second ordering — `oplog_arrival` — over local arrival sequence, with
the oplog still keyed by origin stamp for conflict resolution and anti-entropy.
The maintainer chose this over restamping on arrival or documenting the
limitation.
Resume tokens are unchanged; they are translated to an arrival position at watch
time, because tokens live in clients where no migration can reach them. Detail
in [Oplog](oplog.md).

**Two bugs closed on the way.** Streams de-duplicated by comparing stamps, which
discarded exactly the replicated entries this was meant to deliver; and they
trusted publication order, which can differ from commit order under concurrency.
Both dissolved once the broadcast became a wake-up rather than a data path —
[ADR-030](decisions.md).

---

## 🟢 Reads are bounded by what they return, not by what they scan (ADR-098)

**Was.** Three read paths held something proportional to the collection.
`count` collected every matching document and took the vector's length — over
a `__vectors` shadow, every vector and its text. An index-backed `find`
gathered every candidate key in the range, sorted and deduplicated, before
rechecking the first, so an unselective equality with `limit: 1` held the whole
range and a `$in` union added a set on top. A sorted `find` collected every
match as a `(stamp, document)` pair to sort it, and `skip` was unbounded. Each
was the likeliest way for one ordinary request to take a small host over its
memory.

**Now.** One visitor behind `find` and `count`. A count counts. Index
candidates stream out of one read transaction and are rechecked as they
arrive — an exact probe as one run already in `_id` order, a `$in` as a merge
of such runs, a range in `_id` order by keeping the `skip + limit` smallest
keys of a pass. A sorted `find` keeps a heap of the `skip + limit` least, with
`_id` ascending as the final key so the page is the one the stable sort gave,
and that window may not exceed 10,000: refused with `400`, not clamped.
`explain` reports `indexEntriesRead` beside `documentsExamined`, so an exact
probe stopped early and a range read in full can be told apart.

**What still scans.** `count` is O(n) in time and O(1) in memory. A range plan
in `_id` order still reads its whole range; it no longer holds it. `update` and
`delete` with `multi` use the engine's own in-transaction scan, which gathers
keys per chunk and is unchanged here.

**The behaviour change** is the sort window, recorded in
[ADR-098](decisions.md) and in [Compatibility](compatibility.md).

---

## How to use this document

When something here is closed, move it to 🟢 with a note on what changed —
don't delete it. The record of *why* a thing was once wrong is worth more than
a clean list.

When a new deferral is made, add it **at the time**, not later. Every 🔴 above
became one because it was recorded somewhere local and never surfaced.
