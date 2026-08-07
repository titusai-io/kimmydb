# Testing

[← Documentation index](README.md)

What is tested, how, and — more usefully — *why those particular things*.

---

## Current state

```
299 tests passing · 0 failures · clippy clean at -D warnings
```

| Crate | Tests | Focus |
|---|---|---|
| `kimmy-core` | 54 | HLC, key encoding, comparison, LWW merge, resume tokens |
| `kimmy-storage` | 63 | Codecs, engine lifecycle, document CRUD, change streams |
| `kimmy-query` | 85 | Filter, update, sort, projection semantics |
| `kimmy-auth` | 43 | Passwords, tokens, RBAC, user store |
| `kimmy-api` | 35 | 20 unit (JSON boundary, errors) + 15 end-to-end over a real socket |
| `kimmyd` | 14 | Config layering and validation |
| `kimmy-cluster` | 5 | Discovery string parsing |

---

## The principle

**Test the things whose failure is silent.**

A panic announces itself. A wrong query result does not — it propagates into
someone's report, weeks later, on data you cannot reproduce. So testing effort
is allocated by *how quietly a bug would fail*, not by how much code there is.

That is why the key encoder gets 2,000-case property tests plus a third
independent oracle plus mutation testing, while the CLI argument parser gets a
handful of examples.

---

## Layers

```mermaid
graph TB
    U["Unit tests<br/><i>named behaviours, one module</i>"]
    P["Property tests<br/><i>invariants over generated input</i>"]
    I["Integration tests<br/><i>real router, real socket</i>"]
    M["Mutation checks<br/><i>do the tests actually detect bugs?</i>"]

    U --> P --> I --> M
    style M fill:#2d3748,color:#fff
```

---

## The load-bearing invariants

These five carry disproportionate weight. Anything that breaks one produces
wrong answers rather than crashes.

### 1. Key encoding order

```
encode(a).cmp(encode(b))  ==  canonical_cmp(a, b)
```

Indexes are redb key ranges compared with `memcmp`. Break this and range scans
quietly miss documents.

- `encoding_order_matches_canonical_order` — 2,000 generated pairs
- `equal_values_encode_identically` — an index lookup for `5` must find `5.0`
- `compound_encoding_matches_lexicographic_component_order`

The generator concentrates on boundaries where order-preserving encodings
actually break: type edges, `i64::MIN`/`MAX`, `2^53 ± 1`, subnormals, NaN, ±∞,
NUL bytes in strings, empty documents and arrays.

### 2. Numeric order against an independent oracle

The encoder cross-check has a hole: `keyenc` encodes numbers *through*
`cmp::decompose`, so the two are not independent for numeric values. A bug there
corrupts both identically and the property test passes.

So `numeric_order_matches_an_independent_oracle` (4,000 cases) checks against a
third implementation sharing no code with either — comparing integers as
integers, and mixed int/double pairs by splitting the double into floor plus
remainder, never rounding the integer.

Plus `ordering_is_transitive` and `ordering_is_antisymmetric`, because sorts and
range scans over a non-total order are undefined.

### 3. HLC monotonicity

No sequence of physical timestamps, however adversarial, may produce a
non-increasing clock.

- `tick_is_strictly_monotonic` — arbitrary time sequences including backwards jumps
- `observe_dominates_local_and_remote` — interleaved local and peer timestamps
- `successor_is_immediate` — nothing sorts between a timestamp and its successor,
  which is what makes resume-after exact
- `encoding_preserves_order` — the oplog range-scans by raw bytes

### 4. LWW convergence

Merge must be commutative and idempotent, or replicas will not converge.

`concurrent_writes_converge_regardless_of_arrival_order` applies the same two
conflicting writes in opposite orders on **two separate engines** and asserts
byte-identical results.

### 5. Change-stream continuity

`resuming_under_continuous_writes_has_no_gaps_and_no_duplicates` — 500 writes on
a background thread while a subscriber reads a prefix, disconnects, and resumes
from its token. Asserts the delivered sequence is exactly `0..500`, in order.

This is the test that justifies the subscribe-then-replay ordering. Also
`a_lagging_consumer_recovers_from_the_oplog_without_losing_events` (2,500 events
with nobody reading).

---

## Mutation testing

A passing property test proves nothing if it cannot detect a bug. Two faults
were injected into the encoder deliberately, and both were caught with minimal
counterexamples:

| Injected fault | Caught by | Counterexample |
|---|---|---|
| Removed negative-number byte inversion | encoder cross-check | `Int32(-1)` vs `Int32(-2)` |
| Changed exponent normalization `63 → 62` | numeric oracle | `Double(2^53)` vs `Int64(2^53)` |

Worth repeating whenever a new invariant is added: break it on purpose, confirm
the suite goes red, revert.

---

## Integration tests

`crates/kimmy-api/tests/api.rs` drives the **real router over a real TCP
socket** — not handler functions directly — so routing, extractors, status
codes, and the JSON boundary are exercised the way a client meets them. Each
test binds port 0 so they run in parallel without collision.

Security properties are asserted as behaviour, not assumed:

| Property | Test |
|---|---|
| Login does not reveal whether a user exists | `a_wrong_password_does_not_reveal_whether_the_user_exists` |
| RBAC blocks writes, DDL, and user admin | `rbac_is_enforced_on_every_route` |
| 403 does not leak existence | same test — a nonexistent collection also returns 403 |
| Listing hides unreadable collections | `listing_hides_what_the_caller_cannot_read` |
| `2^53 + 1` round-trips exactly | `extended_json_types_survive_the_boundary` |
| The last user cannot be deleted | `the_last_user_cannot_be_deleted` |

---

## Tests that caught real bugs

Worth recording, because each one shows the test doing its job:

| Bug | Consequence | Found by |
|---|---|---|
| `apply_remote` treated an equal stamp as a win | Redelivered oplog entries republished duplicate change events — and peers resend overlapping ranges routinely | `applying_the_same_remote_entry_twice_is_idempotent` |
| Index ids derived from `max(existing) + 1` | A new index would inherit a dropped index's stale entries | `index_ids_are_never_reused` |
| `$options` parsed as an independent operator | Regex flags silently dropped, so `$regex` + `$options: "i"` was case-sensitive | `regex_honours_sibling_options` |
| `$elemMatch` scalar form did not parse | `{$elemMatch: {$gt: 5}}` errored on arrays of numbers | `elem_match_works_on_arrays_of_scalars` |

In the first two cases the test encoded the *intended* invariant and the
implementation was wrong — which is the right way round.

---

## Verified by hand

Some things are only convincing against a running server. These were driven
manually and are recorded so they can be repeated:

| Check | Result |
|---|---|
| Full CRUD + query + update over HTTP | ✅ |
| Live change stream over WebSocket, single node | ✅ 3 events with `fullDocument` |
| Resume after a token | ✅ no redelivery of the acknowledged event |
| Ancient resume token | ✅ `410 resume_token_expired` |
| RBAC for a scoped analyst | ✅ read allowed; write, DDL, user admin all 403 |
| Restart durability | ✅ data, users, node identity all survived |
| `docker stop` | ✅ exit 0 in ~20 ms |

A minimal WebSocket client was written for this (`scratchpad/wsclient.py`) since
none was installed — worth keeping for future manual verification.

---

## Conventions

**Test names are sentences.** `a_lagging_consumer_recovers_from_the_oplog_without_losing_events`
says what must be true. A failure list then reads as a list of broken
properties.

**Comments explain the *why*, not the *what*.**

```rust
// Distinguishing them would turn login into a user-enumeration oracle.
assert_eq!(wrong_password.to_string(), no_such_user.to_string());
```

**Assertions carry messages** where the failure would otherwise be cryptic.

**Determinism.** Time is a parameter, so no test sleeps waiting for a clock.
Async tests use explicit timeouts and fail with progress information rather than
hanging.

---

## Running

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check

cargo test -p kimmy-core keyenc          # one module
cargo test -p kimmy-api --test api       # integration only
PROPTEST_CASES=10000 cargo test -p kimmy-core   # deeper property search
```

CI runs fmt, clippy, and tests, then builds the Docker image and smoke-tests it
with `check-config`.

---

## Gaps

Honest list of what is not covered:

| Gap | Notes |
|---|---|
| No benchmarks | 📋 M5. No regression baseline exists |
| No fuzzing | The codecs are the obvious target |
| No multi-node tests | Nothing to test until M4 |
| No crash-consistency tests | redb is trusted for durability |
| No concurrent-writer stress test | Only the change-stream test writes concurrently |
| Property tests use default case counts | 256 unless overridden; the critical ones raise it explicitly |

---

## Adding a test

Ask: **if this broke, would anything notice?**

- Would it panic? Lower priority — it announces itself.
- Would it return a wrong answer? **Highest priority.** Property-test it.
- Would it leak information? Assert the observable behaviour, not the code path.
- Is it an invariant rather than an example? Property test, then break it on
  purpose to confirm the test has teeth.

---

## Next

- [Key Encoding](key-encoding.md) — the most heavily tested component
- [Decisions](decisions.md) — the choices these tests defend
