# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-26 — **standing unique violations are a query**

Branch `feat/unique-violation-surfacing`, ADR-087. `GET
/v1/db/{db}/coll/{coll}/violations` — counts per index, or with
`?index=<name>` the colliding groups with their documents — derived on
request from the retained oplog: `Engine::live_unique_violations` pages
`read_oplog_from` (inclusive at `from`, so the previous page's tail is
skipped by stamp), keeps `UniqueViolation` entries for the collection,
deduplicates by index and id set (a resend records twice), and keeps only
those whose named documents all still exist. `exec::violations` shapes the
two responses; authorised as `read`. Spec: the path, a `ViolationGroup`
schema, and both shapes driven by the contract test. Docs: a "Resolving a
unique violation" recipe in `indexes.md`, the route table and a paragraph in
`http-api.md`, a pointer in `time-and-conflicts.md`. Known limit, recorded
in the ADR: a rewrite that changes the colliding value resolves the
constraint but not the report, because the route does not re-evaluate keys;
a delete always clears it. Test: the collision is manufactured through
`apply_remote` in the API harness — the same path a replicated write takes
— rather than the multi-process cluster harness (deviation recorded on the
plan, same reasoning as WS6): report appears with both documents, names the
merged one, clears on delete, and the route is `read`-authorised.
## As of 2026-08-26 — **`multi` commits in chunks; the 10,000 cap is gone**

Branch `feat/multi-chunked-commits`, ADR-086. ADR-083 had made a
`multi: true` request one transaction and, because that holds the single
writer for the whole request, capped it at `MAX_CANDIDATES` with a refusal —
a new failure mode for a request that used to run to completion. Now
`Engine::modify_where` loops: one write transaction per chunk of
`Engine::multi_chunk_docs()` documents (an `AtomicUsize` set from
`storage.multi_chunk_docs` at startup, default `DEFAULT_MULTI_CHUNK_DOCS` =
1,000, clamped to `1..=MAX_CANDIDATES`), `collect_matches` taking an `after`
key bound that every candidate path honours (`Keys` and the index union are
sorted and deduplicated first; the scan uses `doc_range_after`), publish per
chunk after its commit, resume strictly after the last key written. The
`MAX_CANDIDATES` refusal now applies only when `limit` is `None`, i.e. to
`find_and_modify`'s `choose`. `ModifyManyOutcome` gains `commits`; `update`
and `delete` responses carry it; the spec, storage/http-api/query-language/
compatibility docs and README say "chunked" where they said "refused above
10,000". Tests: 10,001 documents → 11 commits and none visited twice; chunk
size 5 over 12 documents → 3 commits with events arriving chunk by chunk in
key order; a failure in chunk two leaves chunk one committed and exactly its
events published; `Keys` offered out of order and duplicated resume
correctly; config bounds; an API test of `commits` over 2,500 documents.

## As of 2026-08-26 — **`chunk.max_tokens`: a byte-budget ceiling under the character rule**

Branch `feat/token-aware-chunks`, from `main`, no ADR (a config field with a
non-breaking default). The test cluster's recovery rescan on 0.10.1 logged
one `backfill permanently failed` for `app.transcripts`: llama-embed refused
a 1073-token chunk that the 2000-character rule had let through, and the
document has no vectors on any scan since. `ChunkConfig` gains `max_tokens:
Option<usize>` (`serde(default)`, skipped when absent, so stored
configurations round-trip unchanged) and `split` cuts a window at the earlier
of `max_chars` characters or `max_tokens × 2` bytes — two bytes per estimated
token is below prose (≈4), code (≈2.5) and CJK (≈3), so the estimate errs
short. Overlap stays in characters; the stride is the window's own length
less the overlap, never below one. Validation refuses `Some(0)`. The three
permanent-failure `WARN`s in the worker now name the collection and document
(`db`/`collection`/`doc` on backfill, `collection`/`doc` on the streaming and
deferred paths) so an operator can find what was skipped. Tests: dense CJK is
cut under a 100-token budget where the character rule shipped one chunk;
prose under budget is untouched; overlap holds when the byte budget cuts;
zero refused; absent round-trips absent. Docs: vectors.md Chunking, openapi
`chunk.max_tokens`, deviations (the entry stays — it is a ceiling on an
estimate, not a tokenizer). The test-cluster mitigation is separate and
operator-side: raise llama-embed's `--ubatch-size`/`--batch-size` or set
`max_tokens: 1024` on `app.transcripts` and reindex.
## As of 2026-08-26 — **retention guards: tombstones outlive the oplog, stale rejoiners are named**

Branch `feat/retention-guards`, ADR-085, from `main`. Two operational
guards around partition resurrection. `Config::validate` refuses
`tombstone_retention_secs < oplog_retention_secs` — the one setting under
which a partition shorter than the oplog window resurrects data — with a
message saying why, a row in `operations.md`'s refused-at-startup table, and
a unit test. The replication loop now knows which peer it talked to
(`open_handshake` returns the `NodeId` from `Welcome`) and how far that peer
trails us (`SyncOutcome::behind_ms` = `lag_behind_ms(theirs, mine)`); when
that exceeds `ReplicationConfig::tombstone_retention` it warns once on the
transition, logs once on recovery, and calls `on_peer_staleness` — the same
callback shape as `on_lag`, for the same reason. `AppState` keeps the record
(`report_peer_staleness` / `stale_peer`, `StalePeer { since_ms, behind_ms }`,
the clock starting on the first report) and `/v1/topology` adds `staleSince`
/ `behindSecs` to the peer's entry only while the condition holds; spec
updated. Tests: config refusal, the reversed-lag unit test in `sync.rs`
(a fresh member trails nobody), and an API test that injects reports and
watches the entry appear, keep its start, and disappear. Not done: a
multi-process cluster-harness test — the harness has no way to restart a
node on its data directory (`Drop` SIGKILLs), and a two-second retention
would make the cluster flag itself while idle; the decision is a pure
function and is tested as one.
## As of 2026-08-26 — **conditional writes: `if_stamp`, `stale`, and stamps everywhere**

Branch `feat/if-stamp-cas`, ADR-084, stacked on `docs/guarantee-table`. The
check-then-act primitive an AP store can honestly offer: every write reports
the stamp it produced, `find` returns stamps on request (`stamps: true`, a
parallel array), a read by id sets `ETag`, and every single-document write
accepts `if_stamp` — `PUT` / `DELETE` by id as a query parameter, `update` /
`delete` / `find_and_modify` as a body field. A mismatch is `409 stale`,
`retry: no`, on every route alike, and writes nothing. Engine:
`Stamp::encode/decode` (opaque base64url of hlc‖node) in `kimmy-core`;
`StorageError::Stale { current }`; `replace_if` / `delete_if` on top of a
shared `delete_where` in `docs.rs`; `ModifySpec::expected_stamp` checked in
`modify_in_txn`, with `collect_matches` now carrying each match's stamp;
`insert_stamped`, `get_stamped`, `get_record_by_encoded_key`,
`for_each_record_after` as the stamped twins of the existing reads. API:
`ErrorCode::Stale` (the closed set is now 18), `Capability::ConditionalWrites`,
`exec::parse_if_stamp`, `ReplaceParams`, and `multi` + `if_stamp` /
`upsert` + `if_stamp` refused as `400`. Spec: every touched operation, the
`Stamp` schema, the `stale` row, the capability row — and two stale
descriptions fixed on the way (`update`/`delete` "one document at a time",
`getDocument` "find on `_id` is a scan"). Clients: Rust `update_if` /
`replace_document_if` / `delete_document_if` / `Query::stamps` /
`ErrorCode::Stale`; Python `if_stamp=` on `update` / `delete`, `stamps=` on
`find`, `is_stale`; Go `UpdateIf` / `DeleteIf` / `Query.Stamps` / `Stale()`.
Conformance scenario `stale_write_is_typed` in all three drivers. Tests: six
API tests including an eight-writer race with exactly one winner; four
`modify.rs` tests; two `docs.rs` tests. Deliberate non-goals, recorded in the
ADR: `If-Match`/`412`, and any cross-node meaning.

## As of 2026-08-26 — **one table for what each operation guarantees**

Branch `docs/guarantee-table`, documentation only. The guarantees were
spread over four places that had each drifted a little — the README list,
`storage.md`'s durability table, `time-and-conflicts.md`'s consistency
table, and prose in `http-api.md` — and the lost-update defect fixed in
ADR-083 sat in the gap between them for months. `docs/compatibility.md`
now carries "What each operation guarantees": every route, what it
promises, where the engine enforces it, and the **test name** that defends
it, so a claim without a test is visible as such. The other three places
link to it and name it the authority. Found and fixed on the way:
`time-and-conflicts.md`'s Status section still said the cluster transport
"does not exist yet; M4 adds gossip" — nine milestones stale.

## As of 2026-08-26 — **`update` lost concurrent increments on a single node**

Branch `fix/update-in-transaction`, ADR-083. Found by reading `exec::update`
against the durability table rather than by an incident: the route collected
its targets through `collect_matching` in a read transaction, applied the
operators in memory, and stored each document through `Engine::replace` in
its own write transaction — so the operators ran on an image another writer
could already have replaced. A multi-threaded API test (four tasks × 500
`$inc` on one document) left the counter at **500** on main. `delete` had
the same shape, and a `multi` update that failed on a later document had
already committed the earlier ones.

Fix: `Engine::modify_where` in `kimmy-storage/src/modify.rs` — the
`find_and_modify` body applied to every match instead of a chosen one, with
`modify_in_txn` as the single per-document function behind both and
`collect_matches` as the single candidate scan (with a `limit`, so
`multi: false` is `stop_after = 1` on the same path). `Candidates` gained a
`Keys` variant, because the storage enum had no primary-key path and routing
`update {_id: 1}` through it would have scanned the collection under the
writer; `exec::candidates_for` now plans primary key → index → scan for
`update`, `delete` **and** `find_and_modify` — the last of which had been
scanning on `_id` all along. `explain` reports the plan as chosen, from a
`PlannedAccess` plus the engine's `examined`/`matched` counts.

Consequences worth knowing: a `multi: true` request is one transaction
(one fsync, all-or-nothing) and inherits `MAX_CANDIDATES` as a refusal —
CHANGELOG `### Changed` calls it out. `modified` still counts documents
written, not changed (the documented deviation). Tests: the API race test
(`multi_thread` flavour — a current-thread runtime cannot interleave two
synchronous executors and passes either way), eight storage tests in
`modify.rs` covering one-commit-per-request, `stop_after`, no-op, all-or-
nothing abort, removal, `Keys` lookup/dedup, an eight-thread counter, and
the cap. Suite 1371 passed / 0 failed. Lifting the cap by committing in
bounded chunks is the next storage decision, not this branch.

## As of 2026-08-26 — **the streaming path never counted its embeddings**

Branch `fix/streaming-embed-counters`, the last of the day's finds and the
oldest: the 0.5.0 "three of five `kimmy_embed_*` counters never move" report,
finally reproduced cleanly once the 0.10.1 roll gave the test cluster a live
owner worker. A probe insert into `testdb.notes` embedded and replicated in
under ten seconds while member A's `documents_embedded` stayed at the 1 its
recovery rescan had counted. `EmbeddingWorker::process` embeds a locally
written document inline from its entry — `provider.embed` then `put_vectors`
— and neither line touched `self.counters`; `embed_one` carried a comment
calling itself "the single choke point every embedding path funnels through",
which was false, and every counter test went through it (deferred re-checks
and scans), never through `process` with a local entry. Fix: `inspect_err` on
the provider call and `counters.embedded(count)` after the write, mirroring
`embed_one`; both comments now say there are two sites. Test drives `process`
with a locally written entry (1 doc / 1 chunk / 0 failures), then a
`fail_times` provider outage (1 failure, docs unchanged, retryable error) and
the retry (2 docs, still 1 failure). Fails on `main` at the first counter
assertion. The 0.5.0 investigation had eliminated the plumbing correctly and
pointed at "vectors are being written without passing through `embed_one`",
which was exactly right.

---

## As of 2026-08-26 — **the embedding worker now survives a collected position**

Branch `fix/vector-worker-lost-position`, found minutes after the 0.10.0 repair
roll on the test cluster: a probe document inserted into a vector-configured
collection was never embedded, and every `kimmy_embed_*` counter on the owner
stayed at zero. The owner's log had the answer one second after its restart:
`embedding worker stopped: change stream resume token is no longer available`.
Member A had been dark for ten hours (see the two entries below), retention had
collected the oplog past the worker's persisted position, `Engine::watch`
refused the token (correctly — a silent skip would hide a gap), and
`EmbeddingWorker::run` propagated the error, so `kimmyd` logged once and ran on
with no worker. `ChangeEvent::Invalidate` mid-run ended it just as quietly.
Fix: `run` is a recovery loop — on a refused position (or an invalidated
stream) it reopens from the oldest retained entry *first* (`watch` subscribes
before it reads, so nothing written during the scan is lost) and then
`rescan_owned` walks every owned, server-embedded collection through the same
`scan_collection` a `ConfigureVectors` backfill uses, idempotent via the
staleness check. The stream loop moved into `drive`, which returns
`StreamEnd::{Ended, Invalidated}` instead of breaking. Test: position recorded
at the *first* oplog entry (the GC never collects the newest, which is why the
first fixture attempt did not reproduce), GC with zero retention, `watch`
asserted to refuse it, then the worker must embed a pre-outage document (the
rescan) and a post-outage one (the fresh stream) and still be running. Fails on
`main` — the worker task ends and neither document is embedded. Not an ADR: the
recorded position's semantics are unchanged; only what happens when it is gone.
**The 0.5.0 "dead embed counters" plan is likely the same symptom seen through
a different lens** and should be re-measured after this ships: on 0.10.0 the
counters are wired correctly end to end (`WorkerCounters` →
`set_vector_counters` → `render`), and a worker that is not running reads as
zeros.

---

## As of 2026-08-26 — **snapshots could not encode half of all collection ids**

Branch `fix/snapshot-collection-id-encoding`, found while verifying the
livelock fix below against the test cluster: at 06:40–06:55Z — 24 h after
cluster birth — every pair's pinned resume point fell behind the 24 h oplog
retention horizon, every round fell back to a snapshot, and every snapshot
failed. The serving side said why: `malformed frame: Unsigned integer
10245841737121747810 cannot fit into BSON`. `SnapshotCursor.collection` and
`SnapshotDoc.collection` were bare `u64`; `CollectionId` itself learned to
serialise as reinterpreted `i64` bits in ADR-031, but the snapshot types
never picked it up, and `snapshot.rs`'s tests transfer pages in-process
without serialising. The existing network horizon test passed only because
`("shop", "orders")` happens to hash below `i64::MAX`. Fix: both fields are
`CollectionId`. Tests: a storage BSON round-trip of a page (cursor included)
for a collection found by searching names until the derived id has the top
bit set, and a network horizon test with the same fixture — which fails on
`main` with exactly the test cluster's error and passes here. No ADR: a
defect fix. **Operationally this is the release that un-darkens the test
cluster**: with it, each member's first round past the horizon pulls a
snapshot, and `absorb_version_vector` raises the witnessed vector too, so the
pin clears.

---

## As of 2026-08-26 — **the sync livelock: a full batch now proves its window (ADR-082)**

Branch `fix/sync-full-batch-progress`. Root-caused from a 25-minute debug
window on member B at 0.7.0: 94 of 95 pulls from member C read `applied=0,
superseded=1021` (plus `ddl=3` on the peer summary — 1024, a full batch), the
lag gauge pinned at the cluster's age. Mechanism: `VersionVector::behind` is
one threshold — my own floor at the origin I trail most — and an advertised
stamp I can never receive (a unique-violation entry is locally stamped and
never shipped, ADR-029) pins that floor; the cure, absorbing the peer's
vector, ran only on a *short* batch, so once a full window of
already-witnessed entries sat under the pin, no round was ever short again.
Fix: `coverage_after_batch` in `kimmy-storage/src/sync.rs` — after a full
batch, raise every advertised origin to `min(their coverage, last delivered
stamp)`; short batches unchanged. The transport's `Entries` arm now calls one
storage method, `apply_peer_batch`, so the decision is tested between engines
with no network. **Trap found writing the test:** the batch limit applies
*before* the violation filter, so a violation *inside* the raw window
shortens the batch and the old cure fires — the pin needs the unshippable
stamp *beyond* a full window, which is why it took a rarely-writing member
with a large replicated backlog to show it. The three-engine test fails
without the fix (`superseded 7, ddl 1` every round for the whole budget) and
converges with it. Rider: the storage debug line `merged a batch from a peer`
now prints `ddl` and `unknown_collection`; the asymmetry with the INFO
summary cost an hour on the test cluster. Storage suite 262/0. After this
rolls to the test cluster: expect batches to shrink then stop, the `ddl=5`
lines to cease and the gauge to decay toward 0 — then remove member B's
temporary `KIMMY_LOG_LEVEL` from the test deployment's compose file.

---

## As of 2026-08-26 — **incarnation floors: the drop/recreate CI flake, root-caused**

Same branch as below (`feat/cli-token-flow`) after its CI run tripped
`documents_written_before_a_drop_do_not_return_to_a_recreated_collection` —
left 1 / right 0, unreproducible locally across 16 runs. Mechanism: collection
ids are *derived* (`CollectionId::derive(db, name)`), so drop-and-recreate
reuses the id; the tombstone guard compares strictly and a peer's final
pre-drop write tying with the drop's millisecond escapes it. Fix = **ADR-081**:
`CollectionMeta.incarnation_floor` (serde-defaulted `None` for legacy rows, no
migration), set to the **drop's own stamp** whenever a creation happens over a
tombstone, compared inclusively (`<=`). First attempt floored at the creation
stamp instead — broke four replication tests, because a replicated creation's
meta records the *receiver's* clock, which postdates the whole catch-up
backlog; the drop stamp is the boundary that travels correctly. Deterministic
regression test drives stamps at exactly `dropped_at` and at `ca_old.created`.
Full storage suite green 259/0; network replication suite green.

---

## As of 2026-08-26 — **login once, then the CLI just works (ADR-080)**

Branch `feat/cli-token-flow`. The maintainer's own transcript broke it open: `init` →
`login` → `whoami` returned 401 *missing Authorization header* because login
printed a token and kept nothing, while whoami read only `--token` /
`KIMMY_TOKEN` / the dotfile. Three changes: login caches unconditionally and
`--cache-token` is removed (ADR-080 amends ADR-075); every data command falls
back to that cache via `cached_bearer` — same discovery, same key precedence
(`cached_key_client_id`, env > file, mirrored from apply_kimmy_file so lookups
cannot drift from writes); the device flow offers Enter-to-open-browser,
terminal-gated. Verified live by planting a fake cached token in a sandboxed
XDG cache: whoami went from "missing Authorization header" to "authentication
token is invalid" — proof the cached bearer is found and sent. The `.kimmy`
`cache_token` key still parses but no longer wires to anything.

---

## As of 2026-08-26 — **init discovers instead of interrogating; a shipped panic fixed**

Branch `feat/init-discovery`. `kimmy init` asked nine questions and showed no
defaults on first run, made the operator hand-type the RFC 8707 resource (the
one value the node already publishes — and the easiest to mistype), and read
secrets with terminal echo on. Now it asks for the node URL alone, discovers
resource + issuer from the node's RFC 9728 metadata (`oidc::discover_from_node`,
same document `login` already consumed), shows `kimmy-cli` as a real default
for the client id, falls back to explicit prompts only when the node publishes
no metadata, carries existing secret keys forward untouched on re-runs, and
never reads an interactive secret. Colors gate on tty + `NO_COLOR`.

The re-run exposed a **v0.7.0 panic**: with any settings file present,
`apply_kimmy_file`'s `_` arm still asked clap for `issuer`'s value source on
subcommands that have no such argument — so every non-login command
(`databases`, `ping`, …) died with `"issuer" is not an id of an argument` until
the file was deleted. Fixed by skipping provider-field application for anything
that is not Login/Token. Manual matrix verified: init first run, init re-run,
databases and ping with file present. Gap worth closing later: no test harness
drives apply_kimmy_file against arbitrary subcommands; the fix is guarded by
manual verification only.

---

## As of 2026-08-25 — **wildcards no longer reach the system database**

Branch `feat/system-db-wildcard` (ADR-079). `{db:"*"}` stopped matching
`__kimmy`: the cluster owner's own `*/*` federated role listed it in
`kimmy databases`, and reading `__users` meant reading password hashes. Two
doors remain — `admin` anywhere (root untouched, proven by the suite passing
unmodified before the new tests landed) or an exact `{db:"__kimmy"}` grant,
honored down to its collection pattern; patterns like `__k*` are wildcards and
do not match. Choke point is `Principal::can`, so listings inherit the rule.
Collection listings inside `__kimmy` keep hidden-not-forbidden: empty list,
never a name. Tests cover wildcard denial (listing + find/count 403s), exact
grant honored to the pattern, admin-anywhere, and the pattern-is-not-exact
trap at both rbac and api level.

---

## As of 2026-08-25 — **the admin surface reaches the CLI**

Branch `feat/admin-cli`. Implements [[kimmydb-admin-cli-roles-and-users-management]]:
`kimmy roles` (list/show/create/grant/revoke/delete) and `kimmy users`
(list/show/create/reset-password/set-grants/set-roles/disable/enable/delete).

- **One server addition**: `POST /v1/users/{name}/disabled` — the `disabled`
  field existed and the session check honored it, but nothing could set it.
  Bumps `token_version` + evicts sessions, so disable *is* revocation;
  re-enable does not restore those sessions. Guards mirror deletion (not
  yourself; not the last enabled user — checked only on a real state change).
- Grant shorthand everywhere: `db:actions` or `db:collection:actions`, e.g.
  `--grant 'sales:orders*:read,search'`. Duplicates refused client-side.
- `roles grant/revoke` are fetch-modify-post against the replace-semantics API
  — the concurrent-operator race is documented, not hidden.
- Docs contract satisfied: route literal in http-api.md table, openapi path +
  driven twice in `tests/openapi.rs`, behavior tests in `api.rs` (disable ends
  sessions & refuses logins; guards; 404). Full workspace green.

---

## As of 2026-08-25 — **v0.6.0: the federation round ships**

Release prep only — no behaviour changed on this branch. Workspace version
`0.5.0` → `0.6.0` and `## Unreleased` retitled to `## 0.6.0`, which is the step
that has twice nearly shipped empty release notes; verify with
`dist plan --tag=v0.6.0 --output-format=json` and a non-empty
`announcement_changelog` before tagging.

What the release carries, all merged 2026-08-25: `KIMMY_OIDC_ROLE_MAPPINGS`
(#112, ADR-078), `kimmy whoami` + the zero-grant note (#113), and the flipped
login default with `kimmy token` (#114). Together they close the gap where a
federated caller authenticated successfully and still could not see anything.

---

## As of 2026-08-25 — **`kimmy login` federates by default, and `kimmy token` re-prints**

Branch `feat/login-defaults-to-device-flow`. Two changes to how the CLI hands
out credentials, both driven by how the test deployments actually authenticate:

- **The default flipped.** Bare `kimmy login` now runs the device flow;
  naming a user (`kimmy login ada`) is the local password path. The old
  default refused with "name the user to log in as", so nothing scriptable
  broke. `--oidc` is still accepted and now only spells the default out —
  docs and scripts written against it keep working.
- **New `kimmy token`.** Prints the token again: cache hit while fresh,
  otherwise one federated flow whose result is cached. It is
  `--cache-token` as a subcommand — ADR-075's "only when asked" holds because
  invoking it *is* asking. Deliberately federated-only: a local token has no
  issuer/client/resource to key a cache entry by, so `kimmy login <user>`
  stays that path.

Flow selection lives in one tested helper (`login_flow`: named user → Local,
then `--client-credentials`, else Device). The unauthorized hint, after-help,
and every doc example were updated to the new spellings; historical handoff
sections keep their old ones on purpose.

---

## As of 2026-08-25 — **the CLI can see a zero-grant identity coming**

Branch `feat/cli-zero-grant-notice`. Companion to the merged
`feat/oidc-role-mappings-env` (#112, the server-side half). Two additions:

- **`kimmy whoami`** — prints `/v1/auth/whoami` (an existing endpoint) as
  one-line JSON: principal, federated flag, grants. The diagnostic that would
  have answered the zero-grants session in one command.
- **The note.** After `databases` or `collections` returns empty, the CLI asks
  whoami once; if `grants` is present-and-empty it prints a stderr hint naming
  `kimmy whoami`. Stdout is byte-identical either way (machine contract);
  the gate is strictly "grants field exists and is empty", so an identity with
  grants looking at a genuinely empty namespace is never nagged, and any
  unexpected payload shape fails closed (no hint).

Decision functions (`listing_is_empty`, `is_zero_grant`) are unit-tested
including fail-closed shapes; whoami endpoint semantics stay covered by
`crates/kimmy-api/tests/api.rs`. No route changes, openapi contract untouched.

---

## As of 2026-08-25 — **role mappings become deployable from an environment block**

Merged as #112. The gap: a deployment configured through env vars (compose,
swarm, k8s) could federate but could never configure `role_mappings`, which was
TOML-only. Every federated caller on such a node holds zero grants, which
presents as empty listings and bare 403s — found live by the cluster owner
against his own database.

What landed: `KIMMY_OIDC_ROLE_MAPPINGS` (ADR-078) — one JSON array,
**replaces** the file's list when set, no per-mapping flags (ADR-066's rationale
stands), all startup refusals apply through the same `validate`. Parser errors
name the variable and show the shape. Tests cover replace-over-file precedence,
the error message, and that validate still refuses an empty mapping arriving
through the env form.

Not decided here, deliberately: whether the test deployments adopt it is a
change on the deployment side, outside this repository; IdP integration
guides are their own docs page.

---

## As of 2026-08-24 — **v0.4.0: roles become objects**

Release prep only — no behaviour changed on this branch. Workspace version
`0.3.0` → `0.4.0`, and `## Unreleased` retitled to `## 0.4.0`, which is the step
that has twice nearly shipped empty release notes. Verified rather than
eyeballed: `dist plan --tag=v0.4.0 --output-format=json` reports a
2434-character `announcement_changelog` naming both features, and
`dist generate --check` is clean.

A `0.MINOR` under ADR-062: the round adds features and changes behaviour — a
role mapping naming neither `role` nor `grants` now stops the node at startup,
where it was previously accepted and silently granted nothing.

**What it ships:** WS5d, roles as first-class stored objects (ADR-073) and
`auth.oidc.allow_federated_admin` (ADR-074). This closes the architectural
asymmetry the WS5 review found — local users carried grants directly on their
record while federated users got them from an IdP claim, so "analyst" meant one
thing in a config file and a hand-assembled copy of it on every user record.

**Nothing about the default posture changed.** `allow_federated_admin` is off,
so a federated principal still cannot hold `admin`, and the test that pins it
asserts the same answer the live provider ran into when it presented a token
claiming `roles: ["user", "admin"]`.

**Upgrade notes:** none. No storage migration, no schema bump, and a user
holding no roles gets exactly what it got before. The one thing an operator
should check before upgrading is that no existing `[[auth.oidc.role_mappings]]`
entry is empty — one that names neither `role` nor `grants` is now a startup
refusal rather than a silent no-op.

---

## As of 2026-08-24 — **WS5d: roles become objects, and `admin` becomes federatable on request**

Closes the architectural asymmetry the WS5 review found: the system had two
authorization models in it. Local users carried grants directly on their record
— an ACL, copied onto every principal — while federated users got them from an
IdP claim through `[[auth.oidc.role_mappings]]`, which is RBAC. "analyst" meant
one thing in a config file and a hand-assembled copy of that thing on each user
record, with nothing keeping the two in agreement.

A role is now one stored object both paths point at. `Role`, declared since the
first RBAC pass and never constructed outside a round-trip test, is finally the
thing this stores. ADR-073 and ADR-074.

**No storage migration, and the obvious reading is wrong.** `UserStore` is not a
redb table — users are BSON documents in an ordinary system collection created
on demand — so `__roles` needs no migration, `User.roles` behind
`serde(default)` decodes every existing record as holding none, and
`SCHEMA_VERSION` stays at 3. An earlier plan for this branch specified a 3 → 4
bump; following it would have added a migration for nothing.

**Three things worth carrying forward:**

- **Resolution is per request, and that is the whole feature.** Pre-resolving
  the mapping table when the verifier is built is the obvious cache, and it
  silently freezes every federated principal's permissions at startup so a role
  edit changes nothing until a restart. `OidcVerifier` does no I/O by design, so
  resolution lives in the `Auth` extractor — the first place holding both the
  role names and the engine. There is a test that fails if you cache it.
- **`allow_federated_admin`'s check had to move to resolution time too.** A
  startup refusal is enough for an inline mapping, which cannot change while the
  process runs. It is not enough for a stored role, which can be edited to
  include `admin` minutes after the node booted. Off by default, so nothing
  about the shipped posture changed — the boundary a live provider asserting
  `roles: ["user", "admin"]` ran into still holds.
- **The documentation contract earns its keep.** Registering four routes failed
  three separate tests: absent from `docs/http-api.md`, absent from
  `docs/openapi.yaml`, and — the one worth having — documented in the spec with
  nothing driving them. The last forced real coverage rather than a spec entry.

**Every new test was checked against its own removal**, not merely observed to
pass: disabling the admin filter makes the federated-admin test return 200 where
it expects 403, and making role resolution a no-op fails both resolution tests.

`invalidate_holders_of_role` now returns the holder *names* rather than a count.
Bumping the stored token version is only half a revocation — the session check
reads a cache in front of it — and a caller cannot evict from that cache with a
number.

**Still open:** WS4, which the maintainer rescoped on 2026-08-24 to a documented
client-library pattern rather than engine features, and the resource identifier's
public hostname not resolving while the 401 challenge points at it.

---

## As of 2026-08-23 — **v0.3.0: the OAuth 2.0 round, and the first release validated against a real provider**

Release prep only — no behaviour changed on this branch. Workspace version
`0.2.0` → `0.3.0`, and `## Unreleased` retitled to `## 0.3.0`, which is the
step that has twice nearly shipped empty release notes. Verified rather than
eyeballed: `dist plan --tag=v0.3.0 --output-format=json` reports a 7201-character
`announcement_changelog`, `dist generate --check` is clean, and both binaries
report `0.3.0` with the commit.

A `0.MINOR` under ADR-062 because the round carries a user-visible behaviour
change: `kimmy login --client-credentials` no longer requests `openid profile`.

**What it ships:** WS5a resource identity (ADR-071), WS5b verifier hardening
(ADR-072), WS5c the CLI as an OAuth client (ADR-075), WS5e the stated
boundaries (ADR-076), plus two defects found by actually running the thing —
`check-config` printing the signing key, and `/mcp` sitting outside the layer
that counts and challenges.

**This is the first release whose federation was validated end to end against a
live provider** rather than a stub JWKS. What that run established, beyond the
happy path: a token's `aud` is the resource identifier and not the issuer; a
node configured with a *different* audience refuses the same valid token; the
provider stamps `typ: at+jwt`, so `require_at_jwt = true` is safe here; a local
HS256 token still works on the same node as a federated one; `/v1/auth/refresh`
refuses a federated principal while a local one still refreshes; and — the one
worth keeping — **an identity the provider asserts is `admin` gets no admin
here**, because nothing maps it. ADR-067's break-glass boundary was tested
against a provider actively claiming superuser, not merely reasoned about.

---

## As of 2026-08-23 — **`/mcp` was outside the instrumentation layer**

Found during the WS1 end-to-end validation, by sending an unauthenticated
request to `/mcp` against a live provider and noticing the 401 carried no
`WWW-Authenticate` while the identical REST 401 did.

One cause, four symptoms. `kimmy_api::router()` applied the `count_request`
layer to its own routes and `node.rs` then did `app.merge(mcp_router(..))`.
**A router merged after a layer keeps its own empty middleware stack** — `merge`
combines route tables, and a layer already applied stays with the routes it was
applied to. So `/mcp` had no request counter, no latency timing, no trace span,
and no challenge header.

The header is the half that matters. WS5a's §5.1 singled out `/mcp` as the
reason RFC 9728 mattered *more* than usual: an MCP client holding no credentials
has no other way to find its authorization server. The metadata document was
published correctly the whole time — the 401 just never pointed at it, so the
discovery chain was broken at its last link.

The counting half was measurable and is the one that proves it: three MCP calls
moved `kimmy_requests_total` by zero.

Fixed by adding `kimmy_api::router_with(state, extra)`, which merges before
layering; `router()` delegates to it with `None`, so every existing caller is
unchanged. `node.rs` passes the MCP router through it.

Two regression tests, both verified to **fail against the old mounting** and
pass against the new one. The counting test is deliberately self-calibrating:
`kimmy_requests_total` is a single unlabelled counter and a `/metrics` scrape
counts itself, so "the number went up" passes whether or not `/mcp` is counted.
It measures a scrape-only interval first and requires exactly one more than
that.

**Worth noting for its own sake:** the MCP test harness said *"Merged exactly as
the daemon merges it, so the test exercises the real mounting rather than a
convenient stand-in"* — and that faithfulness is precisely why it reproduced the
defect instead of catching it. Mirroring production exactly makes a test share
production's blind spots. 1301 → 1303.

No ADR: a defect, not a decision.

---

## As of 2026-08-23 — **check-config stops printing the signing key**

Found by running `check-config` against the live identity provider for the WS1
end-to-end validation, which is the first time anyone had run it with real
secrets in the environment. It printed `auth.jwt_secret` and
`auth.root_password` in full, because it serializes the entire `Config` and
nothing redacted them.

The signing key is the serious half: it is the HS256 secret behind every local
token, so anyone reading it can forge `root`. And the leak is in the
*documented* path — `kimmy.example.toml` keeps both fields commented out and
tells the operator to use `KIMMY_JWT_SECRET` and `KIMMY_ROOT_PASSWORD` instead,
so the secret is meant to exist only in the environment right up until
`check-config` writes it to stdout.

Both fields now serialize as `<redacted>` via one `serialize_with` on the two
`Option<String>`s. `None` still serializes as absent, so "is it set?" survives
— that is what `check-config` is for. The placeholder is deliberately shorter
than the 16 bytes `validate` requires of a signing key, so `check-config` output
pasted back into a config file refuses to start rather than running with a
placeholder for a key; that refusal fires first whenever authentication is on,
which is also what stops the bootstrap password being taken literally on a fresh
database.

Two tests pin it: one asserts no secret value reaches the serialized form while
both field *names* still do, the other that the placeholder is refused by
`validate`. 1299 → 1301.

No ADR: nothing was decided here that was open. It was a defect.

---

## As of 2026-08-23 — **the boundaries are written down**

Documentation only; no code changed and no test count moved.
[ADR-076](decisions.md) records the two decisions.

**Authorization stops at RBAC, and the collection is the ceiling.** No
document- or field-level security, no ABAC, no policy engine. The load-bearing
sentence is that **named roles will not move that ceiling** — WS5d's arrival is
otherwise easy to read as having solved multi-tenancy, and it does not. The
same words appear in the known-limits table and in ADR-076 so the two cannot
drift apart.

**`/metrics` keeps its unauthenticated place on the main listener.** Evaluated,
not built, with a stated trigger: **if `/metrics` ever gains a label carrying a
name, the trade inverts** and the second listener stops being ceremony. Adding
such a label and adding the listener are one piece of work — do not do the
first without the second.

**A trailing `*` on a grant's `db` matches a prefix**, so `sales*` covers
`salesforce` and anything created later with that prefix. Documented, with the
habit that avoids it (`sales_*`). **Deliberately no startup warning**: it is a
legitimate grant, `{ "db": "*" }` is the commonest one in existence, and a
warning on every start for something correct is one nobody reads by the second
week.

**ADR numbering.** WS5e took **076**, so the numbers now run 071 (WS5a), 072
(WS5b), 075 (WS5c), 076 (WS5e), with **073 and 074 still reserved for WS5d**.
An earlier note said WS4's ADRs start at 076 — **they start at 077.**

## As of 2026-08-23 — **the CLI behaves like an OAuth 2.0 client**

Three conformance fixes in `crates/kimmy-cli/src/main.rs`, none of them
touching the server. [ADR-075](decisions.md) covers the third.

**Client authentication prefers HTTP Basic**, read from the provider's
`token_endpoint_auth_methods_supported`. RFC 6749 §2.3.1 makes Basic mandatory
for a server and the request body optional, so the body is the fallback and a
provider that advertises nothing gets Basic. **The id and secret are
form-encoded before the Basic header is built** — §2.3.1 requires it, and
skipping it is invisible until a secret contains a `:`, a `+`, a space or a
non-ASCII character, at which point the failure reads as a wrong password.
`form_urlencode` is written out rather than pulled from a crate, to keep this
binary's short dependency list.

**The two flows now default to different scopes.** `--scope` was one shared
`default_value` of `openid profile`; client credentials has no end user, so
`openid` there asks for an ID token that cannot exist. It now asks for nothing.
**This is the one behaviour change a user could notice** — a service account
relying on the old default must pass `--scope` explicitly.

**`--cache-token` is opt-in and stores only the access token.** The no-disk
promise was stated in three places — the `after_help`, the `Login` doc comment,
and `docs/security.md` — and all three were amended in the same change rather
than left contradicting the code. `cache::get_from` and `cache::put_into` take
an explicit path so **no test ever sets `XDG_CACHE_HOME`**: environment
variables are process-global and cargo runs tests in parallel, which is the
`include_names` race in a different costume.

**If you extend the cache**, note that a token with no `expires_in` is
deliberately not stored, that every read failure is a miss rather than an
error, and that `0600`/`0700` are applied explicitly after creation because
`File::create` goes through the umask.

Gates: fmt clean, clippy clean, 39 CLI tests (up from 28). Verified by running
the binary: the bare `--cache-token` flag, `KIMMY_TOKEN_CACHE=1`, `=0` (does
not enable, does not error) and `=maybe` (errors) all behave. **The full
login → cache → reuse loop against a live provider is not exercised yet** —
that needs the two client registrations the deferred end-to-end validation
covers.

## As of 2026-08-23 — **the federated verifier checks three more things**

Small, independently testable hardening of the OIDC path. Nothing here changes
a shape; it closes gaps a review of the shipped WS1 code found against the
RFCs. [ADR-072](decisions.md) carries the reasoning.

**`nbf` is validated.** `jsonwebtoken` defaults `validate_nbf` to `false`, so
setting `validate_exp` — which reads like "check the times" — left the front
edge unchecked and a token stamped valid from next week was accepted today. The
claim stays optional, because it is optional in RFC 7519; the same 60-second
leeway as `exp` applies.

**A discovery document must name the issuer it was fetched for**, and its
`jwks_uri` must be `https`. **This is checked in two places, and they are not
redundant:** `fetch_jwks` in `crates/kimmyd/src/node.rs` (the node picks
signing keys from it) and `mod oidc`'s `discover` in
`crates/kimmy-cli/src/main.rs` (the CLI sends a client secret to endpoints from
it). The rule is written twice rather than shared because `kimmy-cli`
deliberately links no kimmy crate that could carry it — the same reasoning
already recorded on `PROTECTED_RESOURCE_METADATA_PATH`.

**Loopback is exempt from the `https` requirement**, per RFC 8252 §7.3, and the
host is *parsed* rather than prefix-matched — `is_secure_url` in both files.
`http://127.0.0.1.attacker.example` and `http://127.0.0.1@attacker.example` are
both refused, and there are tests for each. The exemption is load-bearing for
the test suite too: `stub_idp` serves over plain HTTP on `127.0.0.1`, and
without it the fetch-path tests would need TLS.

**`typ: at+jwt` is opt-in**, `auth.oidc.require_at_jwt`, default `false`. It
**cannot** default to strict: Entra ID stamps `typ: JWT` on v2 access tokens.
This is the same shape as ADR-071's narrowing — an RFC applied literally would
break a named target provider — and it is worth recognising the pattern before
adding a third such check. Note that ADR-071 already closes the ID-token
confusion for any operator whose audience is an `https` URL.

**If you extend `OidcSettings`, five test constructors need the new field**:
`kimmy-auth/src/oidc.rs`, `kimmy-api/src/federation.rs`,
`kimmy-api/tests/api.rs`, `kimmy-client/tests/client.rs` and
`kimmyd/src/node.rs`. `cargo build` will not tell you — it does not compile
`#[cfg(test)]` code. Use `cargo clippy --all-targets`.

Gates: fmt clean, clippy clean, **1289 passed / 0 failed / 12 ignored** (up 12
from the 1277 baseline this branch started at).

---

## As of 2026-08-23 — **KimmyDB has a name of its own to an authorization server**

The branch that wrote this section closes the gap that made WS1's federation
correct and unusable: **the CLI could not send an RFC 8707 `resource` parameter
at all**, so the only audience `kimmy login` could ever obtain was whatever the
provider defaulted to — its issuer URL, for a conformant server. That made
`audience = "<issuer>"` the one working configuration, which is a single
audience shared by every resource the provider serves, which is exactly what an
audience restriction exists to prevent.

**Read [ADR-071](decisions.md) before touching any of it.** The short version:
`auth.oidc.audience` *is* the resource identifier when it is an `https` URL.
There is deliberately no second key. Two values that must always be equal are
one value, and the startup refusal invented to police them was the tell.

**Three things follow from the identifier, and they are one feature:** RFC 9728
metadata at `/.well-known/oauth-protected-resource`; a `WWW-Authenticate`
challenge on every 401/403 pointing at it (RFC 6750 §3); and `kimmy login`
sending the identifier as a `resource` parameter on both flows. The payoff is
`kimmy login --oidc --url <node>` with nothing else set — the CLI reads the
issuer and the resource off the node — and it is the same path a conformant MCP
client uses to discover where to authenticate, which matters because `/mcp` is
on this listener.

**Three things worth knowing before changing it:**

1. **The `http://`-only refusal is deliberate and narrow.** The obvious reading
   of RFC 8707 §2 is "validate every audience that has a scheme", and it would
   refuse `api://<guid>` — Entra ID's own default audience for a registered
   application, and Entra is a named target provider. A scheme does not make an
   audience a resource identifier; `https` does. `api://` and `urn:` values stay
   valid and simply publish nothing.
2. **The challenge is a middleware, not part of `ApiError`.** The RFC 6750 §3
   distinction between a request that offered no credentials and one that
   offered a bad one depends on the *request*, which an error value has never
   seen — and `From<AuthError>` has no state to find the metadata URL with
   anyway. `count_request` already wraps every route and still holds the
   request. `/v1/auth/login` is excluded on purpose.
3. **The router registers the well-known path as a literal.** Building it from
   `PROTECTED_RESOURCE_METADATA_PATH` made the route invisible to the route
   scanner in `tests/openapi.rs`, which reads source literals — so the docs
   contract stopped covering it while still passing. That scanner also now
   normalises axum's `{*name}` catch-all to OpenAPI's `{name}`, because the two
   spell the same parameter differently and nothing else in the file knew.

**Not done, and next:** the end-to-end validation against the live IdP. It needs
the resource registered at the provider — on some providers that is
`oauth.protected_resources` plus `allowed_resources` on each client
registration, all matching `auth.oidc.audience` byte for byte. Nothing in the
code waits on it.

---

## As of 2026-08-23 — **OpenTelemetry is in, and it exports spans only**

The branch that wrote this section adds distributed tracing and OTLP metrics.
It is **off unless `telemetry.endpoint` is set** — no `enabled` flag, for the
same reason `[server.tls]` and `[auth.oidc]` have none — and when it is on, the
default exports no database name, collection name or request path.

**Three ADRs hold the reasoning**, and the first is the one to read before
changing anything here: ADR-068 (attribute privacy — `include_names` off by
default, span names from route templates and operation names, **log events not
exported at all**), ADR-069 (OTLP over HTTP, never gRPC), ADR-070 (counters
bridged to OTLP rather than duplicated).

**The layering is load-bearing.** `kimmy-api` takes the OTel *API* plus
`tracing-opentelemetry` and the semantic-conventions constants — enough to
extract an inbound `traceparent` and inject an outbound one, and nothing that
can start an exporter. `kimmyd` takes the SDK and is the only crate that
decides whether one exists. `kimmy-storage`, `kimmy-cluster` and `kimmy-vector`
gained **no new dependency at all**: they were instrumented with the `tracing`
they already had, and the binary's layer converts their spans. Dotted field
names (`db.operation.name = "find"`) are valid `tracing` and become OTel
attributes; `tracing` also takes a *constant* field name in braces, which is
what lets the semconv constants be used directly rather than re-spelled.

**Spans go where the invariants already are.** `exec.rs`'s `op_span` sits beside
the `authorize` that was already the first line of every executor function, so
REST and MCP produce identical spans for free — the same argument that put the
authorization check there. `routes.rs` opens the request span inside
`count_request`, reusing its `"/healthz" | "/readyz" | "/metrics"` predicate.
`WriteTxn::commit` is the storage span, and `commits_are_counted_at_one_chokepoint`
already proves it is the only fsync.

**The two things that surprised me, both found by running it rather than by
reading it:**

1. **Log events leak.** `tracing-opentelemetry` turns every event inside a span
   into a span event *carrying that event's own fields*. With the audit filter
   alone in place, a live collector received `collection: "orders"` hanging off
   a `create_collection` span while `include_names` was off and every span
   attribute was correctly empty. The filter is now `metadata.is_span() &&
   target != "kimmy::audit"` — spans only, logs stay logs. Gating field by
   field would have meant auditing every `info!` in the workspace, forever.
2. **Unsigned numbers arrive as strings.** `tracing-opentelemetry` has no
   `record_u64`, so a `u16` status code falls through to `record_debug` and
   reaches the collector as `Str("200")` — which the semantic conventions say
   is an integer, and which a backend filtering on it would not match. Every
   numeric span field is recorded as `i64` for that reason. Verified as
   `Int(200)` on the wire.

**Metrics are bridged, not duplicated.** `Metrics::snapshot` is the new read
surface; observable instruments read it at export time. `/metrics` is
byte-for-byte unchanged and pinned two ways — a golden test over the whole
`render()` string, and an ordered series-name assertion over the HTTP body,
because the route prepends engine gauges and `kimmy_storage_bytes` is a file
size. Deployed clusters scrape that endpoint; treat a diff as a
release note. Note the OTLP names deliberately differ: `kimmy.requests`, not
`kimmy_requests_total`, because a collector's Prometheus exporter would
otherwise emit `kimmy_requests_total_total`.

**`Config` lost its `Eq`** (kept `PartialEq`) because `sample_ratio` is an
`f64`. Nothing in the workspace needed it.

**`main.rs` was restructured** so the command is known before `logging::init`:
`check-config` and `restore` pass `None` for telemetry, so validating a file
does not open a connection to production's collector. The `TelemetryGuard` is
held across `node::run` and dropped after it returns, matching the shutdown
discipline already written there.

### Verified, not asserted

- **An unreachable collector costs nothing.** Pointed at a closed port, 2,000
  point reads gave p50 0.153 ms / p99 0.311 ms against 0.159 / 0.315 with
  telemetry off, and the node kept serving.
- **A live collector receives what it should.** A REST insert carrying an
  inbound `traceparent` produced exactly three spans in *that* trace — the
  route template, `insert`, `storage.commit`, correctly nested and rooted at
  the caller's span — with no span events, no `db.namespace`, no
  `db.collection.name`, no `url.path`, and the strings `sales` and `orders`
  absent from the entire export.
- **A webhook delivery carries a valid `traceparent`**, unsigned, and the
  `x-kimmy-signature` a receiver verifies is unchanged.
- **The cost of tracing** is in `docs/benchmarks.md`: about 6–17% of read
  throughput at `sample_ratio = 1.0` with the collector on the same machine,
  and −31% in the one-client cell where the server is otherwise idle. It is
  span construction, not export.

### Worth knowing before changing this

- `internal-logs` is kept on for `opentelemetry_sdk` and `opentelemetry-otlp`
  even though everything else is `default-features = false`. Without it a node
  pointed at the wrong port exports nothing, forever, silently.
- The exporter takes a programmatic endpoint **verbatim** — it does not append
  the signal path — so `signal_url` does it. A base handed straight through
  would POST to the collector's root and be answered 404 forever while the node
  served perfectly.
- **`https://` endpoints are refused at startup.** `opentelemetry-http` uses
  `reqwest` 0.13 (a different major from this workspace's 0.12) pulled with no
  TLS backend, and the only way to give it one drags in
  `rustls-platform-verifier` → `security-framework-sys`, a new native crate.
  That is a decision, not a line for the allowlist, so it was left undone and
  the refusal names the alternative. **If TLS to a collector is wanted, that is
  the next decision here**, and the native-deps question is the whole of it —
  on Linux the same feature adds nothing.
- The metrics bridge holds a `Weak<AppState>`, so the final export at shutdown
  observes nothing (the state is already dropped). The periodic export is
  60 s; a node that lives less than that reports no metrics at all.

---

## As of 2026-08-23 — **enterprise OIDC federation is in; local auth is untouched**

The branch that wrote this section adds federation with **one** external
OpenID Connect provider, as an extension rather than a rework. `TokenIssuer`
(HS256, cluster secret, local users) is byte-for-byte what it was; a second
verifier, `kimmy_auth::OidcVerifier`, checks RS256/ES256 against the
provider's JWKS with issuer, audience and expiry validation and 60 s of
leeway. The `Auth` extractor reads the **unverified** `iss` and routes to one
verifier or the other, so a token is only ever offered to the verifier it
claims to belong to — which is what leaves no algorithm-confusion surface.
Nothing downstream changed: `Principal::can`, RBAC, MCP, the audit log and the
search-without-read grant all treat a federated principal as a principal, and
`kimmy-storage`, `kimmy-query`, `kimmy-mcp` and the executor were not touched.

**Four ADRs hold the reasoning** and are worth reading before changing any of
this: ADR-064 (verifier routing), ADR-065 (revocation asymmetry — a federated
identity has no `__users` record, so `Sessions::check` skips it and
`/v1/auth/refresh` refuses it), ADR-066 (role mappings inline in the config
file, not in the database), ADR-067 (**`admin` is not federatable**, refused at
startup, a break-glass boundary).

**Shapes worth knowing.** `kimmy-auth` still does **no I/O** and that is
load-bearing — the key set is injected, and `kimmyd::node` owns discovery, the
JWKS fetch and the refresh task (modelled on `spawn_cert_reloader`).
`kimmy_api::Federation` is the swappable holder between them: the request path
reads it, the refresher replaces it wholesale, and an unknown `kid` raises a
**rate-limited** nudge for an early refetch — rate-limited because a `kid` is
attacker-controlled. `Principal::federated` sits beside `unauthenticated` and
reaches the audit record and `/v1/auth/whoami`.

**Boot deliberately does not wait for the provider.** A briefly unreachable
IdP must not stop a database from restarting, so the fetch retries in the
background and local users keep working. The cost is a quiet failure mode —
a node that has stopped reaching its provider serves perfectly until the
provider rotates, then refuses every federated caller at once — which is why
`kimmy_jwks_refresh_total{outcome="failed"}` exists and why `kimmyd
check-config` does a live fetch and fails on it. **Alert on that counter.**

**Left for a later branch, deliberately:** the Python and Go clients did not
get the Rust client's `token_provider` callback. Both accept a bearer token
today, which is all a federated deployment strictly needs; what is missing is
automatic renewal, and the callback's idiomatic shape per language plus a stub
identity provider in the conformance harness is its own piece of design.
`docs/clients.md` says so where someone will read it.

**Also folded in, unrelated:** `publish-ghcr.yml` tagged `:latest` with no
guard, so enabling prerelease publishing for a shakeout would have let
`v0.2.0-rc.1` become what everyone pulls. Now gated on the tag having no `-`.
`release.yml` was not touched — it is generated by `dist generate` and CI
fails any PR where the two drift.

---

## As of 2026-08-22 — **release engineering exists; the first tag is the remaining step**

The branch that wrote this section adds the release machinery: pushing a
`v*` tag now builds both binaries for macOS (arm64, x86_64) and Linux
(static musl, arm64 and x86_64 — verified empirically on aarch64 Alpine),
attaches tarballs and SHA256 checksums to a GitHub Release with notes lifted
from the new `CHANGELOG.md`, publishes a Homebrew formula for `kimmy` to
`titusai-io/homebrew-tap`, and pushes a multi-arch image to
`ghcr.io/titusai-io/kimmydb`. `dist` (cargo-dist) generates
`.github/workflows/release.yml` from `dist-workspace.toml` — edit the config,
run `dist generate`, never the YAML; `.github/workflows/publish-ghcr.yml` is
the one hand-written piece, called by dist as a custom publish job. ADR-062
(pre-1.0 SemVer, one workspace version, tests pin the binaries to it) and
ADR-063 (the pipeline) hold the reasoning. Every build now knows its commit:
`kimmyd`'s startup log, `kimmy --version` and `GET /v1/version` all report
version + commit + date, with `unknown` for a build without `.git`.

**Before the first release, three things that cannot be done from this repo:**
create the public `titusai-io/homebrew-tap` repository; add a
`HOMEBREW_TAP_TOKEN` secret (a PAT that can push to the tap) to this repo;
then shake the pipeline out with a prerelease tag (e.g. `v0.1.0-rc.1`) —
dist skips the Homebrew and GHCR publish jobs on prereleases by default, so
the shakeout proves the build half, and the first real tag proves publishing.
Retitle the changelog's `Unreleased` section to the version as part of
tagging.

---

## As of 2026-08-21 — **the local cluster env exists, and driving it found four bugs**

**#85 through #95 are merged** and `main` is clean; the only thing in flight is
the branch that wrote this section, which adds a deviations entry and this
update. The register holds **zero 🔴** and one new 🟡.

The whole run came out of the local-usability direction below: stand the thing
up the way someone would actually use it, then believe the running node over the
documentation. Every bug here was found that way, and none of them by the test
suite.

### What now exists

`docker-compose.local-ai.yml` (#86) layers a fourth container onto the existing
three-node compose file: llama.cpp serving Qwen3-Embedding-0.6B over an
OpenAI-compatible `/v1/embeddings`.

```bash
docker compose -f docker-compose.yml -f docker-compose.local-ai.yml up -d
```

That makes a live embedding endpoint a `compose up` rather than an errand, which
matters because [ADR-047](decisions.md) pins the provider dialects with fixtures
and **nothing in the suite has ever called a real one**. It also carries
`server.advertise` per node — without it the base compose produces a cluster
whose `/v1/topology` is empty on all three, so it looks headless to a client.

### Four bugs, all found by running it

- **An unbounded dial let one dead node stall replication between the live
  ones** (#87). `sync_once` had no timeout on the TCP connect or the TLS
  handshake, and a round walks its peers sequentially — so an unroutable peer
  held the round for the kernel's connect timeout (~127s) and the healthy peer
  behind it waited too. Convergence with one node down: **~240s before, ~6s
  after.**
- **Dropping a collection wedged replication permanently and silently** (#88).
  Replaying a schema change that names a dropped collection raised
  `CollectionNotFound` out of `apply_batch`, failing the whole round; the entry
  never leaves the peer's oplog, so every later round died on it. A freshly
  restarted node failed its *first* round. It was invisible because `peers.rs`
  warned only on the first failure and used `debug!` after — now first, then
  every `WARN_INTERVAL`.
- **A dropped collection left its vectors behind** (#89), and because the shadow
  name is derived from the parent's, a collection recreated with the same name
  adopted them. A document from the dropped collection came back from
  `vector_search` scoring 1.0000, above the new collection's own document, with
  an `_id` resolving to nothing.
- **Every node embedded every document** (#90). The stored result was always
  right — the HLC check makes a losing write a no-op — but the *provider calls*
  were never deduplicated: **23 embedding requests for 10 documents**. The
  writing node now embeds immediately and the others defer `FOREIGN_GRACE` and
  re-check, which is 1:1 with writes spread across all three nodes. The grace
  period ends in doing the work, so a crashed originator cannot cost an
  embedding.

### The CLI is now complete against what a node advertises

Sweeping every command against the cluster (#92–#95) turned up that
`create-collection` was documented idempotent and was not — it returned `409`
and exited 1, which is also wrong after a failover, since `Idempotent` lets the
request be retried elsewhere and the conflict may be raised by the client's own
first attempt. And two capabilities the node advertises had no command at all:
`vector-search`/`hybrid-search` (#93) and index creation (#94), so proving a
query used an index meant leaving the tool for `curl`.

`kimmy` now covers every route a client needs. **Restore is deliberately not
among them** — `kimmyd restore --from` is offline because redb allows one process
to hold a database, and a network client should not pretend otherwise. The round
trip is verified: 671 records restored into an empty data dir and served
standalone, with every collection, index and document body intact.

### Three things worth carrying

- **Memory per node tracks logins, not data.** A node that looks heavy is the
  one the client is pinned to. `Argon2::default()` is the OWASP profile at
  **19 MiB per hash**, and freed memory stays in glibc's arenas, so RSS is a
  high-water mark. Idle floor is 4–10 MB; embedding a dozen documents moves a
  node 1–3 MB. Chasing that as a leak cost a round of false diagnosis.
- **The client is sticky, not round-robin.** `Client::promote` moves whichever
  node answered to the front and leaves it there, so one node serves everything
  until it fails. The documentation claimed round-robin in eleven places (#91);
  the behaviour is deliberate and stays. `docs/decisions.md` still carries the
  original prediction that a client "will, once task 5 lands, round-robin" —
  wrong prediction, unaffected conclusion, left alone because rewriting a
  decision record is a decision.
- **DDL and documents replicate, and a script can outrun them.** Creating a
  collection on one node and writing to another immediately is a clean `404`
  with `retry: no`; deleting a document on a node it has not reached yet is a
  silent `{"deleted": 0}`. Both are correct. Both bit the test harness
  repeatedly — wait for the state you are about to act on, and never discard a
  write's response in a loop.

### Where to look first

`docker-compose.local-ai.yml` for the environment, `crates/kimmy-vector/src/worker.rs`
for the deferral, `crates/kimmy-cluster/src/{transport,health,peers}.rs` for the
two replication fixes. The register's one new 🟡 is `modified` counting writes
rather than changes — see [Deviations](deviations.md).

---

## As of 2026-08-15 — **the direction has changed** (still current)

**M0–M10 are complete; #78–#83 worked the carried debt down afterwards**, and
#85–#95 have followed the direction below. See the section above for what the
most recent run found.

> ### ⚠️ Read this before the roadmap
>
> **The maintainer has redirected the work, on 2026-08-15.** The next thing is
> **not** M11 task 2, and not anything on any board.
>
> > *"I'm not ready to set up a CI/CD pipeline yet for this project. Right now I
> > want to focus on finishing implementation, testing the actual running
> > behaviour of KimmyDB, and getting this running for local use."*
>
> Three instructions in one sentence, and they outrank
> [Roadmap](roadmap.md):
>
> 1. **Do not add CI.** Not a job, not a workflow, not a step. A job was added
>    on #83 and removed again at the maintainer's word — do not re-add it, and
>    do not treat "this property needs guarding" as a reason to. An
>    `#[ignore]`d test listed in [Testing](testing.md) is how an expensive
>    check gets a home here now.
> 2. **Finish implementation and test the real running behaviour** — the
>    running node, not the suite.
> 3. **Get KimmyDB running for local use.** This is the goal to work toward,
>    and it is a different kind of work from the last six branches. See
>    "What to do next" below — it has not been scoped yet, and scoping it is a
>    conversation with the maintainer rather than a reading of the roadmap.
>
> **M11 is paused, not cancelled.** Its board stays in [Roadmap](roadmap.md).
> Task 1 is done; tasks 2–5 are not started and are not the next work.

| | |
|---|---|
| #78 | `retry: wait` fixed (it failed over instead of waiting), `kimmy-auth` + `kimmy-storage` mutation passes |
| #79 | `kimmy-api` mutation pass — closes the M10 mutation debt entirely |
| #80 | `find {_id}` uses the primary key — 7.328 ms → 0.540 ms |
| #81 | An HNSW build could orphan a tenth of a collection |
| #82 | M11 task 1: the daemon commits twice per insert |
| #83 | #81's threshold, re-sized — it counted budget-limited searches as lost data |

### What to do next

**Not yet scoped.** The direction above is clear about the goal and silent
about the shape, so scoping it is the open item:

> What does "running for local use" mean concretely — a single node you keep
> real data in, the Docker image, or the three-node compose setup?

**What exists already, checked rather than assumed.** A first draft of this
section guessed and got two of four wrong, so these are verified:

- **`README.md:40` has a Quick start**, and it is a good one — from source *and*
  Docker, then login, create a collection, insert, a Mongo-style
  filter/sort/projection, configure embeddings, vector search, and a change
  stream over `websocat`.
- **[Operations](operations.md) does not assume building from source** —
  `operations.md:114` is a `docker run -d`.
- **`docker-compose.yml` is a three-node cluster** (`kimmy1`/`2`/`3`, named
  volumes). There is no single-node compose.
- **No image is published.** `ci.yml` builds one with `push: false`, and the
  README's Docker path starts with `docker build -t kimmydb .`, so every route
  to a running node compiles the tree first.

**So the gap is probably not "there are no instructions" — it is that nobody
has run them verbatim from a clean clone recently.** That is exactly what
"testing the actual running behaviour" asks for, and it is the cheapest first
task: follow `README.md:40` literally, on a scratch directory, and record what
happens. Every branch in the last two milestones found something by driving a
real node; none of them started from the documented first-run path.

**Two things that are true and easy to forget** when the goal is "make it
usable": the register in [Deviations](deviations.md) holds **zero 🔴**, and the
verification gate below still applies to every branch. A usability branch is
still a branch — fresh `main`, one task, a PR, and the maintainer merges.

### M11 task 1 — the write gap, and what explaining it turned up

*(Merged as #82, and the branch after it came out of verifying this one — see
"The reachability threshold" below.)*

**The daemon spends two durable commits on an insert where the engine spends
one.** The second is the embedding worker's: `EmbeddingWorker::run` records its
oplog position after *every* entry — including entries it skips, in collections
with no vector configuration — and `put_consumer_position` is its own write
transaction and its own fsync. redb has a single writer, so that commit is not
just extra work on the same disk, it is a queue position in front of the next
write. That is why the penalty looked fixed per request: a request waits behind
at most one in-flight commit however many documents it carried.

Measured rather than reasoned, on a node, through a new `kimmy_commits` metric:

| | inserts/s | ms each | commits per insert |
|---|---:|---:|---:|
| as shipped | 156 | 6.39 | **2.00** |
| embedding worker not started | 226 | 4.42 | **1.00** |
| webhook dispatcher not started | 156 | 6.40 | 2.00 |

The HTTP benchmark's `insert one` cell goes **54 → 236 req/s** with the worker
off. The webhook dispatcher — the other candidate M10 named — is not implicated;
it wakes on a two-second tick. The third candidate, fsync on a runtime worker
thread, cannot explain a gap that is present at one client, where the runtime
has nothing else to schedule.

**The bigger finding is behind the batch API.** The client waits behind one
commit; the worker commits once per *document*. 4,000 documents in 40 bulk
requests took 0.39 s and left the node committing for another **12.3 s** —
4,041 commits. Bulk exists so 100 documents cost one fsync instead of 100, and
the worker pays the 100 anyway, deferred out of view. This happens on every
node whether or not anything uses vectors.

**Three things worth carrying:**

- **`kimmy_commits` is the instrument, and it stays.** Every write transaction
  an open engine takes goes through `Engine::begin_write`, and
  `commits_are_counted_at_one_chokepoint` scans the source so a new write path
  cannot quietly stop being counted. Commits per client-visible write is not
  derivable from a latency figure, which is why this cost survived a whole
  milestone of benchmarking.
- **The first version of the engine benchmark reported +0.2 ms for a cost that
  is +3.1 ms**, because it timed the insert and let the consumer's commit fall
  outside the window. A consumer that lags has not made the write cheaper — it
  has moved the commit somewhere nobody is looking, which is the same thing the
  daemon was doing. The benchmark now ends an iteration when the consumer has
  caught up: 3.51 ms against 6.58 ms, reproducing the gap with no HTTP in it.
- **Nothing tested `run`.** Every other test in `worker.rs` drives `process`
  directly, and the position write is in `run`, not `process`.

**No fix is included, by decision.** Coalescing a consumer's position writes
trades a crash replaying a few idempotent entries for an fsync per write. It
changes the oplog-consumer contract, and it is reserved — see the M11 board. It
must not become "record only when there was work to do": a position that
advances only on work gets killed by retention.

### The reachability threshold — what verifying #82 turned up

**Found by the baseline check, not by a test.** Running
`scripts/bench-baseline.py check` as part of #82's verification reported
`hnsw_build/4000` at **3.00×** and `hnsw_build/2000` at **2.03×**. Neither was
reachable from that diff. 3.00× is exactly `MAX_BUILD_ATTEMPTS + 1` — the
signature of a graph being discarded and rebuilt the maximum number of times,
every single time.

**#81's threshold counted two different things as one.** A probe that fails to
retrieve its own point is either a search that ran out of budget or a point the
graph cannot reach. An ordinary search explores a fixed amount, so the first
grows with collection size and width and the second does not. `3` was sized
against a **400-vector, 16-dimensional** fixture; at 384 dimensions — the
realistic embedding width, the one `recall_holds_at_a_realistic_embedding_width`
exists to cover — misses reach a median of 8 in a sample of 128 at 4,000
vectors. Seven builds in ten were rebuilt twice and then reported to the
operator as losing data. Healthy graphs.

**The fix is the split, not a bigger number.** `Reachability { sampled, missed,
unreachable }`: a miss is re-probed with the budget removed, and only what stays
missing counts. Threshold 8, and `hnsw_build/4000` goes 15,955 ms → 5,354 ms,
1.01× of the pre-#81 baseline.

**Both sides of the gap are measured**, which is what was missing the first two
times. 600 builds at the size where the catastrophic failure occurs, checking
every point rather than sampling:

| | true orphaned | sampled score |
|---|---|---|
| 599 healthy builds | 0.8%–3.0% | 0–5, median 1 |
| the 1 catastrophic build | 14.8% | **22** |

**And a false explanation was corrected.** #81's comment said a healthy miss was
a near-duplicate being edged out — "approximation working as designed". It is
not: with the budget removed those points are still not found. **Every graph
this builds orphans 0.8%–3.0% of a collection**, which is now its own 🟡 in the
register because it was previously believed to be zero. The check has always
been separating routine orphaning from catastrophic orphaning.

**Three constants in one file have now been sized against an assumption.** Both
distributions ship as `#[ignore]`d tests so the next person re-derives them
instead of trusting them, and the regression test runs at 384 dimensions in its
own CI job in release — 21 s there against 146 s in debug, on the cluster
harness's reasoning that a suite doubled to hold one property loses the
property.

### #81, the branch these two came out of — still worth reading

**A test that looked flaky was reporting silent data loss.** About **one HNSW
index build in 250** left **10–24% of a collection unreachable** from the graph:
those documents were returned by no vector search, at any `k`, for any query,
with nothing failing and nothing logged.

The evidence chain, because it is the model for this kind of work:

- 1,500 builds → **bimodal**: median 0.995, ~1 in 250 collapsing to **0.545**.
- In a collapsed build **19 of 20 queries degrade together** — a global
  property, not one hard query.
- Asking every vector to retrieve **itself** found it: healthy builds miss 2 of
  400, bad builds miss **40–96**.
- Six candidates eliminated by measurement — SIMD (`simdeez_f` is off), core
  count (rayon is not on this path), `Hnsw::new` argument order, graph height,
  `keep_pruned`, and `ef`. **The `ef` plateau proved it**: 5/800 at ef=50,
  2/800 at ef=100, **2/800 again at ef=200**. More exploration cannot cross into
  a disconnected component.

**The fix is at build time**: `HnswIndex::build` asks a sample of stored vectors
to retrieve themselves and rebuilds a graph that cannot. Min recall **0.5350 →
0.9600**, zero failures in 1,500 builds. The recall test is restored
**unchanged** — fixing the defect made the original assertion honest rather than
lenient.

**Three of my own mistakes were caught during it**, and all three came from the
same root — choosing a number without knowing the shape of what it bounds:

- The first fix averaged three graphs. It would have **hidden the bug** and
  failed CI *more* often (a mean draws three chances at a bad graph).
- The retry trigger `misses > 0` was a **~3× build-cost regression**, invisible
  to any recall measurement because every rebuild is fine and only wasted work
  differs.
- A test I nearly shipped was itself flaky — threshold set from one observation.

Both surviving constants are now sized from measured distributions with the
arithmetic in the code.

**Operational consequence, still open:** the check runs at build time and does
**not** repair a graph already cached in a running process or persisted under
`<data_dir>/hnsw`. [Operations](operations.md) has rebuild instructions. Any
node with real vector data written before 2026-08-15 should be rebuilt.

**Sharding is the one thing still deliberately deferred — do not re-open it
unasked.** Replicated-not-partitioned is the current position and is considered
correct for now; the maintainer wants to decide from experience of running
KimmyDB, not from a feature comparison. The *client story* half of that old
deferral was settled by [ADR-055](decisions.md) and is what M10 carries out.

### Getting oriented

#### 1. Find out where the tree actually is

```bash
cd /path/to/kimmydb
git checkout main && git pull
gh pr list --state open            # expected: nothing
git log --oneline -1               # expected: the #83 merge, or later
```

**As of this writing there is nothing open and nothing in flight.** If that is
still true, the next work is **not on any board** — read the boxed note at the
top of this file, then "What to do next". The short version: the maintainer
wants local usability and real running behaviour, not the next milestone task,
and **adding CI is specifically ruled out**.

**If something *is* open**, that branch is the work and it is waiting on review.

**Either way, check the register first.** `docs/deviations.md` holds zero 🔴.
If a 🟡 has become a 🔴 in your absence, that outranks the plan.

#### 2. What this project is, in one paragraph

KimmyDB is a document and vector database in Rust — MongoDB-shaped query
surface, leaderless replication, HNSW vector search with server-side embedding,
change streams, webhooks, and a full HTTP/WebSocket protocol. Eleven crates in
one workspace, three first-party clients (Rust, Python, Go), 1,131 tests, and
a specification (`docs/openapi.yaml`) that a contract test holds to the running
server. **[The oplog is the spine](#the-one-structural-idea-if-you-read-nothing-else)**
— read that section, because nearly every subsystem is a consumer of one log
and the design only makes sense once that lands.

#### 3. Verify the tree is healthy before you believe anything

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
./scripts/check-native-deps.sh          # must report `cc` alone, nothing more
cargo test --workspace                  # ~2 minutes, 1,131 tests
```

Then, if the change is anything but documentation, **drive a real node** — the
live drive has caught what the suite could not on nearly every branch. "How to
run and verify things" below has the recipes, including the cluster harness,
the three client suites, the conformance runner and the examples.

#### 4. What is already decided — do not re-open these unasked

- **Sharding.** Replicated-not-partitioned is the position, deliberately, until
  there is operational experience. Not an oversight.
- **The 60 ADRs in [Decisions](decisions.md).** M10 added ADR-056 through
  ADR-060: the hand-written specification, the three-valued retry taxonomy,
  path-major versioning with capability discovery, sliding token re-issue, and
  the replicated node registry. Each was put to the maintainer first.
- **The register in [Deviations](deviations.md)** holds **zero 🔴**. Everything
  open is a 🟡 that was agreed. Read the register itself, not a summary — M8
  found a debt that existed only there.

#### 5. The four failure modes that keep catching people here

**A claim with no mechanism behind it is usually false.** M8 found one in four
branches, M9 in every task, M10 in four more — including a sentence in a PR
description that turned out to describe a system that does not exist, and a
specification paragraph about creating a collection twice that no test had ever
done. When you find an assertion in a comment, a doc or a spec, the useful
question is "what fails if this stops being true?" If the answer is "nothing",
that is the next bug.

**Transport-free tests agree with each other and lie about clusters.** Three
separate defects passed every single-node test and were only ever visible on
real nodes — most recently a replicated schema change that was appended to the
oplog and never published, so a dropped collection ended its own node's
watchers and left every other node's hanging. Use `cargo test -p kimmyd --test
cluster -- --ignored --test-threads=1`.

**A test that looks flaky may be reporting a real defect.** The HNSW recall test
failed CI once at 0.865 against a 0.90 bar, on a branch that could not reach
that crate, and the obvious reading was that a randomised structure makes a hard
threshold unreliable. It was reporting **silent data loss**: about one index
build in 250 orphaned 10–24% of a collection, so those documents were returned
by no search at any `k`. The first fix averaged three graphs to quiet it — which
would have hidden the bug *and* failed more often, since a mean of three draws
three chances at a bad graph. **Before making a test quieter, find out what it
is telling you**, and be sure any threshold is sized from a measured
distribution rather than from one observation. Both mistakes here came from the
latter.

**A cost outside the measurement window is a cost nobody sees.** The write gap
survived a whole milestone of benchmarking because the second fsync per write
belonged to a background consumer and landed *after* the response — so every
latency figure was honest and every one of them missed it. The first benchmark
written to catch it made the same mistake a second time, reporting +0.2 ms for
a cost of +3.1 ms, because it stopped the clock when the insert returned. Ask
what work a request *causes* as well as what it *does*: `kimmy_commits` over
`kimmy_requests_total` is the version of that question this project can now
answer from a running node.

#### 6. What M11 is, and why it is not what you should start

**M11 is paused.** The maintainer redirected the work on 2026-08-15 toward local
usability and real running behaviour — see the boxed note at the top of this
file. This section is here so the milestone is not lost, **not** as a to-do
list.

**M11 is index-ordered scans**, chosen by the maintainer on 2026-08-14. The
board is in [Roadmap](roadmap.md#m11--index-ordered-scans) — five tasks, of
which **task 1 is done** (the write gap, explained above). Tasks 2–4 are
engine-side and unblocked; task 5 reaches the wire and has a reserved decision.

**The remaining work, and the shape is known:**

- `kimmy-storage/src/index.rs` — both `scan_range_in` and `scan_range_in_write`
  end `out.sort(); out.dedup();`. **The sort is not gratuitous**: `dedup` only
  removes *adjacent* duplicates, and duplicates are real (a multikey index
  stores one document under several keys; a `$in` union produces several
  ranges). Removing it needs a seen-set that preserves first-occurrence order.
- `kimmy-query/src/plan.rs` — `IndexPlan` has **no notion of which order it
  yields**. That is the extension point.
- `kimmy-api/src/exec.rs:166` — `stop_after` is only set when the sort is
  empty, which is why every sorted `find` materialises its whole match set; and
  `:152` refuses any cursor sort but `{"_id": 1}`.

**Lifting that refusal is the prize, and it reaches the wire**: a sorted
cursor's token must carry the index key *plus* the document key, which touches
`kimmy-core/src/cursor.rs`, `docs/openapi.yaml`, all three clients and the
conformance scenarios. Expect a reserved decision on the token shape before
writing code — it is public surface and must stay additive under
[ADR-058](decisions.md).

The other material considered and not chosen is still in "How to size the next
thing after M10" below.

### How work runs here — read this first

The rhythm:

- **One branch per task, always off fresh `main`.** `git checkout main &&
  git pull && git checkout -b m10-<task>`.
- **Every branch lands through a PR (`gh pr create`)**, which the maintainer
  reviews and merges. Pushing a branch is not finishing it; the PR is.
- **The gate, before every commit:** `cargo fmt --all -- --check` ·
  `cargo clippy --workspace --all-targets -- -D warnings` ·
  `./scripts/check-native-deps.sh` · `cargo test --workspace`. Then **drive
  the change against a running node** — the live drive has caught what the
  suite could not on nearly every branch (empty webhook fields, the reindex
  that embedded nothing, the `open_ai` wire tag). CI additionally runs the
  cluster harness (`cargo test -p kimmyd --test cluster -- --ignored
  --test-threads=1`).
- **Every deviation from plan gets a `docs/deviations.md` entry at the time
  it is made**, 🔴 (open drift) / 🟡 (agreed deferral) / 🟢 (superseded/closed).
  Design decisions get an ADR in `docs/decisions.md` (next number: **ADR-062**).
  M8 task 7 found the register can silently lose an entry — this file's debt
  table pointed at a 🟡 for bulk insert that had never been written down.
  Check the register itself, not the summary of it.
- **CI caches are written only from `main`.** A cache is scoped to the ref that
  wrote it and a PR can already read the base branch's, so `save-if` keeps PR
  branches from writing byte-identical copies. Before this was fixed the repo
  sat at the 10 GiB ceiling with 87% duplicates, and eviction was removing
  *main's* caches to make room for copies of them. `cache-cleanup.yml` deletes
  a PR's caches when it closes. Do not remove `save-if` to "make CI faster".
- **Do not add to CI.** As of 2026-08-15 the maintainer is not investing in the
  pipeline: *"I'm not ready to set up a CI/CD pipeline yet for this project."*
  The six jobs that exist keep running; adding a seventh does not. A job was
  added on #83 to run an expensive check and removed again in the next commit —
  **an expensive check gets an `#[ignore]` and a line in [Testing](testing.md),
  not a workflow.**

### The one structural idea, if you read nothing else

**The oplog is the spine.** Every mutation appends exactly one durable,
HLC-ordered entry *in the same redb transaction as the change itself*. Four
independent subsystems consume that same log: change streams (WebSocket, and
resumable by token), the embedding worker, cluster anti-entropy, and the
webhook dispatcher. Two consequences that keep mattering:

- A new background consumer is "subscribe to the log", not new machinery —
  and backfill is not a special case, the consumer just starts earlier.
- **Consumers record their position after doing the work, never before.**
  Crashing replays the entry; idempotency makes the replay a no-op. Recording
  first silently skips work — the failure mode behind three separate bugs so
  far.

This is why single-instance change streams work here when they don't in
MongoDB: there the log is a byproduct of replication, so it needs a replica
set. Here clustering is a *consumer* of the log, not its cause.
[Architecture](architecture.md) and [Oplog](oplog.md) have the detail.

### State of the branches

| | |
|---|---|
| `main` | PRs #16–#63 merged: all of M8 and all of M9, SWIM authentication (ADR-053), the witnessed vector (ADR-054), ADR-055 and the M10 board |
| `m10-protocol-spec` | ✅ Merged as #65. M10 task 1: `docs/openapi.yaml`, its contract test, ADR-056 |
| `m10-error-taxonomy` | ✅ Merged as #66. M10 task 2: `ErrorCode` as an enum, the three-valued retry class, `JsonBody`, ADR-057 |
| `m10-versioning` | ✅ Merged as #67. M10 task 3: `docs/compatibility.md`, `GET /v1/version`, the `Capability` enum, ADR-058 |
| `m10-token-refresh` | ✅ Merged as #68. M10 task 4: `POST /v1/auth/refresh`, `expiresIn`, ADR-059 |
| `m10-topology` | ✅ Merged as #69. M10 task 5: `GET /v1/topology`, the node registry, `server.advertise`, ADR-060 |
| `m10-protocol-cursors` | ✅ Merged as #70. M10 task 6: the paging contract, and cross-node paging in the harness |
| `m10-http-bench` | ✅ Merged as #71. M10 task 7: the HTTP benchmark, and the numbers in [Benchmarks](benchmarks.md) |
| `m10-rust-client` | ✅ Merged as #72. M10 task 8: `kimmy-client`, the CLI converted, [Clients](clients.md) |
| `m10-python-client` | ✅ Merged as #73. M10 task 9: `clients/python`, the `kimmydb` package, a CI job |
| `drop-invalidates-change-streams` | ✅ Merged as #74. Three change-stream defects: the drop invalidate, the unpublished replicated DDL, and a recreated collection serving the dead one's history |
| `m10-go-client` | ✅ Merged as #75. M10 task 10: `clients/go`, package `kimmydb`, a CI job |
| `m10-conformance` | ✅ Merged as #76. M10 task 11: `clients/conformance`, three drivers, a CI job |
| `m10-examples-closeout` | ✅ Merged as #77. M10 task 12: `shelf` in three languages, the client mutation pass, the closeout |
| `close-m10-test-debts` | ✅ Merged as #78. `retry: wait` **failed over instead of waiting** — the comment above it said otherwise and nothing tested it. Plus the `kimmy-auth` and `kimmy-storage` mutation passes |
| `m10-api-mutation-pass` | ✅ Merged as #79. `kimmy-api`'s 76 mutants; closes the M10 mutation debt. Found `capabilities()` could return an empty list with every assertion still passing |
| `find-by-id-uses-the-primary-key` | ✅ Merged as #80. `plan::choose_primary_key`: 7.328 ms → 0.540 ms over 10k documents, examined 10,000 → 1 |
| `recall-test-does-not-depend-on-core-count` | ✅ Merged as #81. Badly named — it is the HNSW build fix. An index build could orphan 10–24% of a collection |
| `m11-write-gap` | ✅ Merged as #82. M11 task 1: the daemon spends two commits per insert where the engine spends one. `kimmy_commits` on `/metrics`, and the tests that hold both numbers |
| `hnsw-reachability-does-not-depend-on-collection-size` | ✅ Merged as #83. #81's threshold, re-sized. It counted budget-limited searches as lost data, so at a realistic embedding width every large build was rebuilt twice and reported as incomplete (ADR-061). A CI job added here was removed again before merge — see the note at the top of this file |
| `jwt-secret-required-when-auth-is-on` | 🔵 **PR open, awaiting review.** An auth-on node with no `KIMMY_JWT_SECRET` fell back to a signing key that ships in the source, so a `root` token could be forged with no login. Now refused at startup for a single node, not just a cluster. Confirmed by forging a token against a real node before and after the fix |

### The M8 and M9 boards — all seventeen done

M8 (twelve tasks, PRs #41–#51 plus the closeout) built the cluster harness,
observability, benchmarks, HNSW snapshots, vector reindex, the provider
dialects, bulk insert, certificate reload, SRV discovery, webhook ownership by
node id and token revocation. Its ADRs are 046–052 and its lessons are in the
invariants below; the per-task narrative was retired from this file once the
milestone was two behind. [Decisions](decisions.md) and
[Deviations](deviations.md) hold the record.

| # | M9 task | |
|---|---|---|
| 1 | Computed expressions + `$addFields`/`$set` + `$replaceRoot` | ✅ #57 |
| 2 | TTL / expiring documents | ✅ #58 |
| 3 | `findAndModify` | ✅ #59 |
| 4 | Partial indexes | ✅ #61 |
| 5 | Cursors / efficient pagination | ✅ #63 |
| — | `update`/`delete` use the planner (drift found during task 3) | ✅ #60 |
| — | CI cache hygiene | ✅ #62 |

**M9 wrote no ADRs.** Every decision was recorded as a 🟢 entry in
[Deviations](deviations.md) instead, because each was a feature decision with
its reasoning rather than an architectural choice with alternatives. **M10 in
turn wrote five** — ADR-056 through ADR-060 — because each of its reserved
decisions had real alternatives to weigh rather than being a feature choice
with a reason. Next ADR number is **ADR-062**.

### What the M9 branches did, and what they found

Read this before touching the query engine or the index path.

- **Task 1 — computed expressions (#57).** `kimmy-query/src/expr.rs` is a
  recursive tree: arithmetic, strings, conditionals, comparison, boolean, date
  parts, `$dateToString`, and `$literal`. Because `$group`'s `_id` and every
  accumulator argument were *already* typed as `Expr`, they gained the whole
  operator set by construction. **Found a latent `$sum` bug**: `finish()`
  carried an `all_int` flag and a comment citing ADR-002 about not losing
  precision above 2^53, while accumulating in `f64` and casting back — so a sum
  of 2^53+1 and 1 returned 9007199254740992. The comment was right and the code
  was wrong and nothing failed when they disagreed. **Deliberate behaviour
  change**: a document-valued expression now computes, so `{_id: {c: "$city"}}`
  groups by city where it used to put every input in one bucket. `$literal` was
  added beyond the agreed operator list because the list is incomplete without
  it — once `$`-strings are field paths and documents are expressions, there
  must be a way to produce the literal string `"$city"`.
- **Task 2 — TTL (#58).** A TTL index (`expireAfterSeconds` on `IndexMeta`),
  not a collection setting: the pass must *find* expired documents every tick,
  and an index range scan costs ~1.66 µs per expired document against ~8 ms per
  pass for a 10k collection scan. **One node expires a given collection**, by
  rendezvous hashing through `kimmy-api/src/expiry.rs` — every node expiring
  independently converges but produces N deletes per document. An expiry is an
  ordinary `OpKind::Delete`, because `op_kind_from_tag` rejects an unknown tag
  as *corruption* and a new variant would be a stop-the-cluster upgrade.
  **The near-miss worth knowing**: marking an expiry by putting a payload in the
  delete's body looks harmless, and `apply_remote` decodes any delete carrying a
  body as a **live document** — every marked expiry would have resurrected
  itself on every node. `kimmy_ttl_expired_total` exists so "one document, one
  delete" is measured; the cluster harness sums it across three nodes and
  requires exactly 1.
- **Task 3 — `findAndModify` (#59).** One `/find_and_modify` route, and **the
  match happens inside the write transaction**: redb has a single writer, so a
  match found inside the write cannot be taken before the commit. Atomic by
  construction, no retry loop. `MAX_CANDIDATES` (10,000) bounds the writer hold
  as a *refusal*, since choosing from a prefix would return a document the sort
  did not pick. **The crate boundary held and improved the design**:
  `kimmy-query` is a dev-only dependency of `kimmy-storage`, so query semantics
  arrive as `ModifySpec` — pure functions the engine calls inside its
  transaction, the same shape `delete_guarded` took for TTL.
- **Task 4 — partial indexes (#61).** Partial only; a sparse index is
  `{field: {$exists: true}}`. The filter language in `kimmy-core/src/partial.rs`
  is **deliberately bounded** — `$exists: true`, equality, four comparisons,
  conjunction — because general implication between filters is undecidable, and
  a best-effort containment check returns a *subset* with nothing to indicate
  it. The refusal lands at index creation. **Three things were verified rather
  than assumed**, each of which would have been silent: `bson::Document`
  round-trips losslessly through the metadata JSON as canonical Extended JSON
  (checked with a date and 2^53+1); a comparison never matches an absent field,
  which is what makes `$gt`/`$lt` safe to treat as proving existence, **except
  `{a: null}` which matches missing fields** and so contributes nothing to
  containment; and `impl Eq for Bson` exists despite the `f64`, which is what
  lets `IndexMeta` keep its derives.
- **Task 5 — cursors (#63).** An opaque token that is *the encoded document key
  of the page's last row and nothing else*. `keyenc` is order-preserving, so
  byte order is `_id` order and "next page" is a range bound storage already
  takes — no new comparison logic, no sorting, no server state, and node
  portability falls out because the token is a pure function of the `_id`.
  Measured: 100,000 documents walked in 1001 pages in 1.09 s, flat ~1 ms per
  page, against `skip` growing to 89 ms by page 500. **A design fault was found
  by writing the test**: the first draft returned `nextCursor` only when a
  cursor had been *sent*, so the test needed a magic first-page constant that no
  client could have discovered. `nextCursor` is now offered whenever the page
  filled and the query is one a cursor can continue.
- **The drift (#60).** `exec::update` and `exec::delete` never called
  `plan::choose` — they used `for_each_doc`, a straight collection scan, so an
  index never sped up a filtered update or delete. The register *had* said
  "found by a scan", but inside a row about atomicity where it read as being
  about partial application. Both now go through `collect_matching`, and both
  take `explain`. Matching alone on 20,000 documents: 17.09 ms → 0.61 ms.
  A second bug went with it — `update` counted `modified` by incrementing per
  target rather than reading the write's answer.

**The pattern across all five:** every task turned up something the code
claimed but did not do, and in four of five cases the claim was in a comment
sitting directly above the code contradicting it. Read the comment, then check
the code does what it says.

### The M10 board — all twelve done

**Theme: KimmyDB has a protocol and has never written it down.** HTTP framing,
Extended JSON v2, bearer tokens, a typed error envelope and WebSocket streaming
answer every question a wire protocol has to answer. Nothing specifies,
versions or tests it — so every client would be hand-written and nothing fails
when a route drifts. [ADR-055](decisions.md) settles the direction; this is the
work.

| # | Task | Reserved decision to settle first |
|---|---|---|
| 1 | ✅ **Protocol specification** (OpenAPI 3.1) + a contract test | Settled: hand-written, checked by inventory *and* live responses (ADR-056) |
| 2 | ✅ **Error taxonomy as public surface** | Settled: three-valued `no`/`wait`/`elsewhere`, closed by an enum (ADR-057) |
| 3 | ✅ **Versioning and compatibility policy** | Settled: path-major, `/v1` never breaks, capabilities over version numbers (ADR-058) |
| 4 | ✅ **Token refresh** | Settled: sliding re-issue, no grace for an expired token (ADR-059) |
| 5 | ✅ **Client-visible topology / discovery** | Settled: replicated registry for addresses, SWIM for liveness (ADR-060) |
| 6 | ✅ **Cursors at the protocol level** | — (M9 settled the shape; this settled the wire contract) |
| 7 | ✅ **HTTP-level benchmark harness** | — |
| 8 | ✅ **Rust client**, `kimmy-client`; the CLI is its consumer | Settled: `reqwest`, already in the tree |
| 9 | ✅ **Python client** | Settled: `httpx` + `websockets`, sync first |
| 10 | ✅ **Go client** | Settled: stdlib `net/http` + `coder/websocket` |
| 11 | ✅ **Conformance suite all three clients pass** | — |
| 12 | ✅ **One example app per language** + mutation pass and closeout | — |

**All twelve landed, in twelve PRs (#65–#77) plus one unplanned fix (#74).**
Task 12 is the only one not yet merged. The protocol is now written down
(`docs/openapi.yaml`), versioned, and held to the running server by a contract
test — so a route that drifts fails a build instead of silently breaking a
client.

### What task 1 did, and what it found

`docs/openapi.yaml` describes every route the router registers — the auth flow,
Extended JSON v2, the error envelope, cursors, and the WebSocket frame shapes
that OpenAPI has nowhere else to put. `/mcp` is deliberately absent: a
different protocol, config-gated, specified by MCP itself.

**The document is not the deliverable; `crates/kimmy-api/tests/openapi.rs` is.**
It checks inventory both directions, then drives every documented operation
against a real server and validates each response against the declared schema,
and **ends by asserting every documented operation was exercised** — so a route
cannot be added to the router and the spec without also being driven. That
last assertion is what every later task pays: changing the wire now means
editing the document and the scenario together.

Three findings, all from the live half, all in the same shape as M9's:

- **`PUT /docs/{id}` returned `matched` and `modified` as booleans** while
  `/update` and `/find_and_modify` return them as counts — one field name,
  two types, on one protocol, because the route serialized `WriteOutcome`'s
  bools straight through. **The route had no integration test at all**, and
  this file described it as `{"matched": 0}`, which was wrong in a way nothing
  could contradict. Normalized to counts by the maintainer's decision, at the
  cheapest possible moment: no client exists and nothing in-tree reads them.
  `upserted` stays a boolean because it is one.
- **`GET /v1/users` returns names, not user objects** — the spec's first draft
  was written from the handler rather than the store.
- **The M8 inventory test had a hole.** It matched `.route("` at the start of a
  line, so it skipped the three registrations rustfmt breaks across lines,
  including `/docs/{id}`. Green while never checking the busiest route on the
  API. There is one scanner now, in the new test, and it covers `http-api.md`
  too.

**Verified beyond the suite**, as every branch is: a release node on port 7911,
driven by a Python script that parses `docs/openapi.yaml` itself and checks 32
real responses against it — a second reader, independently implemented, of the
same document. It also walked a two-page cursor and confirmed the three
`PUT` shapes on the wire. No disagreements, and the node log was clean.

### What task 2 did, and what it found

The seventeen error codes are a closed set: `ErrorCode` is an enum, and both the
wire string and the **retry class** come from exhaustive matches on it, so a new
code does not compile until both are answered. The class is `no`, `wait` or
`elsewhere`, and it **rides in the envelope** — `{"error", "message", "retry"}`
— so a client acts on it without a table of codes compiled at release time.
That is what has to be true for task 3 to call "adding a code" additive.

`elsewhere` exists because this is a leaderless cluster. `internal`,
`misconfigured` and `snapshot` are conditions of the node that answered, and a
peer holds the same data; a boolean `retryable` would tell a client to retry the
machine that just failed it. [ADR-057](decisions.md) has the full division.

Six findings, and **four of them came from the live drive, not the suite**:

- **`no_vectors` was in neither document** — the enumeration's first catch, and
  the reason the roadmap's "enumerate it" had to be exhaustive over the code
  rather than over `error.rs`.
- **422 was in the prose reference and specified nowhere.** Seventeen
  operations can return it; all seventeen declare it now.
- **Sixteen routes were outside the taxonomy entirely.** Axum's body rejection
  is bare text, and the M5 mapping that fixes it is only reached by a handler
  taking `Result<Json<T>, JsonRejection>` — **one handler of nineteen did**. It
  now lives in an extractor, `json::JsonBody<T>`, which cannot be used without
  it. The conformance test had only ever driven a wrong-shaped body against
  `/bulk`, the one route that was right.
- **`/watch` refused non-upgrade requests with no envelope at all**, same
  reason, same fix.
- **The specification had the OpenAI provider tag as `openai`.** It is
  `open_ai`; `openai` is the *display* name from `ProviderConfig::name()`, and
  the spec was written from it. This project had already been caught by that
  exact distinction once.
- **`no` is not a string in YAML 1.1.** `enum: [no, wait, elsewhere]` reads as
  `[False, "wait", "elsewhere"]` in PyYAML and as the string in Rust's reader,
  so two readers of the specification disagreed about a value that goes on the
  wire. Quoted now. Nothing inside the Rust test could have seen it — this is
  the argument for the drive being an *independently written* second reader,
  not a rerun of the same logic.

**Verified beyond the suite**: two release nodes, two scripts. The task 1 drive
re-run unchanged (32 responses, no disagreements) as a regression against the
new envelope, and a second that parses the retry table out of the specification
and checks it against the wire — seven codes observed, including `misconfigured`
as `elsewhere`, provoked by pointing a collection at an API key environment
variable the node does not have.

### What task 3 did, and the shape it sets for tasks 4–12

[Compatibility](compatibility.md) is the policy: the path carries the major,
`/v1` does not break, additive changes ship in it without ceremony, and
anything breaking mints `/v2` served alongside for at least one minor line and
six months. Date-versioned requests were rejected — a shim layer per released
version has to be *exercised* to mean anything, and that is a permanent tax
taken on before the first client exists.

**`GET /v1/version` is the load-bearing half, and it advertises capabilities
rather than a number.** Nodes are upgraded one at a time, so a client that
round-robins meets nodes of different ages; "does this node have the feature I
am about to use" is not a question a version number answers unless the client
also carries a version→feature table, which is the table that goes stale in
every client independently. `Capability` is an enum checked against the
specification, like `ErrorCode`.

**Every client-visible feature from here on owes a capability**, and tasks 4
and 5 are the first two — refresh and topology are exactly the things a client
must detect rather than assume.

**Four claims became mechanism**, which is the part worth keeping:

- Every versioned route is under `/v1/`, and the prefix agrees with the
  server's reported protocol *and* `info.version` in the specification.
- The advertised capabilities are the documented ones, each with an
  explanation rather than only a name.
- **No response schema forbids unknown properties.** Without it, "a new
  response field is additive" is false for any client that validates — it would
  break on the next field added, silently, and only for them.
- The default build must not advertise `local-embeddings`, which is what proves
  the list is answered per build rather than asserted.

**Two things stay prose and are marked as such** in the policy: the six-month
window, which is a promise about calendar time, and "changing what a route
means is breaking", which nothing reading shapes can detect. Naming them is the
point — the failure this milestone keeps finding is a claim that reads like a
mechanism and is not one.

### What task 4 did

`POST /v1/auth/refresh` exchanges a valid token for a fresh one — **sliding
re-issue, not a second credential**. Nothing new for a client to store, no
second lifetime, nothing kept server-side. A stored rotating refresh token was
rejected for an architectural reason worth remembering: rotation is a
compare-and-set on a replicated record, which a leaderless store does not
offer, and two concurrent rotations resolve by last-writer-wins — discarding a
credential a client is holding ([ADR-059](decisions.md)).

**The security half is structural rather than written.** The route takes the
`Auth` extractor, so a token whose account was deleted, disabled, or had its
password or grants changed is refused *before the handler runs*. Refresh cannot
launder a revoked session by construction, not by remembering to check — which
matters, because the remembering version is one deleted line away from being an
indefinite session-laundering endpoint.

**Three deliberate non-features**, all now written down and tested:

- The old token keeps working until it expires. A stateless token cannot be
  recalled; ending a session early is what the version bump is for.
- A grant change stops refresh along with everything else, because grants live
  in the token. That is the cost of carrying them rather than looking them up.
- No grace for an expired token, so `exp` means one thing on every route. A
  client idle past the lifetime logs in again — a thing a library may ask of an
  application, where re-sending credentials hourly is not.

**`expiresIn` is reported at login and refresh**, so a client never decodes a
token it is told to treat as opaque. It is also the first spend of the
compatibility promise written one task earlier: a new response field, shipped
in `/v1` without ceremony.

### What task 5 did — the one with distributed-systems risk

`GET /v1/topology` lists the cluster. **Addresses come from a replicated
registry** each node writes itself into; **liveness comes from SWIM**. Two
sources because neither answers alone, and because the single-source version
was not available: `Member` carries the *gossip* address and is postcard-
encoded, so putting a client address in it is a stop-the-cluster upgrade — the
fourth in three milestones — and inferring one from the gossip address is a
guess that breaks on separate interfaces, TLS termination and container
networking ([ADR-060](decisions.md)).

**Both inherited traps were handled on purpose, not survived by luck:**

- `Members` holds **peers only**, so the answering node is added explicitly and
  listed first. A list derived from membership alone would tell a client the
  cluster does not include the node it is talking to.
- The set holds **only authenticated peers** (ADR-053), and that invariant now
  guards something new: an unauthenticated peer would be advertised to clients
  as a node to send credentials to.

**`status` is `live` or `unknown`, never `down`**, because a node whose gossip
is partitioned while its HTTP works is a real state here. Hiding it removes an
option exactly when a client wants one.

**The registry is an address book, not a heartbeat** — written at startup and
only when the content changes, so an idle cluster appends nothing. Freshness of
liveness is SWIM's job.

**Verified in the cluster harness**, which is the only thing that could verify
it: three real nodes, each listing all three as `live` with real addresses —
including a node whose seed list never named it, which only replication
explains — then a token from one node used at every advertised address, then a
node killed and required to read `unknown` rather than vanish. All six harness
tests pass.

### What task 6 did

M9 built cursors and documented them well — as *engine* behaviour. The wire
carried three claims nothing checked, and the specification said nothing about
page size at all.

**Node portability is now tested, not argued.** A harness test walks a
collection across three nodes, changing node every page, and requires the walk
to see every document exactly once and in order. The claim was sound — a token
is a pure function of the `_id` — but the protocol now *tells* clients to
round-robin, so paging that broke when they did would be a data bug reached by
following the protocol's own advice.

**Two silent behaviours were in no specification**, both of the kind a client
author meets in production:

- **An unlimited `find` returns 100 documents, not all of them.** The prose
  said so; the machine-readable document a client is generated from did not.
  `count` has no cap and is the honest source for a total.
- **A `limit` over 10,000 is clamped rather than refused** — the request
  succeeds and returns less than was asked for.

**And one real trap, now stated and tested:** a final page that is exactly full
still carries a token, because the server cannot know it is the last without
looking further. **A client ends its walk on a short or empty page, not on a
token no longer being offered.**

**One property is documented rather than enforced, on purpose.** A token is a
*position*, not a query: sent with a different filter it resumes that filter
after the same key. Enforcing it would mean putting the query inside the token,
which makes it large and gives clients structure to depend on in something they
are told to treat as opaque.

### What task 7 did, and the one thing it could not explain

`cargo bench -p kimmyd --bench http` spawns the **shipped binary** and drives it
with concurrent clients over a real socket — plaintext and TLS, reads and
writes, warm-up discarded, percentiles rather than a mean. Not a Criterion
bench: throughput under contention is not a shape Criterion measures. Recorded,
not gated, like everything else in [Benchmarks](benchmarks.md).

- **TLS is close to free** — within noise at one client, ~10% at thirty-two.
- **The protocol costs ~0.1 ms per request**, measured as a point read's p50.
- **Reads scale, writes do not**: 8,001/s → 70,660/s against 143/s → 602/s,
  with write p99 going 10 ms → 246 ms. One redb writer, experienced by a client
  as tail latency.
- **`count` is a collection scan** at 30/s over 10,000 documents, and barely
  scales. Worth knowing before a client polls one.

**And a gap it could not explain.** A single insert is 7.0 ms over HTTP against
~3.4 ms at the engine. It is *not* protocol overhead — the read numbers bound
that at 0.1 ms — and not per-document encoding, since the gap is fixed per
request rather than growing with batch size. Candidates: the background oplog
consumers a daemon runs and a bare `Engine` does not, or a commit's fsync on a
runtime worker thread. **Recorded as a 🟡 question**, because a cause that has
not been measured is not a cause. Halving it would double single-write
throughput for every client that does not batch.

### What task 8 did — the first client, and what it found

`kimmy-client` holds a token and refreshes it, fails over between nodes
discovered from `/v1/topology`, pages with cursors, returns typed errors
carrying the retry class, and resumes change streams. Each of those is a server
promise from an earlier task, which is what tasks 1–7 were for.

**It depends on no `kimmy-*` crate, and a test keeps it that way.** That is the
property that makes it a *check* rather than only a convenience: it sees what
the Python and Go clients will see. A shared type would make this the one
client that works for a reason the others cannot have.

**Retries are deliberately conservative.** A read moves to another node on
`elsewhere`; a write does not, because `elsewhere` says *this node* did not
answer, not that the work did not happen — and no status distinguishes an
insert that failed before its commit from one that failed after. A caller who
knows the request is idempotent says so, and an insert carrying its own `_id`
qualifies while one without does not.

**Converting the CLI found three defects**, which is exactly why the roadmap
made it the first consumer:

- **`Client::request` took a `reqwest::Method`**, putting the HTTP stack in the
  public API — every consumer had to depend on `reqwest` to name a verb. The
  crate has its own `Method` now.
- **Login did not fail over.** It tried only the first endpoint, so a client
  handed a list whose first address was dead could not authenticate at all —
  the one failure that makes every other endpoint useless. Found by a test
  putting a dead address in front of a live one.
- **The CLI could not create a collection.** On a fresh database the first
  `insert` failed with "collection not found" and offered nowhere to go but
  `curl`. Found by driving the converted binary.

`kimmy watch` and `kimmy topology` came along with the conversion, because the
client made them a few lines each.

### What task 9 did, and the two things it found

`clients/python`, package `kimmydb`, synchronous, on `httpx` and `websockets`.
Both were chosen for the same reason: each has a sync *and* an async API behind
nearly the same surface, so "sync first" costs nothing when async is wanted.
The stdlib was rejected for a measured reason rather than a taste — `urllib`
opens a connection per request, and against a ~0.1 ms request a handshake per
call would dominate everything.

**It shares no code with the Rust client, and passes the same scenario list.**
That is the arrangement task 11 depends on: two independent readers of one
specification, so a disagreement between them means something.

**The surface is Python rather than a translation** — iteration where a Python
caller expects it, `documents()` for the shape most callers want, exceptions
rather than returned errors, and `.code` as a plain string because codes are
additive.

Two findings:

- **A lazy change stream misses events.** Python's natural shape is a
  generator, which would open the socket at the first `next()` — so anything
  written between `watch()` and that read is lost silently. It connects when it
  is asked for now. Found by a test that hung for ten minutes.
- **A dropped collection leaves a stream open and silent.** No event, no close,
  no error: the stream waits for changes to something that no longer exists.
  This is a *server* behaviour — change streams carry data, not DDL, and the
  only invalidate reasons are `ConsumerLagged` and `ResumeTokenExpired` — and
  it surprises anyone arriving from MongoDB, where dropping a collection
  invalidates its streams. Recorded as a 🟡 and asserted in both clients'
  tests, so the day it changes something fails. **Whether it should change is
  worth a decision**, and it is a change-stream question rather than a client
  one.

**CI runs the Python tests against a real `kimmyd`**, on the same reasoning as
every other client test here: a mocked server asserts only what the client
already believes.

### What task 10 did

`clients/go`, package `kimmydb`, **one dependency** — Go's `net/http` pools
connections, so the reasoning that ruled out Python's standard library does not
apply, and the only thing missing is WebSocket framing.

`coder/websocket` rather than `gorilla/websocket` for a reason specific to this
design: it handshakes through an ordinary `*http.Client`, so a change stream
inherits the same client, TLS configuration, proxy and timeouts as every other
request. `gorilla` dials with its own `Dialer` — two configurations that can
drift, which is a split this project has paid for before.

**Idioms rather than a translation.** Paging and streaming are
range-over-function iterators, so the error is the second loop variable rather
than something a caller has to remember; everything takes a `context.Context`,
including the change stream, and cancelling it is how watching stops.

**It found nothing new, and that is the result.** The roadmap put Go third
because it was least likely to surface a gap the other two had missed. Two
clients and a specification had already taken the surprises, and the third
agreeing is the evidence task 11 rests on.

### What task 11 did, and the defect it exposed

`clients/conformance` holds all three clients to **one** set of scenarios. A
declared list with the observations a correct client must produce, a small
driver per client that runs a named scenario and prints what it saw, and a
runner that starts a fresh node per scenario and compares. Sixteen scenarios,
three clients, forty-eight runs.

**A driver reports; it never judges.** Three clients that each decided whether
they had passed would be three opinions rather than one oracle and three
answers. Coverage is checked separately from behaviour, because a client that
stops implementing a scenario must fail rather than fall silent.

**Shown to go red.** The Python driver, made to stop one page early, produced
`documents_seen: expected 250, observed 200` while the other two passed. A
suite that has never failed is a suite nobody has tested.

**And it found a real defect on its first full run.** The specification had
said since M10 task 1 that collection creation is idempotent — "Created, or
already present" — and the server has always returned `409 conflict`. Both
clients repeated the claim in a comment.

**Nothing caught it because nothing ever created a collection twice.** The
contract test's coverage assertion checks that every *operation* is exercised,
not that every documented *outcome* is, so a false sentence about the second
call went unchallenged through four tasks. It is now corrected in the
specification, in both clients, in the contract test, and as a conformance
scenario — four places, because the claim was in four places.

**Worth carrying forward:** a documented outcome that no test produces is the
next place to look for this kind of error.

### The change-stream fix, and the second defect it exposed

The 🟡 above lasted a day. A dropped collection now ends the streams watching
it — `InvalidateReason::CollectionDropped`, decided **in storage** rather than
at the HTTP edge, because that is where the other two reasons live and where
`finished` is set. Scoped: only a stream watching that collection ends, so a
`Cluster` stream — the embedding worker's — is untouched, and a test says so.

The sharp part was never the stall. **Ids are derived from `(database,
name)`**, so a collection recreated under the same name has the same id, and
the old stream would silently resume delivering for it — one stream spanning
two different collections with nothing in between.

**Then fixing it exposed a second defect.** A replicated schema change was
appended to the receiving node's oplog but **never published**, while a
replicated document was. So a drop ended its own node's watchers immediately
and left every other node's waiting for an unrelated write to nudge them.
Invisible for as long as streams filtered DDL out, because "delivered late" and
"not delivered" looked identical — and **only the cluster harness could have
found it**, since a single node applies its own drop directly. That is the
third time the harness has caught something every transport-free test agreed
was fine.

**One representation fixed on the way past.** The invalidate reason went onto
the wire through `{:?}`, so renaming a variant would have silently renamed a
value clients branch on. It has an `as_str` now, with the existing two names
kept exactly as `Debug` rendered them — the invariant `NodeId` and
`CollectionId` each cost a replication outage to learn.

**And a third defect, older than both, found by checking a claim.** A pull
request description said a client could resume past an invalidate and would
replay to it again. Probing a real node showed that was not what happened —
and what *did* happen was worse. Because ids are derived from
`(database, name)`, a recreated collection reuses its id, so the oplog still
held the dead incarnation's entries and streams still matched them:
`from_start` on a healthy recreated collection replayed a dead collection's
documents and then invalidated immediately, never showing the live data.

A stream now never reads across a drop, and a resume token from before one is
**refused** with `resume_token_expired` rather than moved forward silently —
between that token and this collection's first event is a gap, and a silent gap
is what the invalidate machinery exists to prevent.

**The pattern across all three:** each was found by asking a running node what
it did, not by reading what it was supposed to do. The third one came from
verifying a sentence I had written in a PR description, which turned out to
describe a system that does not exist.

**Task 5 carried the milestone's distributed-systems risk and is done.** Both
traps were handled deliberately — the answering node is added explicitly because
`Members` holds *peers only*, and the authenticated-peers invariant (ADR-053)
now also stops an unauthenticated node being advertised to clients as somewhere
to send credentials. Anything new reading `Members` still inherits both.

**Task 7 answered the question that had been unanswerable**, and the numbers
are in [Benchmarks](benchmarks.md): TLS is close to free, the protocol costs
~0.1 ms per request, reads scale with clients (8,001/s → 70,660/s) and writes
do not (143/s → 602/s, p99 10 ms → 246 ms). It also found something no
engine-level benchmark could: a single write costs about **twice** as much
through the daemon as at the engine, which is neither protocol overhead nor
encoding, and is recorded as an open question rather than explained.

### What task 12 did, and what the mutation pass found

`shelf`, a small library catalogue, **written three times** — Rust, Python, Go.
Not three snippets: one application that stocks a shelf in a single commit,
groups it by decade, pages it five at a time, searches it by vector, and
watches it change while another thread writes to it. All three print the same
three nearest titles with the same scores to three decimals, which is a
stronger statement about the clients than any of them passing its own tests.
They run in CI against a real node, because an example nobody runs decays into
a document that used to be true.

The embedding is a **deliberate toy** — a normalized bag-of-words FNV-1a hash,
sixteen wide, identical in all three languages — and says so in every file. It
buys a real pipeline (configure vectors, store per document, search, score)
with no API key, no model and no network. Swapping in a real provider does not
change the application code above the search.

**The mutation pass, split by scope.** Running every client mutant against the
workspace suite was a nine-hour job; running the client's mutants against the
client's tests is twenty minutes. The client's first pass caught 101 of 167
viable mutants, and the misses were not marginal:

- **Seven convenience methods had no test at all** — `find`, `update`,
  `delete`, `aggregate`, `replace_document`, `delete_document`, `download`.
  Each could be stubbed out entirely and the suite stayed green, because the
  tests reached the server through `pages`, `insert`, `count` and `request`.
  One-line wrappers are exactly where a wrong verb or a wrong path hides.
- **Topology filtering was untested**, because a one-node harness has nothing
  to filter: every comparison in `refresh_topology` could be inverted with no
  observable effect. It has three nodes to choose between now.
- **Thirteen of the seventeen error codes were never produced by any test**,
  so any of them could have been renamed silently — on public surface that
  callers branch on.

Five new tests took it to **133 caught of 167 viable** (190 mutants: 34
missed, 20 unviable, 3 timeouts). What is left is classified rather than
absorbed, in [Testing](testing.md): reconnect-backoff internals that need
fault injection, and a handful of provably equivalent mutants. **The `retry:
wait` row was closed on 2026-08-14** — and closing it found the branch was not
merely untested but wrong; see the section below.

**The rule from M7 held again**: the value was not the score, it was that
mutation testing asked "which of these lines could I delete?" and the answer
included seven public methods.

**The server-side half of the pass was abandoned at the time, and was finished
on 2026-08-14.** The 90 mutants split 76 `kimmy-api` / 12 `kimmy-storage` / 2
`kimmy-auth`. The original run re-ran all three suites for every mutant at a
parallelism that pushed each past its timeout — 3 caught and 15 timeouts out of
47 before it was stopped. One pass per crate, scoped to that crate, on an idle
machine: 31 minutes for the largest, **zero timeouts**.

**It found three claims with nothing behind them**, which is the same yield as
every other pass this project has run:

- `InvalidateReason::as_str` could return `""` — the method that exists *so
  that* renaming a variant cannot silently rename a value clients branch on.
- `capabilities()` could return an empty list with every assertion still
  passing, because the test compared the wire to the function that produced it
  and then made a check that is vacuous on an empty list.
- `register` documents itself as "silent when nothing changed" and nothing
  tested it — the cluster harness structurally cannot, since it starts each
  node once.

**And it produced a distinction worth carrying**: "missed" means three
different things — a real gap, a property covered only by an `#[ignore]`d
harness test that no mutation run ever sees, and a killer that lives in another
crate and is hidden by the per-crate scoping that makes these runs affordable.
[Testing](testing.md) has the table.

### How to size a milestone, when one is wanted again

**No milestone is running.** M11 was chosen on 2026-08-14 and paused on
2026-08-15 in favour of local usability — see the boxed note at the top. This
section is the material M11 was sized from, kept for whoever sizes the next one
and for the case where M11 is re-opened. **It is not a queue.**

The material, if a milestone is ever needed for its own sake:

- **The carried debt below**, none of it blocking.
- **The gap that scale testing would have caught.** The HNSW orphaning in #81
  existed since M7 and was found only because a test failed once in CI and
  someone asked what it was reporting. It took ~1,500 index builds to
  characterise. Nothing in the suite runs anything 1,500 times, which is the
  same blind spot the row below names.
- **The gaps in [Testing](testing.md)** — nothing runs for long or at scale,
  nothing kills a node mid-write, multi-node tests are pairwise and
  short-lived. Every serious bug of the last two sessions lived in exactly
  those blind spots.
- **The evidence M8 and M9 both produced:** every claim that mattered and had
  no mechanism behind it turned out to be false. Four M8 branches found one,
  the two post-M8 fixes were two more, and M9 found one in every task. Prefer
  work that turns another standing assertion into something that fails when it
  stops being true.
- **Index-ordered scans**, the one piece M9 named and did not build.
  `scan_range_in` ends with `out.sort()` over document keys, so index
  candidates arrive in `_id` order rather than index order. That is why sorted
  paging still uses `skip`, and why every sorted `find` materialises its whole
  match set before sorting. Fixing it would make sorted paging constant-work
  per page *and* make every sorted query cheaper — the largest single
  performance item known to be outstanding.

### What driving a real cluster established

Beyond the suite, on 3- and 5-node clusters from a release build — worth
knowing so it is not re-derived:

- Formation, one JWT cluster-wide, DDL/document/bulk replication, LWW
  convergence under 1000 concurrent writes (~7s) and under 100 conflicting
  writes to a single key.
- Kill/rejoin, `SIGSTOP`/`SIGCONT`, webhook failover, snapshot resync past the
  retention horizon, partition heal with conflicting unique values (both
  documents survive, both nodes count the violation).
- Change-stream resume tokens are **portable across nodes** — a token from one
  node resumes correctly on any other.
- **The auto-embedding pipeline end to end**: document written with no client
  vectors → worker calls the provider → shadow collection → replicated to every
  node in ~2s → semantic search by *text* returns the right document with exact
  cosine scores. Each document is embedded **exactly once cluster-wide**, so an
  N-node cluster does not pay N× for embeddings.
- A 90-second soak with a node cycling down/up/STOP/CONT: 5,351 writes, none
  lost, converged in 42s.

**Three cautions for whoever writes the next drive script.** `PUT /docs/{id}`
without `?upsert=true` returns `200 {"matched": 0}` and writes nothing — a
conflict test built on it wrote nothing at all and passed, because five nodes
agreed on an error. Shadow-collection vectors take ~2s to replicate; a check
that runs immediately reports a broken pipeline that is merely young. And a
**first request against a fresh node is slow** for reasons that are not the
feature — the cursor drive measured 20.7 ms on page zero and sub-millisecond
on every page after it. Discard the first sample or say plainly that you did.

### Where the code is

| | |
|---|---|
| `kimmy-core/src/` | Shared types no crate may own alone: `keyenc.rs` (order-preserving key encoding — the property cursors and TTL both rest on), `partial.rs` (the bounded partial-index filter language), `cursor.rs` (the paging token), `ids.rs`, `hlc.rs`, `oplog.rs` |
| `kimmy-storage/src/` | The engine: `docs.rs` (CRUD + oplog + `delete_guarded`), `index.rs` (secondary indexes, multikey, partial membership), `modify.rs` (`find_and_modify`, matching inside the write transaction), `expiry.rs` (the TTL scan), `sync.rs` (transport-free anti-entropy + `lag_behind_ms`), `vectors.rs`, `rewind.rs` |
| `kimmy-query/src/` | `plan.rs` (the rule-based planner: equality prefix, both-bounds ranges, `$in` unions, and partial-index containment), `expr.rs` (computed expressions), `aggregate.rs` (stages), `filter.rs`, `update.rs`, `shape.rs` |
| `kimmy-vector/src/` | `worker.rs` (embedding + reindex backfill), `provider.rs` (the dialects + `Auth`), `index.rs` (HNSW + snapshot save/load), `cache.rs` (`IndexCache`, snapshot adoption) |
| `kimmy-api/src/` | `exec.rs` (the single authz + query executor both edges call), `expiry.rs` (which node expires a collection), `webhooks.rs` / `dispatch.rs` / `ownership.rs` / `egress.rs` (webhooks), `metrics.rs`, `routes.rs`, `json.rs` (Extended JSON v2 at the boundary) |
| `kimmy-cluster/src/` | `membership.rs` (SWIM/foca, `Members`), `peers.rs` (`replicate`, `ReplicationConfig`), `transport.rs` (TCP framing), `health.rs` |
| `kimmyd/src/node.rs` | Wires everything: spawns cluster tasks, embedding worker, webhook dispatcher, GC, TTL expiry |
| `kimmyd/tests/cluster.rs` | The multi-node harness |
| `kimmy-client/src/` | The Rust client: `lib.rs` (`Client`, `Builder`, `Safety`, `Method`, failover and token renewal), `error.rs` (the mirrored `ErrorCode`), `page.rs` (`Query`, `Pages`), `watch.rs` (`ChangeStream` and its reconnect) |
| `kimmy-api/src/` (M10) | `error.rs` (the 17-code taxonomy and three-valued `Retry`), `version.rs` (`GET /v1/version`, `Capability`), `topology.rs` (`GET /v1/topology`, the node registry in `__kimmy.__nodes`), `json.rs` (`JsonBody`, so typed bodies get the envelope) |

**And outside `crates/`, all of it new in M10:**

| | |
|---|---|
| `docs/openapi.yaml` | **The protocol specification, and the authority.** ~2,100 hand-written lines. If code and this disagree, one of them is a bug — decide which, don't paper over it |
| `crates/kimmy-api/tests/openapi.rs` | The contract test that holds it true: inventory in *both* directions, live response validation, a coverage assertion, and the error-code/retry cross-check |
| `clients/python/` | Package `kimmydb`, synchronous, `httpx` + `websockets`, built with `uv`/hatchling. 19 tests |
| `clients/go/` | Module `github.com/titusai-io/kimmydb/clients/go`, package `kimmydb`, one dependency (`coder/websocket`). 18 tests |
| `clients/conformance/` | `scenarios.json` (16 scenarios), a shared oracle, `run.py`, and one driver per client. **The only test that compares the clients to each other** |
| `examples/` | `shelf` — one application in three languages, plus `run-all.sh`. The Rust one lives at `crates/kimmy-client/examples/shelf.rs` because Cargo wants it there |

### How to run and verify things

- **Scratch server:** `KIMMY_ROOT_PASSWORD=pw ./target/debug/kimmyd --config
  <toml>` with a non-default port and scratch `data_dir` (7878 may collide).
  Log in at `POST /v1/auth/login`; the bootstrap user is `root`.
- **Cluster harness:** `cargo test -p kimmyd --test cluster -- --ignored
  --test-threads=1`. A node with `cluster.enabled` and no seeds refuses to
  start, so the harness pre-allocates ports and cross-seeds.
- **Benchmarks:** `cargo bench -p kimmy-storage -p kimmy-vector`, then
  `scripts/bench-baseline.py check`.
- **Mutation testing:** `cargo mutants --file <f> -o <outdir> -- -p <pkgs>`
  (installed). `--in-diff <diff>` scopes to a diff. No `--no-fail-fast` flag —
  that habit belonged to the retired hand-rolled harness; a non-compiling
  mutant reports `unviable`. Some escapes are *equivalent mutants* no test can
  kill — prove it, don't chase it.
  **Scope the test command to the mutant's own crate** (`--file
  'crates/kimmy-client/src/**' -- -p kimmy-client`). Unscoped, every client
  mutant re-runs the storage and cluster suites: a nine-hour job instead of
  twenty minutes. Set `--timeout` from a *contended* baseline, not an idle one
  — M10 lost most of a day to a run at `-j 8` with a 300s cap that produced 15
  timeouts and 3 caught out of 47, where every "timeout" was `cargo test` still
  linking. And note the binary is `cargo-mutants`: **`pkill -f "cargo mutants"`
  does not match it**, so an abandoned run survives and competes with its
  replacement. **Run these alone** — the #79 pass beside an ordinary
  `cargo test --workspace` stretched that suite from ~2 minutes past 10;
  contention ruins whatever shares the machine, not only the mutation run. On
  an idle box the same 76 mutants finished in 31 minutes with zero timeouts.
  And **"missed" means three different things**: a real gap, a property covered
  only by an `#[ignore]`d harness test that no mutation run ever sees, or a
  killer that lives in another crate and is hidden by the per-crate scoping. Widen
  the scope and re-run before believing a gap is real — both `kimmy-auth`
  "misses" died the moment `-p kimmy-api` joined. [Testing](testing.md) has the
  table.
- **The Python client:** `cd clients/python && uv run --extra dev pytest`.
  Needs a `kimmyd` binary; `conftest.py` starts one. Timeout is 60s per test,
  because a change-stream bug once hung a test for ten minutes.
- **The Go client:** `cd clients/go && go test ./...`. Same arrangement.
- **The conformance suite:** `python3 clients/conformance/run.py` — starts a
  node, runs all three drivers over `scenarios.json`, compares each to the
  shared oracle *and* to the others. This is the test that catches a client
  quietly disagreeing with its siblings.
- **The examples:** `./examples/run-all.sh` — builds nothing, starts a fresh
  node, runs all three `shelf` programs against it in sequence. The second and
  third exercise the "already stocked" path. Requires `target/debug/kimmyd`.
- **Live provider drive:** a fake HTTP endpoint speaking a dialect's shape;
  set the key env var (`OPENAI_API_KEY`, `COHERE_API_KEY`, `GEMINI_API_KEY`)
  before launching the node.
- **Driving a change, in general.** Every M9 branch used the same shape: build,
  start a scratch node on a spare port with a scratch `data_dir`, log in, seed
  with `/bulk` (one commit — seeding 100k documents one at a time takes
  minutes), then exercise the change *and its refusals* with `curl` or a short
  Python script. Where a claim is about cost, measure it against a **release**
  build; a debug build's numbers are anecdotes. Check the node log for
  warnings before stopping it, then `pkill -f "kimmyd --config <path>"`.
- **Driving the protocol specifically.** `docs/openapi.yaml` is machine
  readable, so a drive can validate against it rather than eyeballing: task 1's
  script parsed the document with `yaml` and checked required fields and
  declared types on 32 real responses from a release node. Python has `yaml`
  here but **not** `jsonschema`, so a full validator has to be the Rust test —
  the shallow check is still worth having, because it is a second reader of the
  same document.
- **The suite is 1,131 Rust tests** across 28 targets, plus **11 ignored** —
  8 cluster harness, 3 in `kimmy-vector` — and 19 Python, 18 Go and 16
  conformance scenarios. A full `cargo test --workspace` is about two minutes.
  Counted 2026-08-15 with
  `cargo test --workspace 2>&1 | grep '^test result' | awk '{p+=$4} END {print p}'`.
  **The earlier figure of ~1,126 was close by luck and 902 in two PR
  descriptions was simply wrong** — that one came from piping the grep through
  `tail -25` before summing it, so eight targets were dropped. Count without a
  `tail` in the pipe.
- **The three ignored `kimmy-vector` tests are run deliberately**, in release,
  because a 384-dimensional graph is 146 s to build in debug against 21 s in
  release. One is a regression guard and two are the measurements the
  reachability constants are sized from — see [Testing](testing.md) invariant 6:

  ```bash
  cargo test -p kimmy-vector --release -- --ignored a_healthy_graph
  ```

### Carried debt, none blocking

**The register holds zero 🔴.** M7 closed the last one. What remains is all 🟡
in [Deviations](deviations.md):

| Debt | |
|---|---|
| ~~`find {_id}` is a collection scan~~ | **Closed 2026-08-14.** `plan::choose_primary_key`: 7.328 ms → 0.540 ms p50 over 10k documents, examined 10,000 → 1. `update`/`delete`/`count` inherit it. Ranges on `_id` are deliberately still out |
| **Index scans return `_id` order, not index order** — so sorted paging still uses `skip`, and every sorted `find` materialises its whole match set | the largest known performance item; see "how to size the next thing" |
| Rate limiting covers login only | waits on a capacity decision |
| `update` and `delete` apply document by document and can stop partway — bulk *insert* is atomic, they are not. They *do* use the planner now (#60) | by design |
| Keyword search is term overlap, not BM25; chunking counts characters, not tokens; no minimum score threshold | simplifications inside working features |
| Array/set expression operators, variable binding (`$$ROOT`, `$map`, `$filter`, `$reduce`, `$let`), type conversion | outside M9 task 1's agreed list; variable binding needs an evaluation *scope*, not another operator |
| No `$vectorSearch` pipeline stage; no mTLS | not planned |
| ~~The M10 mutation pass covered `kimmy-client` only~~ | **Closed 2026-08-14.** All 90 classified. Found three real gaps: `InvalidateReason::as_str` unpinned, `capabilities()` able to return an empty list with every assertion still passing, and `register`'s "silent when unchanged" claim tested by nothing |
| ~~The client's `retry: wait` path has no test that reaches it~~ | **Closed 2026-08-14.** The test found the branch was *wrong*: `wait` failed over instead of waiting, so it behaved as `elsewhere` with a delay. Fixed, 9/9 mutants caught |
| **The embedding worker commits once per oplog entry, so a write costs two fsyncs and a bulk of 100 costs 101** | **Explained 2026-08-15** by M11 task 1, and no longer the same debt: the *gap* is understood, the *cost* is still paid. The fix is a reserved decision — see the M11 board |
| **Vector indexes built before 2026-08-15 may be missing 10–24% of a collection** | #81 fixes new builds; it cannot repair a graph already cached or persisted under `<data_dir>/hnsw`. [Operations](operations.md) has the rebuild. **Open until the maintainer confirms their nodes are rebuilt** — worth raising directly, since it is the one carried debt that touches data an operator already has |
| **Every HNSW build orphans 0.8%–3.0% of a collection**, and it is accepted | Raised 2026-08-15 by [ADR-061](decisions.md), which measured it while re-sizing #81's threshold. Those documents are returned by no *approximate* vector search at any `k`; exact search and small collections are unaffected. Not a build defect — a property of `MAX_CONNECTIONS` and `EF_CONSTRUCTION`, so rebuilding cannot fix it. **It was previously believed to be zero and described in a comment as approximation rather than loss** |
| Sharding | **deferred by decision** until there is operational experience |

Three rows left this table in M10 and are noted here so nobody re-opens them:
token refresh (task 4), cluster discovery for clients (task 5), and
client-facing throughput, which is now measured and in [Benchmarks](benchmarks.md).

### Invariants a change must not break

- **The multikey flag is one-way and set in the same transaction as the index
  entries.** A flag that cleared, or lagged its entries by even one commit,
  licenses a two-sided range that silently loses documents.
- **A both-bounds plan is validated in the snapshot that scans it.** A `false`
  read in an earlier transaction proves nothing about this one.
- **Index maintenance reads its definitions inside the write's transaction**,
  never from the caller's handle — a stale handle once skipped a just-created
  index entirely, unique constraint included.

- **The transport moves bytes and nothing else.** Convergence is tested without
  a network in `kimmy-storage/src/sync.rs`; keep it that way, or a merge bug and
  a dropped packet become indistinguishable.
- **The version vector is authoritative, not derived.** Never reintroduce a
  rebuild that lowers it — a snapshot grants coverage the oplog never held.
- **There are two vectors, and they answer different questions.** The
  oplog-derived one is *what I can serve* and is what a peer receives from
  `AskVersions`. The witnessed one is *what I have processed* and is what
  `behind` and `lag_behind_ms` must read. Swapping them either way breaks
  something: comparing against servable re-requests everything a node
  processed without appending, forever; advertising witnessed makes peers stop
  sending entries nobody will then send (ADR-054).
- **Anything that processes a replicated entry must witness it**, on every
  branch — applied, superseded, skipped. That is why the per-entry work lives
  in `apply_one` and the witnessing sits in the caller: a `continue` that skips
  the bookkeeping is exactly how this bug existed since M4.
- **Applying replicated DDL must not log**, and must carry the originating stamp
  into any tombstone it records. Both were bugs; both have tests.
- **Retention never collects the newest oplog entry** — the clock resumes from
  it (ADR-028).
- **Anti-entropy excludes `OpKind::UniqueViolation`** — a node's own observation.
- **Collection and index ids are derived from names**, which is what lets a
  replicated entry address the same thing on every node.
- **`kimmy_api::exec` is the single authorization point** for anything a
  principal asked for. Replication goes through `apply_remote`, not `exec`.
- **A signature is not an authorization.** `TokenIssuer::verify` proves a token
  was issued here and has not expired, and nothing else — the account may be
  gone, disabled, or logged out since. The `Auth` extractor checks that, and it
  must stay there: `verify` has no engine on purpose, which is what keeps it a
  pure function (ADR-052).
- **Both things that evict cached token state are load-bearing.** Admin routes
  evict synchronously so a single node is correct with no background task; the
  oplog consumer evicts so a *replicated* edit reaches this node at all. Drop
  the first and revocation depends on remembering to spawn a task — which the
  integration tests proved is easy to forget.
- **The login rate limit is consulted before the password is verified**, or it
  stops bounding the Argon2 work that is half its purpose (ADR-038).
- **Both serving paths use `into_make_service_with_connect_info`.** Without it
  there is no peer address, and every caller silently shares one rate-limit
  bucket.
- **Certificates are read before the socket is bound**, so a bad one stops the
  node rather than failing for whoever connects first (ADR-039). **At reload
  the rule inverts**: a bad certificate is refused and the one already serving
  stays. Both are right — startup has nothing to fall back to and a serving
  node does — and the reload half is what makes a botched rotation survivable
  rather than an outage (ADR-049).
- **Do not add a second native crypto stack.** `ring` is already in the build;
  anything selecting `aws-lc-rs` adds CMake for the same primitives. This is
  now mostly a *feature-flag* discipline rather than a crate-choice one:
  `hickory-resolver` and `axum-server` both ship aws-lc-rs variants of features
  that look innocuous, so `default-features = false` plus an explicit list is
  the pattern. `./scripts/check-native-deps.sh` is the arbiter, and it should
  keep reporting `cc` alone.
- **A type that crosses a format boundary needs a chosen representation, not an
  inherited one.** `NodeId` and `CollectionId` have both cost a replication
  outage by deriving serde and letting BSON decide — particularly a `u64`, which
  BSON cannot hold above `i64::MAX`.
- **A fixture that is a hash is a sample, not a constant.** Test both halves of
  the range, and assert the fixture still has the property the test needs.
- **Webhook progress is recorded only after an endpoint accepts.** Recording
  first turns a failed delivery into a silently skipped event.
- **A node writes only its own progress record.** The moment two nodes write
  one record, last-writer-wins starts discarding delivery history.
- **A dispatch pass applies progress serially, after the concurrent join.**
  Recording inside the concurrent block would race two subscriptions' writes.
- **The resume point moves even when nothing is delivered**, on a heartbeat.
  Without it the retention horizon overtakes every healthy subscription; without
  the heartbeat an idle node writes to the oplog every tick.
- **An event is never dropped for being large.** `fullDocument` comes off; the
  event still goes.
- **The egress policy is checked before *every* delivery**, not only at
  registration, and every resolved address is checked rather than the first. A
  hostname is not a destination.
- **The delivery client resolves through `CheckedResolver`** — the egress check
  and the dial share one resolution, or a zero-TTL name gets a window between
  them. And never fall back to a default client: it follows redirects and
  resolves unchecked, which is both egress protections gone at once.
- **Webhook ownership hashes node ids, never addresses**, and with FNV-1a,
  never `DefaultHasher` — which is not stable between Rust versions. Either
  mistake reshuffles every subscription for a reason that is not a membership
  change: a compiler upgrade, or a node moving without going anywhere. The
  hashed form is `NodeId`'s hyphenated string, which core fixes deliberately
  rather than inheriting from `Uuid`'s serde.
- **The SWIM identity is a wire format.** `Member` is encoded with postcard,
  which is not self-describing, so adding or reordering a field breaks
  membership across versions outright — a new node rejects an old identity
  rather than tolerating it. Any change here is a stop-the-cluster upgrade and
  needs a note in [Operations](operations.md), as ADR-040 and ADR-051 both did.
- **Ownership candidates are the live peers plus this node.** SWIM's live set
  never contains the node holding it, so an owner computed over it alone can
  never be `me` — the bug that silently undelivered every clustered webhook.
  Any new consumer of `Members` must know it is reading *peers*, not the
  cluster.
- **The SWIM member set must contain only authenticated peers.** Every
  membership datagram carries an HMAC over `cluster_secret`, verified before
  foca sees it. This is not defence in depth on top of the replication
  handshake — ownership is computed over the member set, so an unauthenticated
  peer in it wins a share of the webhook subscriptions and delivers none of
  them (ADR-053). Anything new that reads `Members` inherits this.
- **A cluster feature is not verified until the harness has run it on real
  nodes.** Transport-free tests and single-node drives both passed while
  clustered delivery was entirely broken.
- **Correctness never depends on an HNSW snapshot.** A corrupt one is deleted
  and rebuilt (the load runs under `catch_unwind` because `hnsw_rs` panics on
  bad magic), a behind one serves once and rebuilds, a missing one builds, and
  a restore carries none. A snapshot whose metric or dimension disagrees with
  the live config is refused before the graph is read.
- **Vector backfill scans the collection, never the oplog.** The oplog may
  have been collected; the documents are the durable source. The config
  fingerprint is written **after** the scan completes — recording it first
  would leave the remainder embedded under the old model with nothing to
  notice.
- **A provider is cached against the configuration that built it.** A
  reconfigured collection must not keep embedding through the old provider.
- **Replication lag is pushed from the replication loop, never computed in the
  API layer** — that loop is the only place a peer's version vector exists. An
  unreachable cluster reports its *last* value, not zero: an outage has
  unknown lag, and zero would read as perfect health.
- **Health and metrics routes stay out of the latency histogram** but stay in
  the request counter. They fire every few seconds forever and would crowd the
  buckets real traffic lands in.
- **A metric's buckets or thresholds are measured, not chosen.** ADR-046's
  first draft was written from expectation and the measurement corrected it.
- **A bulk insert is one transaction and one commit, and a failure anywhere
  aborts all of it** — every document, and every oplog entry with them. The
  version vector must not move for a batch that did not land, and nothing may
  be published. Events go out once, after the commit, one per document.
- **A batch is validated against itself, not only against stored state.** Two
  documents sharing an `_id`, or colliding on a unique index, must fail the
  batch. This works only because a redb read sees its own transaction's
  uncommitted writes — the property the whole reuse of the single-document
  checks rests on, so both paths have a test.
- **`Engine::insert` and `insert_many` share `insert_in_txn`**, and
  `Engine::delete` and TTL expiry share `delete_guarded`. A check added to one
  must not be added *beside* the other; the point of the helper is that one
  path cannot drift into being more permissive than its sibling.

**From M9 — the query engine and the index path:**

- **`keyenc` is order-preserving, and three features now depend on it.** Index
  ranges, TTL's expired-document scan, and cursor paging all rest on "byte
  order of encoded keys is canonical BSON order". Breaking that does not fail
  loudly; it silently returns the wrong documents.
- **A guard that decides whether to write runs *inside* the write transaction.**
  TTL's `delete_guarded` re-reads the document before tombstoning it, because
  the scan and the delete are separate transactions and a session refreshed in
  between must survive. `find_and_modify` goes further and does the whole match
  inside the write, which is what makes it atomic on a single writer.
- **A partial index may answer only a query provably contained by its filter**,
  and the filter language is bounded (`$exists: true`, equality, four
  comparisons, conjunction) precisely so containment is a decision rather than
  a guess. **Do not widen that language** without also solving general
  implication — a wrong containment check returns a subset with nothing to
  indicate it, which is the multikey failure again.
- **`{a: null}` matches an explicit null *and* a missing field**, so it can
  never prove a field exists and must contribute nothing to partial-index
  containment. Comparisons are safe to treat as proving existence because
  `condition_matches` evaluates over resolved values, which are empty when the
  path is absent.
- **A document outside a partial filter contributes no index keys at all**,
  which is what makes membership fall out of ordinary maintenance —
  `apply_entries` derives removals and insertions from the same function — and
  what keeps such a document from flipping the multikey flag.
- **`$cond` and `$ifNull` evaluate lazily.** A guard like
  `{$cond: [{$gt: ["$n", 0]}, {$divide: [100, "$n"]}, null]}` must work, not
  fail on exactly the inputs it exists to protect.
- **In expressions, null propagates but a type violation refuses.**
  `{$add: ["$typo", 1]}` is null; `{$add: ["text", 1]}` is a 400. Collapsing
  both to null makes a typo and a type error indistinguishable in the output.
- **Integer arithmetic is exact.** `$add`/`$subtract`/`$multiply` compute in
  `i64` while every operand is integral, promoting only on overflow or a
  double. `$sum` accumulates the same way. Accumulating in `f64` and casting
  back loses precision above 2^53 — that was a real bug, sitting under a
  comment saying it must not happen.
- **A TTL index is single-field and dates-only**, and a document whose indexed
  field holds a string or nothing is never expired. That is what stops a policy
  added to a heterogeneous collection from deleting everything lacking the
  field, and type ordering in the key encoding gives it for free.
- **Expiry is owned by one node per collection**, rendezvous-hashed like
  webhooks. Every node expiring independently converges but produces N deletes
  per document. `kimmy_ttl_expired_total` is what keeps that measured rather
  than asserted, and the cluster harness requires it to sum to exactly 1.
- **An expiry is an ordinary `OpKind::Delete`.** `op_kind_from_tag` rejects an
  unknown tag as *corruption*, so a new variant is a stop-the-cluster upgrade.
  And **a delete carrying a body is decoded as a live document** by
  `apply_remote` — never put a payload on one.
- **A cursor carries no server state.** It is the encoded document key of the
  page's last row, which is why it is portable between nodes. Anything that
  makes it node-specific breaks round-robin clients (ADR-055).
- **A query a cursor cannot page gets no `nextCursor` at all** — not a token
  that would silently page in `_id` order when a different sort was asked for.
- **`kimmy-query` is a dev-only dependency of `kimmy-storage`.** The engine
  does storage, not semantics. When the engine needs query behaviour it takes
  it as caller-supplied pure functions (`ModifySpec`, `delete_guarded`'s
  guard), evaluated inside its own transaction. Do not add the production
  dependency; the boundary has improved every design that met it.

**From M10 — the protocol as a contract:**

- **`docs/openapi.yaml` is the protocol's authority, and changing the wire
  means changing it in the same commit.** `crates/kimmy-api/tests/openapi.rs`
  fails when the router and the document disagree about which operations
  exist, when a real response does not match its declared schema, **and when a
  documented operation is not driven by the test at all**. That last one is
  what stops the document becoming prose again; it is also what every new route
  now costs (ADR-056).
- **One concept, one type, across every route that reports a write.**
  `matched` and `modified` are counts everywhere, including on `PUT
  /docs/{id}`, which touches at most one document. `upserted` is a boolean
  because it is one. This was not true until the specification's test drove
  both routes and compared them.
- **A route inventory built by scanning source must handle multi-line
  registrations.** The check that missed them was green for two milestones
  while never looking at `/docs/{id}`. If a scan can silently match nothing,
  assert that it matched something.
- **Every refusal carries the envelope, and the mapping lives in the extractor
  rather than in each handler.** `json::JsonBody<T>` is the only way a typed
  body enters, and `/watch` maps the upgrade rejection the same way. A mapping a
  handler has to remember to reach is a mapping eighteen of nineteen handlers
  did not reach.
- **The error code set is closed by the compiler, and the retry class is part
  of adding a code.** `ErrorCode` is an enum with exhaustive matches for the
  wire string and the class; the class goes on the wire so a client handles a
  code released after it was written (ADR-057).
- **`elsewhere` is only meaningful because every node accepts writes.** Anything
  that reclassifies `internal`, `misconfigured` or `snapshot` is claiming a
  peer cannot answer, which for a full-copy cluster needs a reason.
- **A specification is a wire format, so its own encoding has to be checked.**
  `no` is boolean `false` in YAML 1.1. Quote scalars that could be read as
  something else, and keep the live drive an *independently written* reader —
  a second run of the same parser proves nothing about the document.


## Reading order for someone new

1. **"Getting oriented"** above, then how work runs and the
   oplog-as-spine idea. Nothing else makes sense without them.
2. **[Roadmap](roadmap.md)** — every milestone through M10 is closed, so read
   it for what was built and what was ruled out, not for a task to pick up.
   There is no queued work; the next milestone is the maintainer's call.
3. **[Deviations](deviations.md)** — the register, not a summary of it. Every
   decision M9 and M10 made is a 🟢 entry there with its reasoning, and the
   two debts M10 created are 🟡 entries.
4. **The invariants above**, before touching the query engine, the index path
   or anything that writes.
5. **[Architecture](architecture.md)** and **[Testing](testing.md)** when you
   need the shape of a subsystem or the state of its coverage.

The two things most likely to bite someone new: a claim in a comment that the
code beneath it does not honour (M9 found one in every task), and a cluster
behaviour that passes every transport-free test while being entirely broken on
real nodes (M8 task 1 found exactly that). The harness and the live drive exist
because neither is hypothetical.

---

## Conventions for this file

Replace the sections above when a branch lands; keep only the current state.
A milestone's per-task narrative is worth keeping while it is the most recent
one and worth retiring once it is two behind — its durable lessons should have
become invariants by then. The historical record lives in
[Deviations](deviations.md) and [Decisions](decisions.md), which are
append-mostly by design.

---

## Current state (2026-08-25, after v0.5.0)

The load test of 2026-08-24 (7,219 documents into one vector-enabled
collection on three Pi 3s) ended with every member OOM-killed and the swarm
leaderless; its full reconstruction lives in the NexWiki load-test report.
What shipped since, in order: the provider on the test cluster was
right-sized (its container sat at 96% of its limit swap-thrashing), the Pi
memory split was rebalanced (kimmydb 448M / rabbitmq 192M), rabbitmq was
undeployed then redeployed alone, and PR #110 gave the embedding worker
per-collection rendezvous ownership plus a disable flag and metrics.

Known-open, in priority order: bounded catch-up replay batches (the one
structural weakness the incident proved), catch-up-aware readiness, cheap
collection counts, and the retest protocol in the NexWiki report. The test
swarm is blank and healthy with force-fsck boots; kimmydb's next test home is
likely three small GCP VMs.
