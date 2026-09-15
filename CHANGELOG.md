# Changelog

Notable changes, for people upgrading. Hand-written, deliberately: the commit
log records how the work happened, this records what an operator or client
author needs to know, and the two are different documents (ADR-063 covers the
mechanics — the release workflow lifts the matching section below into the
GitHub Release notes when a `v*` tag is pushed).

Versioning follows the pre-1.0 policy in
[docs/compatibility.md](docs/compatibility.md): a `0.MINOR` bump may carry
breaking changes and says so here; a `0.x.PATCH` bump never does.

## Unreleased

### Fixed

- **A recreated collection can be watched after its drop's oplog entry is
  collected.** A collection watch — the tail as well as `from_start` — on a
  collection recreated under a dropped name answered `410
  resume_token_expired` whenever the drop's tombstone outlived the drop's oplog
  entry, and kept doing so until the tombstone was collected. Latent since the
  incarnation clamp shipped; exposed by a retention deviation, and hit by any
  operator running `tombstone_retention_secs` above `oplog_retention_secs`, and
  by a member holding a peer's tombstone from a snapshot. An open without a
  token now always succeeds, `from_start` beginning at the first retained entry
  stamped after the drop; a resume token from before the drop is still `410`.
  Found in round 0330.

## 0.29.0 - 2026-09-15

**A minor. Read the two entries under *Changed* before upgrading.** The backup
route now needs free disk beside the database and has no request deadline, and
the Rust client's `Error` gains variants an exhaustive `match` must handle
(ADR-170). The wire gains one defaulted field, `AskEntries.marked` (ADR-172),
which an older member ignores and does not send, so a member-at-a-time roll
needs no ordering.

**The roll is not evidence of mixed-version compatibility**
([docs/compatibility.md](docs/compatibility.md)). 0.29.0 rolls member at a time,
with no held mixed-version phase, so its wire compatibility rests on the tests
beside the protocol:
- `ask_entries_marked_crosses_a_version_boundary_in_both_directions` and
  `ask_entries_crosses_a_version_boundary_in_both_directions`, which decode the
  new request as an older member reads it and an older request as this release
  reads it;
- `a_requester_naming_spans_asks_a_sender_that_ignores_them_for_no_more_than_before`,
  which answers a 0.29.0 requester the way a 0.28.1 sender and an ADR-171 sender
  each serve.

ADR-172's live mixed-version test was prepared and parked for this release.

### Added

- **`kimmy_sync_held_marks_released_total` counts the entries a sync window
  releases from being held as state** ([ADR-169](docs/decisions.md)'s addendum). A member caught up
  by snapshot holds what the snapshot wrote above the vector it advertises until
  those entries arrive in position, and a member pulling from it leaves them
  meanwhile under `kimmy_sync_entries_skipped_total{reason="beyond_advertised"}`.
  That is the same label the ordinary race has always counted, so a few there
  and a stop could not say which had happened. The new series counts the release
  itself, once per entry, when the batch that released it commits. It is on the
  OTLP bridge as `kimmy.sync.held_marks_released`. One new `/metrics` series,
  nothing removed, nothing on the wire.

### Changed

- **A backup no longer holds its image in the member's memory.** `GET
  /v1/admin/backup` built the whole backup in the heap and kept it until the client
  had read the last byte. On a three-member test cluster with a 2 GiB limit per
  member, an 808 MB backup took a member to 1,789 MiB, and earlier rounds measured
  90% and 97%. The walk now writes to an unlinked temporary file beside
  `kimmy.redb`, closes its read transaction, and streams the file, so memory holds
  one read chunk and a slow client pins neither the database's pages nor the heap.
  See [ADR-170](docs/decisions.md), which amends ADR-041.

  **What to plan for.**
  - **Disk:** the data directory needs free space equal to one backup for the walk
    and the transfer. A backup that runs out answers `500`, and an `ERROR` line
    with `event` `backup failed` names the directory and the operating system's
    error. Nothing is left behind.
  - **No deadline:** `server.request_timeout_secs` no longer applies to this
    route, which would otherwise cut every real backup at 30 s. Headers arrive
    when the walk ends, which on a cold page cache can be many minutes. Use a
    read-idle timeout, not a total one.
  - **Additions:** the response now carries `Content-Length`, and
    `kimmy_backup_duration_seconds` is a new histogram of how long each backup
    took to produce, with its sum on the OTLP bridge as
    `kimmy.backup.duration_seconds`. The `served a backup` line gains
    `spill_bytes` and `elapsed_ms`.
  - **Unchanged:** the bytes of a backup and the restore.

- **The Rust client streams a download, and `kimmy backup` no longer fails on a
  backup longer than 30 s.** `kimmy-client` downloaded with its 30 s total
  timeout and collected the body in memory. For a download, `Builder::timeout` now
  means a read-idle timeout on the body: there is no total, and the wait for the
  response head is unbounded. See [ADR-170](docs/decisions.md).
  - **New public API:** `Client::download_to`, which streams into any `AsyncWrite`;
    `Error::Stalled`, returned when the body goes that long without a byte; and
    `Error::Io`, for a local write failure.
  - **Breaking for exhaustive matches:** `Error` is not `#[non_exhaustive]`, so a
    downstream exhaustive `match` on it stops compiling. Pre-1.0, no shim.
  - **`kimmy backup`** downloads to a partial file beside the destination and
    renames it into place only once it is complete and synced. A failed download
    leaves an existing file exactly as it was.

### Fixed

- **One write on a quiet member no longer makes every peer re-read the oplog
  above that member's previous write.** A pull asks from one position for every
  origin, the peer's own position on whichever origin it trails most, and the
  window read from it carried every other origin's entries above that position,
  which the puller already held. So one document written on a member whose
  previous local write was an hour old cost each peer a run of back-to-back
  pulls that applied nothing. It was measured on a three-member cluster at 306
  pulls and up to about 313,000 entries per peer in 67 s, and on 0.28.1 at 282
  pulls in 56 s. Every version roll triggered it once per member, because a member
  rewrites its topology record when its build changes. A window now passes over
  every entry the puller's witnessed vector already covers for that entry's
  origin, judged on the vector the puller already sends with its request. In the
  harness test (a debug build, one run; not yet re-measured on a cluster), the
  same write is one pull. The skipped entries are judged on their keys and never
  decoded, and the walk runs off the async worker.
  [ADR-171](docs/decisions.md) has the reasoning and why nothing a puller lacks
  can be skipped.

  A member holding an entry as state below its own position is no longer left
  out by this: see the next entry ([ADR-172](docs/decisions.md)).

  **Nothing to decide before upgrading, and no ordering in the roll.** Nothing on
  the wire changes: the vector was already sent. A member serves the shorter
  window from the moment it runs this release, whatever build pulls from it. A
  member still on the previous build serves the old window, so the roll itself
  can still show the drain against members not yet rolled. No new `/metrics`
  series. The `merged from peer` line reads the same, with fewer `pulls` and no
  run of `applied 0`.
- **An entry a member holds as state below its own position is released on the
  next pull.** A scoped repair that fills a hole leaves the document held as
  state (ADR-160), below what the member has witnessed of its origin. The skip
  above passed over it on every pull, so its mark stayed until the origin wrote
  again or retention collected the entry. `AskEntries` gains an optional field,
  `marked`: per origin, the span from the lowest to the highest such entry. The
  peer serves that span's entries, and the member releases them on arrival
  (ADR-169). The member asks even when it is behind on nothing.

  A span never turns a pull into a snapshot, because the horizon is still judged
  on the member's position. Entries the peer has already collected stay held
  until the member's own retention collects them.
  [ADR-172](docs/decisions.md).

  A span is walked once, a window at a time, from where the last window left
  it. Once a peer has been walked across a span, the span is dropped for that
  peer. It is asked from its bottom again only when a snapshot or repair adds a
  mark below where it resumed, or after five minutes. So neither a peer that
  cannot serve a span's lowest entry nor continuous writes on its origin pin the
  pull.

  **Nothing to decide before upgrading, and no ordering in the roll.** The field
  is defaulted, and an older member sends none. A pull's `from` is never lowered
  for a span. A member on the previous release therefore receives the ordinary
  request and serves what it serves today. It serves nothing inside a span, and
  those entries wait for a member on this release, or for retention. The member
  logs `asking the peer to serve entries this node holds as state below its own
  position` at `INFO` when the spans it names change. On a healthy cluster that
  line should not appear.

## 0.28.1 - 2026-09-15

### Fixed

- **Five walks still ran on tokio's workers, and `/metrics` stalled behind
  them.** ADR-153 moved read scans off the async workers but missed
  `GET …/violations`, both forms of `$lookup`, `describe_collection`'s document
  total (over REST and MCP), and `GET /v1/admin/backup`. Each now runs under the same
  `block_in_place`. The violations route also answers without reading the
  oplog when the collection has no unique index or `?index=` names none of
  them, and otherwise decodes only that collection's violation records rather
  than every retained entry. Before this, a call was measured at about 800 ms
  and several hundred MiB of resident memory on a member retaining a day of
  writes, with `/metrics` stalled for up to 1.3 s under eight concurrent calls.
- **A vector search no longer decodes every vector of the collection before
  searching.** Its check that the collection holds any vectors (the
  `no_vectors` answer) counted all of them on every search; it now stops at
  the first live one, so a search over a large collection gets cheaper by
  that walk. What the check answers is unchanged: a collection whose vectors
  have all been deleted still reads as having none.
- **The divergence check's count walks ran inline on the async runtime.** The
  sync tick's count of the probed collection, and the count a member takes to
  answer a peer's probe, each walked that collection on a tokio worker. Both
  now run under the same `block_in_place` as other walks. On a loaded member
  that is a candidate for the `/metrics` scrape stalls seen under load. The
  tick's count and the version vector it is judged against are now read in one
  read transaction; which collections are compared, and when, is unchanged.
- **A collection drop that gave up on a busy writer left its rows on disk
  until the next restart.** When a chunk of a drop's purge could not take the
  single writer inside the request's budget, it logged a `WARN` and left the
  rest for the next start, and nothing retried it while the node ran. Past
  `storage.tombstone_retention_secs`, only a collection created under the same
  name would have cleared the rows. The retention pass now finishes such a
  purge, logging each collection and its row count before and a line after,
  and keeps the collection tombstone that marks the rows until they are gone.
- **A peer answering windows that move nothing could spend a sync tick's whole
  budget on one contact.** A tick keeps pulling from a peer while each pull
  comes back truncated and budget remains (ADR-157). A broken or hostile peer
  answering with an unexhausted window at or below this member's position was
  pulled from back to back until the interval ran out, every tick. Measured
  against a fake peer on localhost, that was about 14 ms per pull, 145 pulls in
  a two-second tick. A contact now makes at most 128 pulls a tick and resumes
  on the next. Reaching the ceiling is logged at `INFO` with the pulls and
  entries applied, and at `WARN` only when no pull applied anything, which is
  the shape of a peer serving windows that cannot advance.
  Round 0310's real drains fitted 22–23 full windows per five-second tick, so
  ordinary catch-up is unaffected. The cost: a tick that could have pulled more
  than 128 full windows from one peer, about 131,000 entries, leaves the rest to
  the next tick, at most one extra tick per 131,000 entries.

### Documented

- **`docs/mcp.md` states what `initialize` negotiates.** A client naming
  `2025-11-25` or earlier has its version echoed; a later version is answered
  `2025-11-25`, and a client that cannot accept that fails at connect time.
- **The OTLP description of `kimmy.write_lock.held_seconds.drop` no longer says
  a collection drop removes everything in one transaction.** A collection drop
  has purged in chunks since ADR-158, each chunk its own hold; the description
  now matches `operations.md`.
- **`docs/compatibility.md` says a member-at-a-time roll is not evidence that
  mixed versions work together.** The 0.28.0 roll took about 10 s with the
  cluster mixed for 6.8 s, and no sync round crossed versions in that window. A
  release that changes the cluster wire names a deliberate mixed-version test in
  its plan; otherwise the evidence is the version-boundary tests beside the
  protocol, not the roll.

### Dependencies

Versions are as `Cargo.lock` resolves them.

- **`rustls` 0.23.43 → 0.23.45, for RUSTSEC-2026-0285.** The advisory says
  rustls accepted TLS 1.3 handshake messages sent at the wrong encryption
  level. rustls carries both the server's TLS listener and every outbound TLS
  connection (peer sync, embedding providers, the client and CLI), so every
  member should take this release. Nothing about configuration or behaviour on
  a correct handshake changes.
- Patch updates: `rustls-webpki` 0.103.13 → 0.103.15 (with rustls), `chacha20`
  0.10.1 → 0.10.2 (replaces a yanked version, via `foca`).

## 0.28.0 - 2026-09-14

**A minor, and upgrading to it requires reading one entry.** ADR-168 changes
what the divergence check's count half reports, and therefore what
`kimmy_sync_divergent_collections` and the repair counters mean on a cluster
taking writes. The count comparison is now deferred against any member that is
still moving, so on a cluster with continuous writes, a quiet count half no
longer means the counts were compared and matched. It means they were not
compared. The existence half is unaffected. Read the second entry under
*Fixed* before relying on either metric after the upgrade. Nothing on the
wire or on disk is removed or tightened: a snapshot page gains a field that
older members ignore, so a member-at-a-time roll needs no ordering.

The release also carries dependency updates, two of them minor bumps, listed
under *Dependencies*.

### Fixed

- **A snapshot now carries document deletions to a member that holds the
  document.** A member caught up from a peer by snapshot — one that fell behind
  the peer's oplog retention, or one repairing a collection — used to keep any
  document it held that the peer had since deleted, live and writable, on that
  member alone. Completing the snapshot grants coverage of the peer's history, so
  the delete was never replayed to it afterwards. A page now carries the deletes
  in the range it walks, and the receiving member removes the document through
  the same last-writer-wins check a replicated delete goes through: a write newer
  than the delete survives. Mixed-version clusters need no ordering: an older
  member ignores the new field and keeps the previous behaviour until both ends
  of a pair have rolled. See ADR-167.

  **What this does not close.** A member that never held the document records
  nothing, so it carries nothing on: a member that later catches up from it —
  by snapshot or by ordinary replication — can still keep a document the original
  member deleted. More generally, a member caught up by snapshot can serve
  ordinary replication that omits deletes and collection drops it holds only as
  state. When a document survives this way, the divergence check reports that
  collection's document count on the member keeping it. And where a member
  applies such a delete part-way through a catch-up, a member pulling ordinary
  replication from it can be held back from that delete until the member next
  records a write from the delete's origin, or until it takes the delete from
  another member. A delete older than
  `storage.tombstone_retention_secs` on the sending member has no record left to
  carry and does not travel by either route.

- **A member catching up no longer confirms a divergence that is only its own
  lag.** A member taking in a peer's steady writes compared a document count it
  read before pulling them against the peer's count read after, confirmed a
  divergence, and ran a repair that found nothing — on a healthy cluster under an
  ordinary bulk load, holding `kimmy_sync_divergent_collections` above 0 for
  minutes and pushing `kimmy_sync_repair_rounds_total` into the hundreds. The
  count comparison is now deferred while either member trails the other and is
  still catching up; a member whose replication has stopped is still compared by
  its peers once it has stood still for three checked contacts. **The count half
  of the check therefore does not compare against a member that keeps writing** —
  one document at a time is enough — until its writes pause; contacts between
  level members still compare, and the existence half still runs. See ADR-168.

- **An entry a member holds from a snapshot is released when it arrives by
  ordinary replication.** A snapshot document or a carried delete is held out of
  the vector a member advertises until it is known to be history. The release
  that should have happened when the same entry then arrived by replication never
  did, so the member went on withholding it from peers pulling from it and, until
  some unrelated write from the same origin, held a document count that did not
  match what it advertised — enough for the divergence check to confirm a
  difference that was only held state. Arriving in a replication window now
  releases it, with no second change-stream event. See ADR-169.

### Documented

- **A single write behind a bulk load waits for the bulk's fsync.** Under
  `storage.durability = durable` a commit fsyncs inside its hold on the single
  writer, and a bulk insert is one commit, so a concurrent single write queues
  behind the whole bulk's flush; measured, single-write p50 roughly tripled
  under back-to-back bulks of 100 on one member. `operations.md#capacity` now
  says so, and what `coalesced` does and does not change: the same no-loss
  promise and a shared fsync for concurrent writers, but up to one coalescing
  window added per write, a slower lone writer, and a shared flush that still
  holds the writer.

### Dependencies

Versions are as `Cargo.lock` resolves them.

- **`rmcp` 3.1.4 → 3.2.0, a minor bump (MCP server).** One observable change
  on the MCP endpoint: an `initialize` request naming `protocolVersion`
  `2026-07-28` or later used to have that version echoed back, and is now
  answered with `2025-11-25`, the newest version that still has an
  `initialize` handshake. A client naming `2025-11-25` or earlier sees no
  difference. A client that cannot accept the answered version fails visibly
  at connect time, not quietly later. The rmcp release also allows concurrent
  streamable HTTP requests; no difference is expected for KimmyDB's stateless,
  JSON-response endpoint. Both statements come from reading the dependency's
  source, not from testing, and no KimmyDB test pins the negotiated version
  yet.
- **`jsonschema` 0.52.1 → 0.55.1, a minor bump.** Test-only: it validates
  KimmyDB's own responses against its OpenAPI document in the test suite and
  is not part of the shipped build, so request validation and error messages
  are unaffected.
- Patch updates: `rmcp-macros` 3.1.4 → 3.3.0 (compile-time only),
  `hickory-resolver`, `hickory-proto` and `hickory-net` 0.26.1 → 0.26.3,
  `tokio-rustls` 0.26.4 → 0.26.5, `toml` 1.1.4 → 1.1.6.
- CI only: `taiki-e/install-action` updated in the GitHub Actions workflows.

## 0.27.1 - 2026-09-12

### Documented

- **A stored `Decimal128` is matched wrongly by a numeric filter, and `$ne` can
  never exclude it.** Every path that puts a `Decimal128` *in a query* already
  refuses it. The value *in a document* was never covered: a filter whose
  operand is an ordinary number still compares against it, and that comparison
  answers **equal — to every number**. So such a document matches `$eq`, `$gte`,
  `$lte` and `$in` against any number, fails `$gt` and `$lt`, and — the one that
  bites — **is never excluded by `$ne`**, so it cannot be filtered out the
  ordinary way.

  Nothing changes in this release except that the behaviour is now written down,
  with the full matrix, in `query-language.md` and `key-encoding.md`. **It is
  documented as a known defect, not as the contract**: the intended behaviour is
  for a stored `Decimal128` to have a real position among the numbers, and
  giving it one means exact decimal-to-binary comparison that deserves its own
  release rather than riding this one. Until then, do not store a `Decimal128`
  in a field you filter on numerically; `$type: "decimal"` and `$exists` still
  find the ones you have.

- **A snapshot does not carry document deletions, and that is now recorded as a
  known defect.** A node caught up by snapshot keeps any document it held that
  the sender had since deleted — the page carries live documents only, and
  completing the snapshot grants coverage of the sender's history, so the
  deletion is never replayed to it afterwards. Nothing changes in this release
  except that it is written down, in ADR-162's residual, along with why the
  divergence check can still see it: the document counts disagree. Two comments
  in the source claimed the opposite — that deletions travel, and that skipping
  them is safe — and have been corrected (ADR-166).

### Fixed

- **A whole-database snapshot page could destroy a collection on the receiver
  that its sender had never dropped — permanently.** A page carries the
  sender's collection tombstones so it can convey absence, and the receiver
  applied each one after a single check: is this stamp below the incarnation
  standing here. A stamp far enough in the future is below nothing, so one
  malformed entry took an arbitrary collection — including one the sender held
  live on the same page, or one it had never heard of. Nothing reported it, no
  stamp the cluster can mint recreated the name afterwards, the tombstone was
  never collected, and the victim re-broadcast it on every page it served.

  A receiver now honours a denial only where the sender's own advertised
  coverage names its stamp — a legitimate tombstone always is, because the
  sender absorbed the drop in position — and logs and ignores the rest
  (ADR-163). **This needs no misbehaving peer:** the clock has no drift clamp,
  so a node with a misconfigured wall clock mints such stamps through ordinary
  code and pulls the peers it talks to up to them. The feature is new in this
  release and unreleased, so no deployed cluster was exposed.

- **A node catching up by snapshot burned minutes of CPU deciding that pages
  had nothing to do.** Every page of a whole-database snapshot carries the
  sender's collection tombstones, and the receiver looked each one up with a
  full scan of its collection table — once per tombstone, per page. At 600
  tombstones against 600 collections a page with **nothing to do** cost 161 ms;
  it is now 0.5 ms, and no longer grows with the product of the two (ADR-164).
  The effect fell on the node least able to take it, since a node taking a
  whole-database snapshot is the one that had fallen behind.

- **A repair of one collection threw away a whole-database snapshot's resume
  point.** Where a snapshot pull stands is now persisted so a restart resumes
  it, but the record was keyed by peer while the pull it belonged to was only
  named inside it. A scoped repair finishing against the same peer cleared the
  row, and a catch-up thousands of pages in began again at page one. The pairing
  is the common one, not a corner: a node far enough behind to need a snapshot
  is the node whose divergence check is firing repairs at that same peer. A pull
  now clears only a record of its own, and a repair does not take a
  whole-database pull's place (ADR-165). Bookkeeping only — the pages already
  applied stayed applied.

- **The backup route's own documentation said it streams. It does not.**
  `openapi.yaml` summarised `GET /v1/admin/backup` as *"Stream a consistent
  backup"*, while the route builds the whole backup in a `Vec<u8>` before
  sending any of it — and `operations.md` said so correctly all along. The two
  contradicted each other on **exactly the property that decides the memory
  cost**: peak resident memory on the member is the size of the finished backup
  on top of everything else it is holding, and the request is what fails if that
  does not fit. Round 0270 measured one taking 97% of a member's 2 GiB limit.

  No behaviour changes. What changes is that both documents now say the same
  thing, and both say what it costs and what to watch
  (`kimmy_process_resident_peak_bytes`, which is latched — a five-second sampler
  will miss the spike).

- **`check-config` printed `cluster.cluster_secret` in clear.** Every other
  secret — `auth.root_password`, `auth.jwt_secret`, `auth.jwt_previous_secret` —
  was redacted; this one was not, and `check-config` output is meant to be read
  by a person: in a terminal, in CI output, pasted into a bug report. Anyone who
  read it could join the cluster as a peer and be served every document in it.

  It is redacted now, **and `<redacted>` is refused as a secret** wherever a
  placeholder is. That second half is what makes the first safe: pasting
  `check-config` output back into a config file must fail at startup rather than
  run the node with the placeholder, and `cluster_secret` has no minimum length
  to catch it the way `jwt_secret` incidentally did.

- **A node with OIDC configured told operators to run a flag that does not
  exist.** Starting with protected resource metadata published, `kimmyd`
  printed that `kimmy login --oidc --url ...` needs no other configuration.
  `kimmy login` has no `--oidc` flag — it federates by default — so the
  suggested command fails to parse. It now prints `kimmy login --url ...`.

- **`kimmy_sync_ddl_declined_total` rose on a healthy cluster, and its own
  diagnostic then sent you looking for a problem that was not there.** The
  counter is read as *"this member is holding an index its peers have dropped"*,
  and `operations.md` told whoever saw it rise to look for a member whose clock
  ran ahead. But two different things reached that branch, and only one of them
  is news: a drop re-served past the recreation it preceded is the rule working
  correctly, and it was being counted alongside the case worth investigating.
  Round 0270 reproduced the rise deterministically over three iterations.

  A member that applied a drop when the drop was current holds a tombstone
  proving it, so the replay is now recognised and **not** counted — it is logged
  at `debug` instead. What is still counted is a drop the member has never seen,
  which is the case the diagnostic was written for. **No data outcome changes**:
  the drop was declined before and is declined now, and the index standing under
  the name is untouched either way.

- **A collection you dropped no longer survives on a member that catches up
  from far behind.** 0.27.0's release notes recorded one route as not closed:
  a whole-database snapshot — what a member below a peer's retention horizon
  pulls — carried no collection drops, so a member that took one kept a
  collection the sender had deleted, live and writable, with nothing reporting
  the disagreement. Completing the snapshot also granted that member coverage
  of the sender's history, so the drop it never applied was not served to it
  afterwards either: the collection stayed, indefinitely, on one member alone.

  **That route is now closed.** Every page of a whole-database snapshot carries
  the sender's collection tombstones, and the receiver applies each through the
  same incarnation check the rest of the drop machinery uses — so a collection
  recreated after a drop is not destroyed by the headstone of the life before
  it.

  **Nothing to decide before upgrading, and no stop in the roll.** The field is
  defaulted: a member that has not rolled yet sends no denials and ignores the
  ones it receives, so a pair keeps the previous behaviour until both ends have
  rolled and gains the fix the moment they have.

  **Two limits, unchanged and worth knowing.** A collection the sender never
  held at all is not denied — there is nothing to deny it with — and a tombstone
  that has aged past `tombstone_retention_secs` on the sender is gone and cannot
  be carried. As ever, that setting must exceed the longest partition you intend
  to survive. See ADR-162.

- **A restart part-way through a snapshot no longer claims coverage the node
  does not have.** A snapshot document is applied as *state*, out of stamp
  order, and deliberately looks like an ordinary replicated write once it is in
  the oplog — so `Engine::open`, rebuilding the version vector from the oplog,
  could not tell it from history and raised the node's position over it. A
  member restarted mid-repair came back up advertising that it could serve a
  contiguous window it had never seen, and a peer catching up from it would
  have been served a silent gap.

  Such entries are now recorded in a node-local table and skipped by the
  open-time rebuild. Nothing on the wire changes and no negotiation is needed;
  a peer running an older build is unaffected.

  Two consequences worth knowing. A member that took documents from a snapshot
  that **never completed** will not offer that range to a peer until the
  snapshot finishes — the peer is sent state instead of history, which is the
  designed fallback, and the member does not re-request anything in a loop. And
  a backup taken **while a snapshot was running** does not carry the table, so
  a node restored from one behaves as it did before this fix; a backup of a
  settled member has nothing to carry. See ADR-160.

- **A sync round that ran out of time no longer reports itself as a malformed
  frame.** Four bounded waits — the handshake (twice), a push, and a whole sync
  round — mapped a timeout onto `malformed frame: … timed out`, so round 0270
  logged `malformed frame: sync round timed out`. The two conditions send an
  operator to opposite places: a malformed frame is a wire problem, meaning
  peer builds, version skew and capability negotiation, while a timeout means
  load, a stalled disk, a saturated link or a wedged peer — none of which is
  visible in a frame. They are now a distinct error, and a timeout says only
  that it timed out.

  `kimmy_sync_failures_total` counts a failed round whatever the cause, as
  before; what changes is the line beside it.

### Changed

- **A snapshot interrupted by a restart now resumes where it left off.** A
  snapshot that does not fit one round already resumed across rounds, but the
  progress lived only in memory — so a member restarted part-way through a large
  transfer began again at page one and re-sent everything it had already applied.
  It is now recorded, in the page's own transaction, so a snapshot still costs no
  commit of bookkeeping.

  The bound, exactly: a restart resumes at **the last page that wrote
  something**. Pages that were no-ops are re-pulled, and they are cheap to redo
  precisely because they wrote nothing. A node **restored from a backup** starts
  any in-flight snapshot again rather than resuming it — a restore is already a
  full rebuild, and this is a cost of the same kind. See ADR-161.

## 0.27.0 - 2026-09-09

0.26.1 was never tagged; 0.27.0 shipped its content the same day, folded in below.

### Added

- **The writer hold now says what held it.** `kimmy_write_lock_held_seconds{holder}`
  is a histogram of how long each transaction held the single writer, split by
  what it was doing: `write`, `bulk`, `ddl`, `index_build`, `drop`,
  `replication`, `repair`, `retention`, `expiry`, `embedding`, `durability`,
  `rewind`. `kimmy_write_lock_wait_seconds` has always counted the *effect* — a
  hold shows there on every other write that queues behind it — and
  `kimmy_write_lock_held_seconds_max` gives one number about one moment with no
  name attached, so a retention pass, a replicated batch, a collection drop and
  an ordinary client write were one undifferentiated fact. On a three-member
  test cluster, a sustained-load round left roughly 11,900 writes having waited
  more than five seconds and eighteen transactions having held the writer for
  more than five seconds, and nothing on the page or in the log could say which
  of the twelve those eighteen were. Each holder's total time and hold count are
  on the OTLP bridge as `kimmy.write_lock.held_seconds.<holder>` and
  `kimmy.write_lock.holds.<holder>`; the buckets stay on `/metrics` alone, with
  the other two histograms and for the same reason. The `5`-second bucket is the
  threshold the existing `WARN` fires at, so a hold counted above it has a log
  line naming the same holder — and that line now names a holder the write path
  declares rather than whatever tracing span happened to be current, which for
  every client operation was the single word `db.operation` and for every
  background pass was `none`. The since-start maximum and the `WARN` itself are
  unchanged ([ADR-159](docs/decisions.md) has the holders, the buckets and why
  a hold is attributed to the work rather than to who asked for it).

  **Nothing to decide before upgrading.** One new `/metrics` series and no
  removals, no HTTP shape changed, no protocol field added. A dashboard or scrape
  config built on the three existing writer series is untouched.

### Changed

- **Dropping a collection no longer stops every other write on the member.**
  A drop removed the whole collection in one write transaction, and redb has a
  single writer: measured on a three-member cluster, 62 seconds for 261,000
  small documents and 238 seconds for a 12,829-row vector shadow, for the whole
  of which every client write on that member queued and, past
  `server.request_timeout_secs`, was refused. The same round saw members
  briefly leave the cluster during a large drop and one member's version
  endpoint take eight seconds while its container was healthy. A drop is now
  two stages: one short transaction removes the definition, records the
  tombstone and mints the entry, and everything the collection held is removed
  after it a thousand rows per commit — the vector shadow in the same chunks —
  with the writer released between them. The drop still takes as long as it
  takes, and now only its own caller waits for it
  ([ADR-158](docs/decisions.md)).

  **What a client sees is unchanged, deliberately.** The tombstone and the
  removal of the definition are the drop's *first* commit, so from that instant
  the collection is gone to every reader and every peer — not listed, not
  served, not reported by the divergence check, and not brought back by a
  repair from a member that has not applied the drop yet. There is no window in
  which it reads as intact and missing most of its documents, which would have
  been a count divergence, a repair, and the collection back: the state
  [ADR-155](docs/decisions.md) was written about. `DELETE` still answers when
  the removal is finished, so size a client timeout for a large drop and issue
  it once — a retry answers `{"dropped": false}` for a drop already under way.

  **Nothing to do before or after upgrading.** No wire field, no message, no
  HTTP shape and no new `/metrics` series. A member restarted part-way through
  a drop finishes the removal at its next start, **before it binds** — so that
  start can take up to what was left of the drop, and it now logs the
  collections it owes and their row counts before it begins rather than only
  when it ends. The drop itself was durable and replicated before the first row
  went. On a member that stays up, a `WARN` saying a chunk of a drop gave up
  waiting for the single writer means the collection is dropped and the rows
  are removed at the next start — provided the collection tombstone outlives
  the process, since that is what the start reads to find them; past
  `storage.tombstone_retention_secs` the rows are unreachable disk rather than
  data, cleared only by a collection created under the same name. Like every
  wait of that length, the `WARN` is a member whose writer is held for longer
  than a request is allowed to take.

- **A sync tick drains what it can from a peer instead of one batch per
  interval.** A member more than 1,024 entries behind used to pull one batch
  per peer per `cluster.sync_interval_secs` and sit idle until the next tick,
  which capped catch-up near 205 entries a second per peer whatever the wire
  or the writer could do: a sustained-ingest round left three members some
  250,000 documents apart and took about seventy minutes to drain. A tick now
  keeps pulling from a peer while the pull before it came back full at the
  cap, round-robin across the peers it contacted, until every pull comes back
  short of the cap or the tick has spent its own interval. The batch cap and
  the frame limit are unchanged — raising them would only move the work into a
  longer hold on the single writer — and neither is the interval, which is now
  what bounds a tick's own length rather than what bounds a drain: another
  pull is started only when the pull before it would have fitted in what is
  left of the interval, so a draining tick stops short of its own period
  instead of one pull past it, and ADR-154's overrun `WARN` goes on meaning a
  tick that was stuck rather than a tick that was busy
  ([ADR-157](docs/decisions.md)).

  **The divergence counters keep their meanings, and one reading gets deeper.**
  `kimmy_sync_divergence_checks_total` is still one `ran` or one `skipped` per
  peer per tick, decided by the tick's *last* pull at that peer: short of the
  cap means the check ran on it, out of budget while still truncated means one
  skip. The pulls in between move neither counter. So a rising `skipped` now
  means a backlog that outlasted a whole tick's worth of pulls rather than one
  pull — a strictly deeper backlog than before — and the check comes back as
  soon as the backlog drains rather than after it. Failures are untouched: a
  failed pull ends the tick's contact with that peer, backs it off as before,
  and is never retried inside the tick. The `merged from peer` line and the
  `cluster.sync` span are now one per peer per tick, with `pulls` on the line
  saying how many the tick made, so a drain reads as one line rather than a
  dozen; `kimmy_sync_repair_rounds_total` counts those pulls, so its rate
  reads higher during a drain. ADR-148's repair cooldown is counted per
  contact rather than per pull, so "sixty rounds" is still the five minutes
  its constant is argued in. No new `/metrics` series, nothing on the wire,
  and nothing to decide before upgrading.

- **A release ships Linux archives only, and the Homebrew tap stops
  updating.** `aarch64-apple-darwin` is paused, so a tag attaches no macOS
  archives and no bills of materials for them, and no formula is generated or
  pushed to `titusai-io/homebrew-tap` — a formula built from a release with no
  macOS binary pins Linux musl archives and installs nothing a Mac can run,
  which is worse than publishing none. Unchanged: `kimmyd` and `kimmy` for
  `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl`, each with its
  checksum and its CycloneDX bill, `source.tar.gz`, and the multi-arch
  container image at `ghcr.io/titusai-io/kimmydb` — the image is built from
  those Linux archives, so it is exactly what it was. On a Mac,
  `cargo install --git https://github.com/titusai-io/kimmydb kimmy-cli` builds
  the CLI and the container image runs the server; `brew install
  titusai-io/tap/kimmy` still resolves, but to the last version that published
  a formula, and it will not move while this holds. The build is paused rather
  than retired ([ADR-156](docs/decisions.md) has the measurements and exactly
  how to bring it back).

### Fixed

- **A dropped collection came back from a peer that had not applied the drop
  yet, with all its documents.** On a converged three-member cluster a database
  drop answered `200 {"dropped": true}` and, minutes later, its collection was
  back on all three members holding all 48,128 documents — twice, the second
  time as the repair of the first. Nothing failed: no round, no handshake, and
  `kimmy_replication_lag_seconds` at 0 on every member throughout. A drop
  replicates as a change like any other, and while one was in flight the
  cross-member divergence check read a peer that had not applied it yet as
  divergent — which it is not, it is behind — so anti-entropy "repaired" the
  difference by bringing the collection back and re-seeding it onto the members
  that had it right. The collection tombstones that would have stopped it were
  on every member the whole time: the replication entries path consulted them,
  and the divergence check and the snapshot repair, written beside that path,
  never did. Both do now, through the same predicates that path uses, so a drop
  applied on one member is not undone by the machinery that converges the
  cluster ([ADR-155](docs/decisions.md) has the rules and what they cost).

  **Nothing to decide before upgrading, and no stop in the roll.** The two new
  protocol fields are defaulted, and the drop a repair's snapshot now carries on
  every page rather than the first alone adds no field at all — a page has
  carried an optional drop stamp since 0.26.0, and only when a sender fills it
  changed. No new `/metrics` series, no HTTP shape changed. While a roll is in
  progress, a member not yet upgraded names no incarnation for what it holds and
  is read as holding the incarnation its peer dropped, so a collection genuinely
  recreated on it, whose creation has aged out of its oplog, is not pulled back
  by the check until the roll finishes or the tombstone expires — the ordinary
  recreation is unaffected, arriving by the entries path as before. And such a
  member part-way through a repair ignores a drop for a collection it still
  holds, as it always did, so that half closes for a pair only once both ends
  have rolled; it gains nothing there and loses nothing.

  **One route is not closed, and it is worth a check after any member catches up
  from far behind.** A whole-database snapshot — what a member below a peer's
  retention horizon pulls — carries no collection drops, and completing one
  grants that member coverage of the sender's history, so the drop it never
  applied is not served to it afterwards either. It keeps the collection, live
  and writable, and nothing reports the disagreement, because a member's own
  check only reports collections a *peer* holds and it does not. The members
  that applied the drop no longer pull it straight back, which was the
  re-seeding half of the finding — but when their tombstones expire the check
  reports the collection again and repairs it from the member still holding it.
  So compare the collection list of any member that logs `behind the peer's
  retention horizon; falling back to a snapshot` against a member that stayed up,
  and drop anything only it holds. That warning marks one of the two ways in, and
  the `caught up from a snapshot` line after it is logged for a one-collection
  repair too, so check on suspicion as well for a member that has been repairing
  against a peer over many rounds: a repair that falls back to the whole database
  reaches the same state and nothing names it.

## 0.26.0 - 2026-09-08

### Fixed

- **A retention pass no longer holds the single writer for a walk of the
  database, and passes no longer run back to back.** Every
  `storage.gc_interval_secs` the pass collected expired oplog entries and
  tombstones by walking the *whole* oplog table and then the *whole* document
  table inside write transactions — at the default retention of a day, a walk
  that removed nothing, logged nothing, and still read every page of both
  tables while every other write on the node waited. On a member whose
  container had no page-cache room for its file, one pass took longer than the
  interval, the scheduler fired the missed tick at once, and the writer was
  held almost continuously: single-document writes timed out for over an hour
  on two of three members while reads stayed under a second and not one line
  was logged ([ADR-151](docs/decisions.md)). The oplog is now collected by key
  range, so nothing past the expired prefix is read; tombstones are found by a
  scan under a read transaction that visits at most 100,000 documents per pass
  and resumes where it stopped; both are removed in chunks of 1,000 per commit
  with the writer released between chunks; a pass that overruns the interval
  is followed by the next a full interval later, and is logged at `WARN`.
- **A write that cannot take the storage writer inside
  `server.request_timeout_secs` is refused with the documented `503 timeout`
  (retry: `wait`) instead of hanging for as long as the writer is held.** The
  request timeout could not do this on its own: a handler blocked waiting for
  the writer never yields, so the deadline was only seen once the wait ended,
  and a client saw a transport timeout with nothing logged. The wait itself is
  now bounded to the request timeout on the request path; replication,
  retention, TTL and the embedding worker wait as long as it takes, as before.
  Writers now queue with eventual fairness, so a path that takes and releases
  the writer in a tight loop hands it over rather than winning every time.
- **A snapshot repair now completes on a large database: it pulls the one
  collection it was planned for, applies a page per commit, and resumes
  across rounds from the last page applied.** A repair planned for one
  divergent collection pulled the peer's *whole* database, one write
  transaction per document, inside the 30-second round timeout — and a
  snapshot that could not finish in 30 s was cut off mid-page and started
  again from page one on the next round, three times, then cooled down for
  five minutes and was planned again. On a three-member test cluster one
  member spent 108 repair rounds in six minutes with its lag still growing
  ([ADR-152](docs/decisions.md)). A snapshot is now scoped to the collection
  being repaired (`AskSnapshot` carries the collection; a peer on an earlier
  release ignores it and serves the whole database, so a rolling upgrade
  needs no stop), every page is one transaction — none at all for a page the
  member already holds — and where the pull stands is kept between rounds, so
  a round that runs out of budget leaves its pages applied and the next round
  continues from its cursor; only three rounds that apply *no* page abandon a
  repair. The whole-database snapshot a member below a peer's retention
  horizon pulls resumes the same way. Two rules changed with it, on purpose:
  the coverage a completed snapshot grants is the peer's vector as served
  with its *first* page, and a snapshot's documents no longer move the
  receiver's version vectors as they land — the previous form could carry the
  receiver's position past a document the peer wrote behind the cursor while
  the snapshot ran, which nothing then re-served. One residual is recorded
  rather than closed: a member restarted *during* a multi-round snapshot
  forgets where it stood and, on opening, re-derives its position from the
  snapshot documents it already holds, which can carry it past the un-walked
  remainder (on a repair, past the stopped window too) — narrower than
  before, when a timed-out snapshot left the same state on every round, and
  the subject of a separate decision. A snapshot of a collection
  the peer has since dropped carries the drop, so a member stopped at entries
  for it records the tombstone rather than stopping forever. The round logs
  one `INFO` line when it leaves a snapshot to resume, with pages, documents
  and the cursor.

- **`kimmy_sync_divergence_check_age_seconds` is computed when it is read,
  so a stuck anti-entropy loop cannot freeze it.** The age exists to say
  that `kimmy_sync_divergent_collections` is serving a stale reading, but it
  was computed by the loop at the end of each tick and pushed with the gauge
  — so a loop whose tick did not end froze the age with everything else. On
  the three-member cluster above, the two members whose loops waited on the
  writer for over an hour read the same age on every scrape, `ran` flat,
  nothing failing and nothing backed off, while one of them fell some 30,000
  documents behind with the gauge at 0; the documented alert on the age
  never crossed its threshold. The loop now reports the instant of the last
  check and every scrape and OTLP export subtracts it from its own clock, so
  the age rises through a stuck tick exactly as it rises through a run of
  failed rounds. Every documented reading holds: 0 before the first check
  beside `ran` at 0, reset by a check, unmoved by a failed round. A sync
  tick that took longer than `cluster.sync_interval_secs` is logged at
  `WARN` when it ends, and is followed by one tick at once and then the
  next a full interval later, rather than by a burst of every tick it
  missed — an hour-long tick at the default interval used to be followed by
  some 720 rounds fired back to back; the discovery ticker gets the same
  treatment
  ([ADR-154](docs/decisions.md)).
- **A read that walks a collection or an index range no longer holds an
  async worker while it walks, so `/metrics`, `/healthz`, `/v1/version` and
  small reads keep answering while a node is saturated with scans.** `find`,
  `count` and `aggregate` ran their scan inline on a tokio worker, and a scan
  never yields: with as many scans in flight as the runtime has workers (one
  per core), nothing else on the node ran — not a scrape, not a one-document
  `find`, not the request timeout that should have abandoned the request. In
  a load round, seventeen scrapes of `/metrics` across a three-member test
  cluster failed exactly while sixty-four clients counted a 30,000-document
  collection. Reproduced on a two-worker runtime over 300,000 documents: ten
  of fourteen scrapes past a 5 s timeout, and `/v1/version` and a
  `limit: 1` `find` failed the same way. The walk now runs under the same
  `block_in_place` the writer's wait already uses, so the worker hands its
  queue to another thread first; the same cell afterwards answered every
  scrape in 0.8 ms at the median, and `count` throughput was unchanged on a
  runtime with spare workers and rose 8.7 → 21.3 requests/s on one without
  ([ADR-153](docs/decisions.md)). Reads by `_id` are unchanged: a primary-key
  probe is one page and stays on the worker.
- **Opening a database no longer walks the whole oplog twice to decide that
  its arrival index is current.** The check compared the two tables' lengths
  by iterating each to its end, so every start read the entire oplog and the
  entire index through the page cache before the node served a request — at a
  4 GiB file, seconds of disk reads at start and a page cache already full of
  oplog pages by the time the first client arrived. It now reads the length
  redb keeps in each table's header. A third walk at open, the one that
  raises the version vector over the oplog, remains and is recorded in
  [ADR-153](docs/decisions.md) with the measurement.

### Added

- **The wait for the single writer is measured.** `kimmy_write_lock_wait_seconds`
  is a histogram of how long each write transaction waited before it could
  begin — the part of a write's latency that is not its own work, which
  nothing else on `/metrics` separated out; `kimmy_write_lock_wait_timeouts_total`
  counts the writes refused for waiting past their budget; and
  `kimmy_write_lock_held_seconds_max` is the longest any one transaction has
  held the writer since start. A transaction that holds the writer longer than
  five seconds is logged at `WARN` when it lets go, naming the operation it ran
  under. The two summaries are on the OTLP bridge; the histogram is a
  synchronous instrument and stays off it with the latency histogram, for the
  reason recorded there.

## 0.25.1 - 2026-09-08

### Fixed

- **A `find` page now holds what it returns.** The projection was applied to
  the page after it was assembled, so a read that asked for `{"_id": 1}` still
  held every stored document of the page while it built the answer — and a
  sorted read held them for the whole `skip + limit` window. Over a collection
  of large documents that is the difference between a list of ids and
  gigabytes: a vector chunk's embedding is `dim × 8` bytes stored and about
  fourteen times that decoded, so one `limit: 10000` page over a `__vectors`
  shadow collection at 4,096 dimensions took a member from 690 MiB to 2,034
  MiB of a 2,048 MiB limit and ended it. The projection is now applied where
  each match is visited, and the sort window holds each match's sort keys and
  its projected document instead of the stored one. Pages, cursors, sort order
  and refusals are unchanged — including a walk whose projection drops `_id`,
  whose cursor is now taken from the scan rather than from the projected page.
  Measured under a counting allocator, a 1,000-document page of ids fell from
  438.7 MiB held to 1.3 MiB ([ADR-150](docs/decisions.md)). Reading a vector
  shadow collection is worth projecting and paging;
  [Vectors](docs/vectors.md#what-a-page-over-the-shadow-collection-costs) has
  the arithmetic.

- **The metrics table in the operations guide is now held to what `/metrics`
  actually renders.** The section promises that every series the endpoint
  exposes has a row, and nothing in this repository checked it. It has been
  wrong once: fourteen series were exposed with no row until the 0.20.0 test
  round found them, a release after the last of them shipped, which is a
  release in which nobody could have built a dashboard on the embedding
  worker, the webhook block or either TTL counter without reading the source.
  Every series since has landed with its row in the same commit
  ([ADR-145](docs/decisions.md), [ADR-147](docs/decisions.md),
  [ADR-148](docs/decisions.md) each did), but nothing required it. A test now
  compares the rendered exposition against the table in both directions — a
  series with no row fails, a row with no series fails — and requires every
  label key a series carries to be named in its row, so a dimension you could
  split on cannot go unpublished. It reads label keys and not their values, so
  the values a row enumerates are still prose. The table is complete and
  correct today; what was missing was anything that would notice when it
  stopped being.

### Documentation

- **The query-language page no longer says a `$expr` literal is refused with
  the filter message.** It listed "a literal anywhere in a `$expr`
  expression" among the positions refused as *cannot be compared in a
  filter*; the server hands `$expr` to the expression parser, so the refusal
  is *a Decimal128 literal is not supported in an expression*, which the HTTP
  API and aggregation pages already said. The sentence now says so and points
  at them. Found by the 0.25.0 test round's Decimal128 cases, which assert
  the message and not only the status.

## 0.25.0 - 2026-09-07

### Changed

- **The vector-index consumer reconciles a replicated drop with one keyed
  read rather than two walks of the catalogue.** A drop entry carries the
  dropped collection's database and name, and the graph a drop strands is
  keyed by the shadow collection whose id derives from that name, so the
  consumer now reads that one row instead of scanning every database twice
  per entry. On a node holding thousands of collections, that scanning was
  what let a `drop_database` push the consumer behind the change feed and
  into a full reconciliation. The reconciliation itself, and the startup
  snapshot sweep, now list the catalogue in one read transaction rather than
  one per database.

### Fixed

- **A replica could witness a run of documents it was never served, and no
  round, counter or gauge said so.** A three-member cluster finished a bulk
  load holding 142,026 / 142,026 / 141,746 documents in one collection: one
  member was missing 280 documents, in three contiguous runs of one member's
  writes, and stayed missing them through two hours of anti-entropy, a hard
  kill, a restart and a full rolling restart. No sync round failed, no
  connection failed, no schema change was refused, and
  `kimmy_replication_lag_seconds` read 0 on every member throughout — a hole
  in a member's *position* reads as perfect health, by construction, because
  the position is what the gauge is measured from.

  Two things caused it, and both are fixed. A peer answers "what do you
  hold" and "give me your entries" in two separate reads and keeps
  replicating in between; under load that gap is seconds, and an entry the
  peer appended in it arrives in the window *above* the vector the peer
  advertised. Absorbing it raised the receiver's position for that entry's
  origin over everything of that origin between where the receiver really
  stood and where the window began — which the window never carried, and
  which nothing asks for again. A window is now trusted only as far as the
  vector that introduced it: entries above it are left, and the next window
  carries them. Separately, several write paths minted an entry's stamp
  *before* waiting for the storage writer, so under a long writer queue an
  entry could commit below entries already served to a peer; every path now
  mints inside the transaction that carries the entry, which is what makes a
  member's advertised vector a promise its own oplog keeps.

  A third hole is closed with them: a replicated document for a collection
  this member has no record of — it neither holds the collection nor a
  tombstone for it — was counted and skipped for good. It is now left: the
  batch stops at it, nothing past it is witnessed, the window is re-served,
  and the member pulls a snapshot from the peer to bring the collection,
  because unlike a schema change this member cannot apply, that entry
  succeeds the moment the collection arrives. A collection this member
  *dropped* is history as it always was, whichever way the stamps fall — so
  an ordinary concurrent drop-and-write costs one superseded entry and
  stops nothing. The warning names the collection, which it did not
  ([ADR-148](docs/decisions.md)).

- **A confirmed divergence is now repaired, not only reported.**
  `kimmy_sync_divergent_collections` correctly read 1 on all three members
  of the cluster above and nothing acted on it: anti-entropy went on asking
  from a position already past the missing documents. A collection the
  cross-member check confirms against a peer is now re-served from that
  collection's creation on the next round with that peer, window by window
  until one reaches the peer's tail — or pulled as a snapshot when this
  member does not hold the collection at all. This is also what repairs a
  member upgraded into this release while already holding a hole: the check
  finds it, the repair closes it, and the gauge returns to 0. A repaired
  collection is not repaired again until the check reports it clear or five
  minutes of rounds have passed.

  What it does not reach, said plainly: the check compares this member against
  a peer, so a run of documents missing on *two* of three members leaves all
  three counts agreeing — the gauge stays at 0, nothing is repaired, and the
  cluster reads as converged. If you have independent reason to suspect a
  loss, compare the members' counts directly; that case still needs a restore
  or a re-seed ([ADR-148](docs/decisions.md)).

- **A dropped collection now takes its vector index with it, and a snapshot
  can no longer outlive the collection it describes.** Dropping a collection
  or a database removed the documents and the vectors, but nothing told the
  index cache, so the HNSW graph built over those vectors stayed in memory and
  its snapshot stayed on disk for the life of the process — for data that no
  longer existed. The graph is keyed by the shadow collection that holds the
  vectors, and that shadow is removed in the same transaction as its parent,
  so after the drop there was no id left to forget it under. A member with no
  user data at all was seen holding 353 MiB of resident graphs and 280 MiB of
  snapshots twelve hours after the last collection had been dropped, stable
  rather than draining, against peers reporting a cache of zero. Both drops now
  resolve the shadows beforehand and forget them afterwards, a database drop
  forgetting every vector-enabled collection it removes rather than one.

  **The replicated half is closed too, and it is the half that showed on a
  cluster.** A drop that arrives from a peer is applied by the sync path, which
  runs no route, so only the member the request was typed at forgot anything
  and every other member kept the graph — which is why the member that had
  owned the vectors was the one carrying the memory. Each node now runs a
  consumer over the change feed it already publishes, alongside the one that
  keeps token state honest. It reads committed entries only and cannot affect
  what a sync round applies, witnesses or reports. A drop reaches it as
  history rather than as news, so it treats the entry as a question — is this
  collection still here? — and forgets nothing that is.

  **A recreated collection no longer inherits its predecessor's graph.** A
  collection id is derived from its name, so a name dropped and used again gets
  the same id, the same cache key and the same snapshot path. An orphaned
  snapshot sitting there was adopted on the new collection's first search, and
  nothing about its shape gave it away: same metric, same width, and if the
  vector counts happened to agree it was adopted as *fresh* — serving until a
  vector write bumped the generation, which on a collection that is only read
  is indefinitely, and again after every restart. The resident graph had the
  same hole and a wider one: the generation counter is keyed by the derived id
  and only ever counts up, so a recreated collection with nothing written to it
  yet matched the entry its predecessor left behind, and the staleness window
  never opened because nothing looked stale. No wrong data could come of
  either, because scores are recomputed from the stored vectors and candidates
  are resolved against the live collection; but recall was not merely reduced,
  and where the recreated collection used different document ids **every
  candidate resolved to nothing and search returned empty over thousands of
  live vectors**. A graph now records the incarnation it was built for, on disk
  and in memory, and is refused when that does not match — exactly as a
  snapshot from a different metric or width already was. This also covers the
  case no consumer can: a node that resyncs past the oplog horizon never sees
  the drop entry at all.

  **Snapshot format 2.** Because the incarnation is a new field, every snapshot
  written by an earlier build is refused and rebuilt on the first search that
  wants it — the ordinary cost of a format change. And because a format
  mismatch is only noticed when something opens the directory, which never
  happens for a collection nobody recreates, each node now sweeps its `hnsw/`
  directory once at startup and removes snapshots it holds no collection for —
  including the staging directory a build interrupted by a crash leaves behind,
  which nothing else would ever clear. A name it cannot read as a collection id
  is logged and left alone.
  **Upgrading therefore disposes of snapshots stranded by earlier drops**, at
  the cost of one rebuild per collection on its next search.

  **Storing vectors for a document no longer discards the collection's graph.**
  `PUT .../docs/{id}/vectors` and its delete dropped the cached graph and, with
  it, the snapshot — once per document written. The write already bumps the
  collection's vector generation, which is what the search path reads to notice
  it and is all the embedding worker does for the identical write, so the BYO
  path now behaves as the worker path and as [Vectors](docs/vectors.md)
  describes. Keeping the snapshot exposed an edge the worker path had always
  had: a graph evicted under `vector.index_cache.max_bytes` leaves its snapshot
  behind, re-embedding one document usually replaces its chunks with the same
  number of chunks, and the next search would reload that snapshot and, seeing
  the vector count unchanged, adopt a graph from *before* the write as fresh at
  the generation *after* it. The cache now remembers the generation it wrote
  each snapshot at and refuses to call one fresh once the collection has been
  written to since, whichever path wrote it; a snapshot from a previous process
  keeps the count check, as before.

  What an operator should expect: `kimmy_vector_index_cache_bytes` falls to the
  graphs of collections that still exist, within a replication round on every
  member rather than on one; the `hnsw/` directory beside the database file
  loses the dropped collections' snapshots at the next start; and the first
  search on each surviving vector collection after the upgrade pays one
  rebuild.

- **A Decimal128 document is filed unkeyed on the member that accepts it, not
  only on its peers.** A document holding a `Decimal128` at an indexed path,
  written over HTTP, was stored on the accepting member and filed under a
  *real* index key there — `unkeyed` stayed 0, no warning, no
  `kimmy_index_unkeyed_total` — while both peers, applying the same bytes
  through replication, filed it unkeyed as [Indexes](docs/indexes.md) promise.
  The other two unkeyable shapes were filed correctly everywhere. The cause
  was the JSON edge: it emitted `$numberDecimal` but never read it, so the
  wrapper arrived as a nested document, which an index keys happily, while
  every re-decode of the stored bytes — the backfill, a replicated apply, the
  old image on a replace, update or delete — yielded a Decimal128. Three
  things followed on the acceptor: insert-then-`createIndex` and
  `createIndex`-then-insert disagreed on `unkeyed`; a later replace, update
  or delete unfiled the document from the unkeyed run it was never in and
  left its keyed entry behind; and a unique index accepted a Decimal128 at
  its path instead of the `400` it gives every other document it cannot key.
  The edge now reads `$numberDecimal` — the Extended JSON table in
  [HTTP API](docs/http-api.md) says so — and the value in memory is the value
  its bytes decode to, so the write path, the replicated path and the
  backfill file it one way. Storage is unchanged, and a Decimal128 already
  stored reads back as it always did. **An index entry filed before this fix
  is not repaired by later writes.** Unfiling recomputes the old image's keys
  from its stored bytes, which decode to a Decimal128 and classify unkeyed,
  so every later write to such a document removes an unkeyed entry that was
  never there and leaves the keyed one behind; the document is still found
  correctly, but the orphan is read by every scan of that range until the
  index is **dropped and recreated**. An operator whose members took
  Decimal128 writes through an indexed path on 0.24.0 or earlier should do
  that once per affected index ([ADR-139](docs/decisions.md)).

  Reading the wrapper opened a hazard the fix closes in the same change: the
  canonical order ranks a Decimal128 equal to every other number, so
  `{"v": {"$numberDecimal": "1.5"}}` in a filter — refused through 0.24.0
  only by accident, as `unsupported operator "$numberDecimal"` — would have
  matched every numeric `v`, and a `$pull` of one would have emptied an
  array of its numbers. A Decimal128 is now refused **on purpose** wherever
  a query or an update would compare one **on a caller's behalf** — that is,
  wherever the caller supplies the value being compared. A Decimal128 already
  *stored* is still ranked equal to every number by the canonical order, so
  `{"$pull": {"xs": 1}}` removes a stored Decimal128 element and `$group`'s
  `$min`/`$max` still rank one against the numbers beside it; that gap is
  older than this change and untouched by it. What is refused: as a filter
  operand under any
  comparing operator or inside a literal (`… cannot be compared in a
  filter`), as a literal anywhere in an expression, `$expr` included (`a
  Decimal128 literal is not supported in an expression`), as a partial
  index's bound, as the operand of `$min`, `$max`, `$addToSet`, `$pull` and
  `$pullAll` (`$pull cannot compare a Decimal128 operand`), and as a sort key
  a matching document holds (`cannot sort by "v": document 3 holds a
  Decimal128 there`) — each a `400` that names what to do instead. `$set`
  and `$push` store one intact, and `$type: "decimal"` finds such documents
  without comparing them ([Query language](docs/query-language.md),
  [Key encoding](docs/key-encoding.md)).

- **The aggregation ceiling's refusal no longer tells you to raise a setting
  that does not exist.** A pipeline over the 100,000-document limit was
  refused with "Narrow the pipeline with an earlier `$match`, or raise
  `server.aggregate.max_documents`". There has never been a
  `[server.aggregate]` section, and every section of the configuration file
  rejects fields it does not know — so an operator who followed that advice
  and wrote one into their file got a node that would not start. The message
  turned one refused query into a member that will not come back up. The
  ceiling is fixed by design, as [Aggregation](docs/aggregation.md)
  documents it, and both refusals — the one naming a stage and the one
  naming the source the pipeline reads — now say only the thing that works,
  which is to select less with an earlier `$match`. The example refusal on
  that page was quoted with an ellipsis that fell exactly where the false
  advice began; it now quotes the whole message. A test over the sources
  fails the build if any message names a setting the configuration parser
  would reject.

- **A vector index on a member that does not own the collection's embedding
  now notices the chunks that reach it by replication.** The search path
  decides whether a cached HNSW graph is still fresh by comparing the
  generation it was built at with the collection's vector generation, a
  counter that moves once per vector write; a graph whose generation matches
  is served as fresh, and one whose generation does not is served for at most
  30 seconds before it is rebuilt. Only a local vector write moved the
  counter. A chunk that arrives from a peer is applied by the sync path as a
  document write into the shadow collection, which never did, so on a member
  that does not embed for a collection — where every chunk arrives that way —
  the counter never moved at all. A graph built there matched it forever and
  was served as fresh until the process restarted, the graph was evicted, or
  the collection was dropped. Documents embedded after the build were absent
  from that member's results indefinitely, on exactly the members a
  deployment routes searches to, while the embedding member found them within
  30 seconds. Recall, never wrong data: a candidate is re-scored from the
  stored vector and a missing one is skipped. But the bound the design
  promises did not exist there.

  A replicated write into a shadow collection now moves the generation once
  it has committed — after, as a local write does, because a bump ahead of the
  commit would let a build read the new generation against the old data and
  serve that graph as fresh for a write it does not contain. A re-delivered
  entry, which peers send by design and which writes nothing, does not bump;
  a replicated delete of a chunk does, as a local one always has.

- **Dropping a database now takes a shadow collection that has no parent.**
  `DELETE /v1/db/{db}` dropped each collection and let every vector shadow go
  with its parent, in the parent's transaction. A shadow with no parent — the
  residue [ADR-138](docs/decisions.md) describes, where a parent's drop was
  applied before the shadow's create arrived — had no drop to go with, so it
  was skipped: the request answered `dropped: true` and removed the database
  row, but the shadow was still listed under the database, the next collection
  created there brought the database back with the shadow's chunks already in
  it, a vector-enabled collection recreated under the parent's name adopted
  them, and `kimmy_sync_divergent_collections` kept counting a database the
  operator had been told was gone. The drop now lists what the parent pass
  left behind and drops each such shadow directly, each as its own replicated
  entry, so every member holding the orphan removes it and its vector index
  in the same way it would any other dropped collection
  ([HTTP API](docs/http-api.md)).

- **A reconfigured vector collection no longer keeps serving its old graph
  on every member but the one that took the request.** `POST .../vector` on
  a collection that already had a configuration forgot the cached HNSW graph
  on the member that ran the route and on no other: on every other member the
  change arrived by replication, which runs no route, and the graph built for
  the previous configuration stayed resident with nothing to tell it apart —
  the shadow collection is kept, so the incarnation matched, and no vector is
  written, so the generation matched too. A metric change at the same width
  (the only in-place change a `byo` collection permits) left those members
  ordering every search by the old metric, because a graph scores with the
  metric it was built for, and the difference showed only in which hits came
  first. A width change on a server-embedded collection left them holding a
  graph that refused the query, so every search there was a `400` until the
  graph was evicted or the process restarted.

  Two things changed. The change-feed consumer that already forgets a dropped
  collection's graph now acts on a reconfiguration as well, on every member
  the entry reaches: it reads the configuration in force from the store,
  compares the cached graph's metric and width with it, and forgets the graph
  only on a difference. The comparison matters because the same entry reaches
  the consumer more than once — an anti-entropy window starts at the last
  witnessed stamp, inclusive, so a later round re-delivers it, and forgetting
  on every delivery would have discarded a graph already rebuilt for the new
  configuration each time; a re-delivered entry is now a no-op. And the cache
  itself now records the metric and width a graph was built for and declines
  an entry of another shape at the next search, exactly as it declines one
  from a previous incarnation of the same name — so a search is served by a
  graph of the configuration in force whether or not the consumer has caught
  up, and a "too small" verdict decided at one width is not reused at another.

- **A collection dropped and created again under the same name no longer
  keeps its predecessor's graph in memory or its snapshot on disk.** A
  collection id is derived from its name, so the recreated collection has
  the same id, the same cache key and the same snapshot path, and three
  places that decide what to keep asked only whether the id was in use. The
  change-feed consumer, reading a drop that arrived in the same round as the
  recreate, found a collection under the id and forgot nothing, leaving the
  old graph resident — declined on every search, but never released — and the
  old snapshot in place. The sweep at startup kept every directory whose id
  was live, so a recreated name's predecessor survived every restart, along
  with the `<id>.build` staging directory an interrupted build leaves. And a
  consumer that fell behind the feed reconciled only the graphs it held, so a
  snapshot whose graph had been evicted, or that a previous process wrote,
  was named by nothing. No wrong answers came of any of it — a graph and a
  snapshot both record the incarnation they were built for and are refused
  on a mismatch — but a recreated collection never searched on a member kept
  hundreds of megabytes of orphaned disk there indefinitely.

  All three now reconcile on the incarnation as well as the id, each half on
  its own stamp: a resident graph goes when it was built for another
  incarnation than the one standing under its id, and a snapshot goes when
  its own `meta.json` says the same; a `meta.json` that cannot be read is left
  for the first search to discard, as before. A directory is removed only
  when no build is writing it. The startup sweep removes every staging
  directory, since it runs before any build can begin; a lagging consumer
  now sweeps the disk as well as the cache, and leaves staging directories to
  the builds that may own them.

- **A collection dropped while its vector index was being built no longer
  gets that index installed afterwards.** Building a graph takes seconds at
  realistic sizes and runs on a read view of the store that is fixed when it
  starts, so a drop committing during the build changed nothing the build
  could see: it finished, saved its snapshot and installed the graph under an
  id no collection held any more. Forgetting the collection — from the drop
  route, the change-feed consumer or a reconciliation — removed the entry and
  the snapshot that existed at that moment and did not reach the build in
  progress, so a drop landing inside that window left a resident graph
  charged to `vector.index_cache.max_bytes` and a snapshot on disk until the
  graph was evicted or the node restarted; a drop landing just before the
  build's count installed a "too small" verdict instead, which costs no bytes
  and is never evicted at all. The window the entries above close was, for
  the length of one build, still open. Every forget now moves a
  per-collection marker that a build reads before it looks at the snapshot or
  the store, and a build that finds the marker moved when it comes to install
  discards its graph, removes the snapshot it wrote, and answers its own
  search with the exact scan — the answer a search on a dropped or replaced
  collection resolves to anyway. A forget that finds the name created again
  fences the same way, so a build started for the previous collection cannot
  install under the new one; a build of the live collection that happens to
  be under way when a late drop entry is read is discarded too and paid again
  at the next search — a rebuild, never a wrong answer.

- **Storing a document's vectors is one commit, and a failure part way
  through no longer leaves a torn chunk set.** Replacing a document's chunks
  — from the embedding worker or a client's `PUT .../vectors` — wrote each
  chunk as a commit of its own and then removed each stale tail chunk the
  same way, so a document of N chunks cost N commits and, under the
  `durable` class, N fsyncs, measured at about 4.6 ms apiece. A write that
  failed between two of them left the document holding some chunks from the
  new version in front of some from the old, a state nothing could tell
  from a finished one, while [Vectors](docs/vectors.md) described the
  replacement as all-or-nothing. Both writes now go through the scoped
  write ADR-149 introduces: every chunk and every tail delete of one
  document is a single transaction, committed once however many chunks the
  document holds, and a failure inside it leaves the previous chunk set
  exactly as it was. Deleting a document's vectors is one commit the same
  way. The vector generation that tells the index cache a graph is stale now
  moves only after that commit, never before it, and only when a chunk was
  written or removed — an empty chunk set stored over a document that had
  none used to move it for nothing. The change feed carries the same entries
  as before, published together after the one commit rather than one at a
  time.

- **The embedding worker commits once per provider batch, not once per
  document and once more for its position.** The worker stored the documents
  one provider call answers for — about thirty — one write each, every one a
  commit of its own and, under `durable`, an fsync of its own, and then
  recorded its oplog position in one more. A batch is now written through
  one scoped transaction with the position folded into the same commit: one
  commit and one fsync where there were about thirty-three, so `kimmy_commits`
  on a member that embeds falls to roughly one per provider call. A held
  position with nothing to embed is still one commit of its own, on the same
  one-second deadline as before, and `kimmy_embed_documents_total` and
  `kimmy_embed_chunks_total` count what they counted. A batch is also stored
  whole or not at all: a storage failure part way through now leaves every
  document of the batch unstored and the position where it was, rather than
  some documents written, the rest stale, and the position past them.

### Added

- **Resident memory is a `/metrics` series.** `kimmy_process_resident_bytes`
  is the process's resident set as the kernel reports it (`VmRSS` from
  `/proc/self/status`) — the figure a container memory limit is enforced
  against, which neither `kimmy_storage_bytes` (a file size) nor
  `kimmy_vector_index_cache_bytes` (an estimate of one part of the heap) is —
  and `kimmy_process_resident_peak_bytes` is its high-water mark (`VmHWM`),
  readable after the current figure has come back down. Both sit after
  `kimmy_vector_index_cache_bytes` in the engine's block, are read fresh on
  every scrape and every export, and reach the OTLP bridge as
  `kimmy.process.resident.bytes` and `kimmy.process.resident.peak.bytes`
  with unit `By`. Alert on the first against the container's limit;
  [Operations](docs/operations.md#capacity) says how, and what the gauge is
  for testing about memory that does not come back down
  ([ADR-147](docs/decisions.md)). 0 on a platform without `/proc`.

  Why: a 0.24.0 member with a 2 GiB limit climbed to exactly that limit
  under a read burst, held there for eleven minutes after the load on it
  ended, and was then gone with exit status 0 and no line in its log. Only a
  sampler shelling out to `docker stats` saw the climb; a deployment reading
  `/metrics` or a collector could not have.

- **Every exit is named in the log, and the one that cannot be is named by
  the next start.** A run that ends on an error now logs `exiting on an
  error` with the error's text, beside the two lines a signal has always
  logged. Both paths leave `kimmy.last-exit` in the data directory, saying
  how the run ended, its pid, build and time; the next start reads and
  removes it and logs `previous run ended cleanly`. A start that finds the
  database and no marker logs `previous run did not shut down cleanly` at
  `WARN` with the database file's age as `last_database_write_secs_ago` —
  the line an operator gets when a process was ended from outside by
  something that did not let it log, which is what happened to the member
  above. `kimmyd restore` writes a marker too, so a restored directory does
  not start with that warning ([ADR-147](docs/decisions.md)).

  **Expect that warning once per node on the first start after upgrading**:
  no earlier release wrote the marker, so a data directory from 0.24.0 or
  before has a database and no marker, and the node cannot tell an upgrade
  from a crash. The start after that is the first one the line means what
  it says.

- **`kimmy_sync_entries_skipped_total{reason}`** — replicated entries a round
  left rather than took. `unknown_collection` is a batch stopped at an entry
  for a collection this member has no record of — it neither holds the
  collection nor a tombstone for it: the window is re-served every round
  until a snapshot brings the collection, and the warning names it. A
  collection this member dropped is history and stops nothing.
  `beyond_advertised` is an entry above the vector the peer advertised before
  serving the window, taken by the next round. **Alert on
  `unknown_collection` sustained**: it is a member that cannot place what its
  peers are sending it.

- **`kimmy_sync_repair_rounds_total`** — rounds spent repairing against a
  peer: re-serving its oplog from a divergent collection's creation, or
  pulling its snapshot. Rising is a repair under way; it stops when the
  repair reaches the peer's tail. Both series are on the OTLP bridge
  ([ADR-148](docs/decisions.md)).

### Documentation

- **The sort language now says which element a key through an array reads.** A
  sort path that reaches several values — a field holding an array, or a path
  crossing one — is reduced to the *smallest* of them, and `-1` reverses the
  comparison rather than choosing the other end, so a descending sort orders
  by each document's smallest element too: descending, `[2, 3]` comes before
  `[1, 100]`, and the `100` never enters it. One comparison serves `find`,
  `find_and_modify` and the pipeline's `$sort`, so the three order a
  collection identically.
  [Query language](docs/query-language.md#sort-and-projection) states it,
  along with the empty-array case and the contrast against `$lookup`'s join
  key, which reads the first element of a path that crosses an array rather
  than the least; [Aggregation](docs/aggregation.md)'s `$sort` row now points
  there instead of restating the rule. Nothing changes about what the server
  does — this was true in every release and written down in none.

- **Three settings an operator can be told about by name are now in the
  settings table.** `server.advertise`, `storage.multi_chunk_docs` and
  `auth.oidc.allow_federated_admin` are each named in a startup refusal or a
  log line, and each was documented only in prose on another page.
  [Operations](docs/operations.md#settings) now carries them with their
  defaults beside every other key.

- **The write-path benchmark rows for storing vectors were re-measured after
  the one-commit change, and the operations guide now says `coalesced` slows
  a lone writer, the embedding worker included.** The two `put_vectors` rows
  in [Benchmarks](docs/benchmarks.md#the-write-path) were taken when each of
  a document's chunks was a commit of its own, so the four-chunk row cost
  3.3× the one-chunk row and the multiplier sat in the table without a name.
  Measured again on one machine, before and after, one chunk costs what it
  did, four chunks cost 3.2× less and thirty-two 14× less: the marginal chunk
  fell from a commit to about a fifth of a millisecond, and the page now says
  what ceiling the embedding worker runs against instead — one commit per
  provider batch, its position folded in. Beside `storage.durability`,
  [Operations](docs/operations.md#settings) now says that `coalesced` shares
  an fsync among *concurrent* committers only: a lone writer waits out a
  window for company that never comes, and the embedding worker is one, so
  on a member that embeds the class makes that member's own embedding slower
  per batch, not faster. [Vectors](docs/vectors.md#batching) says a batch's
  documents and the worker's position are one commit, where it used to say
  the storage write was still per document.

## 0.24.0 - 2026-09-06

### Changed

- **A document an index cannot key is stored, not refused — and it no longer
  wedges replication.** Three shapes used to draw a `400` from an index: a
  document holding arrays at two of a compound index's paths, one that would
  produce more than 1,000 index entries, and one with a `Decimal128` at an
  indexed path. Each is now stored and filed under the index's *unkeyed* run,
  which every scan of that index reads beside its ranges and rechecks like any
  other candidate — so every query still finds the document, and none finds
  it wrongly. A unique index still refuses such a document on a local write,
  because a unique index must be able to key every document it covers and the
  client is there to be told; replicated, it is filed unkeyed and warned about
  ([ADR-139](docs/decisions.md)).

  Why: a three-member cluster wedged twice in one hour. A member built a
  compound index while no document violated it; a peer that had not yet
  received the definition legally accepted a two-array document; when that
  document reached the holder, the holder could neither apply the entry nor
  skip it, every sync round failed as a `malformed frame`, and the member's
  whole inbound stream stopped for the life of the process with four
  collections diverging behind it. ADR-123 had covered the mirror order — a
  definition arriving after the document — by skipping the definition; this
  order had no answer. Refusing a document a peer has already accepted is a
  choice a leaderless store cannot make and converge, so the rule is gone
  rather than patched: an index is an access path, not a schema.

  What a client sees: the write returns `200`; `explain` reports
  `unkeyedCandidates` beside `indexEntriesRead`; the index listing, `describe`
  and `createIndex` report `unkeyed`, the number of documents the index could
  not key; `/metrics` gains `kimmy_index_unkeyed_total`, on the OTLP bridge as
  `kimmy.index.unkeyed`; and each such write is logged at warning naming the
  database, collection, index and document id. A replicated `createIndex`
  whose backfill meets such a document now **builds**, so
  `kimmy_sync_ddl_refused_total` no longer rises for it — that counter is
  left for a definition a member genuinely cannot apply.

  Breaking, deliberately: a client relying on the `400` to police document
  shape must check `unkeyed` on the index instead, or split the compound
  index into single-field ones, which key every shape.

- **The divergence check compares document counts against a member that
  has stopped, and says how old its reading is.** On a three-member cluster
  where one member's inbound replication failed every round for half an
  hour, four collections diverged in document count under the same names
  everywhere, and `kimmy_sync_divergent_collections` read 0 on every member
  throughout: the healthy members deferred the count half against the
  wedged one on every one of some 250 checks, because a member that is
  behind was read as a member catching up, and the wedged member's own
  gauge served a value nothing had re-examined with no series to say so.
  The count half now defers only while the peer is behind *and moving*, and
  compares once the peer's position has stood still for three consecutive
  checked contacts; each checked contact is counted in a new
  `kimmy_sync_divergence_count_probes_total{outcome="compared"|"deferred"}`
  (bridged as `kimmy.sync.divergence_count_probes.compared` and
  `.deferred`), so a count half that has never compared against anyone is a
  flat `compared`, not a clean 0. A new gauge,
  `kimmy_sync_divergence_check_age_seconds` (bridged as
  `kimmy.sync.divergence_check_age`), is seconds since the last contact
  whose round ran the check — **alert on it above a few multiples of
  `cluster.sync_interval_secs`**, and treat the gauge as unknown while it
  is. A failed round is now counted as `skipped` on
  `kimmy_sync_divergence_checks_total` as well as in
  `kimmy_sync_failures_total`, so `ran` + `skipped` is every round
  attempted; a PromQL rule that added the failures counter to the pair now
  counts a failed round twice and should drop the addition. The check still
  does not run on a failed round ([ADR-145](docs/decisions.md), amending
  ADR-133 and ADR-135).
- **The divergence check's "behind" is judged on what a member has
  processed, not on what it can serve.** The gate that decides whether a
  peer's document count can be trusted compared the peer's servable version
  vector against this node's witnessed one, and the two measure different
  things: a member that processed this node's latest entry from some origin
  without appending it — the loser of a concurrent write to one document, a
  schema change it refused, a unique-violation stamp it witnessed through a
  batch's coverage without ever being sent the entry — can
  serve less of that origin than it has processed, for ever, and read as
  behind for ever. Before ADR-145 its count was silently never compared;
  after, it was compared only once it had "stood still" for three checked
  contacts, so a converged idle cluster showed a trickle of
  `kimmy_sync_divergence_count_probes_total{outcome="deferred"}` from a
  member with nothing to catch up on. A sync round now asks a peer for both
  vectors on the frame it already spends (`AskVersions` gains a `witnessed`
  flag, answered by a new `Vectors` message) and gates on what the peer has
  processed; on a converged idle cluster `deferred` no longer rises. Rolls
  without failing a round: a member on the previous release ignores the
  flag and answers as before, and the requester gates that contact on the
  old rule and logs `peer answered without saying what it has processed`
  once per such member, so a `deferred` trickle during a roll is expected
  and ends with it ([ADR-146](docs/decisions.md), amending ADR-133 and
  ADR-145).
- **A drop is recorded and replicated wherever it lands.** `DELETE
  …/indexes/{name}` on a member that holds the collection but not the index
  used to answer `200 {"dropped": false}`, mint nothing, and do nothing
  cluster-wide — so a drop issued through a front that spreads requests
  across members was a silent no-op whenever it landed on a non-holder, and
  the index at the centre of the wedge above survived its own cleanup for 42
  minutes. It now mints the drop entry and records the tombstone under a
  fresh stamp, so the drop removes the index on every member that holds it,
  and on a clustered node is pushed to every live member before the
  response. `dropped` keeps its meaning — whether *this* member held the
  index — and `confirmation` says who applied it. A name a create would
  refuse is now `400` on drop too. A replicated drop declined as older than
  the index standing under its name is logged at info with both stamps and
  counted in a new `kimmy_sync_ddl_declined_total` (bridged as
  `kimmy.sync.ddl_declined`): a re-served window does that once and rarely,
  and a count that keeps rising is a member whose clock ran ahead when it
  created the index ([ADR-141](docs/decisions.md), amending ADR-123).

- **A failed request's log line names its event in an `event` field, and
  `message` is written once.** Every `request failed` line used to carry two
  `message` keys — the event name, which a `tracing` format string is, and
  the client-facing text — and the JSON format wrote both, so a parser kept
  one of the two without saying which. The line now carries `event:
  request failed`, `code` and `message`, each once, at all three levels; a
  pipeline that matched on `message == "request failed"` should match on
  `event` instead, and one that read `message` for the text now gets it every
  time. The pretty format renders `event="request failed"` where the bare text
  was. Nothing in the response moves ([ADR-144](docs/decisions.md), amending
  ADR-136).

### Fixed

- **Nine `/metrics` series were never on the OTLP bridge.** `kimmy_databases`,
  `kimmy_collections`, `kimmy_unique_violations`, `kimmy_commits`,
  `kimmy_fsyncs`, `kimmy_commits_grouped_total`, `kimmy_storage_bytes`,
  `kimmy_vector_index_cache_bytes` and `kimmy_up` were rendered by the
  `/metrics` route ahead of the process counters, which is the block the
  bridge reads and the block its guard test checks, so a collector-only
  deployment could not see unique violations, commit and fsync cost, storage
  size or the vector cache. They now render with everything else and reach
  the bridge as `kimmy.databases`, `kimmy.collections`,
  `kimmy.unique_violations`, `kimmy.commits`, `kimmy.fsyncs`,
  `kimmy.commits.grouped`, `kimmy.storage.bytes`,
  `kimmy.vector.index_cache.bytes` and `kimmy.up`; the guard now covers the
  whole page ([ADR-142](docs/decisions.md)). `/metrics` itself is byte for
  byte what it was.
- **`docs/openapi.yaml` no longer declares a `maximum` on a field the server
  clamps.** `FindRequest.limit`, the `limit` query parameter of
  `GET …/docs`, and `SearchRequest.k` carried `maximum: 10000`,
  `maximum: 10000` and `minimum: 1, maximum: 1000` while the server clamps
  each rather than refusing it, so a generated client that honoured the
  keyword rejected `limit: 50000` or `k: 2000` before sending — requests the
  server answers `200` with a shorter page — and `count`, which takes the
  `find` body, inherited a cap on a route that has none. The keywords are
  gone; each description states the clamp, `count` says which body fields it
  reads, and `docs/vectors.md` now documents that `k` defaults to 10 and is
  clamped to 1–1,000 (`0` is read as `1`, not as an empty page), which
  nothing had written down. The MCP tool schemas say the same: `find`'s
  `limit` and `vector_search`/`hybrid_search`'s `k` each describe their
  clamp, since both route through the code that applies it. A spec lint
  keeps a clamp out of the validation keywords from here on. Nothing the
  server does has changed.

### Added

- **`createIndex` and `dropIndex` confirm the change on every live member
  before answering.** On a clustered node each member SWIM considers alive is
  handed the window it lacks from this node, ending in the change — a push is
  a pull the sender starts, so a member's history never gains a hole
  ([ADR-143](docs/decisions.md)) — and the response carries `confirmation`:
  who applied or already held it, who refused it (counted on their own
  `kimmy_sync_ddl_refused_total`), and who did not answer within
  `cluster.ddl_confirm_timeout_secs` (default 10 s; `0` turns it off) or was
  too far behind for one push to reach. A
  client that creates an index on one member and writes through a front a
  moment later now finds the index enforced wherever the write lands, which
  closes the ~3.5 s window the wedge above came through. A node without a
  member set answers as before ([ADR-140](docs/decisions.md)).
## 0.23.2 - 2026-09-05

### Fixed

- **The cross-member divergence check no longer goes blind on a database that
  holds nothing but a vector shadow collection.** `kimmy_sync_divergent_collections`
  is built from a set that excludes vector shadow collections, because only the
  owning member builds one and comparing them would report that by-design
  difference on every round. The exclusion was wider than its reason: it also
  hid a shadow whose *base collection is gone*, which is not lifecycle lag but
  residue. A database left holding only such a shadow was therefore filtered out
  on both sides of the comparison, and a database present on one member and
  absent on another became structurally invisible — the gauge read `0` while
  `kimmy_sync_divergence_checks_total{ran}` climbed and `{skipped}` stayed at
  `0`, which is exactly the reading that is supposed to mean *checked and
  agreed*. An orphaned shadow is now compared; a shadow beside its collection
  still is not ([ADR-138](docs/decisions.md)).

  Measured on a live three-member cluster before the fix: a real one-sided
  difference held for over 60 seconds with every instrument reporting clean.
  Nothing about the wire, the gauge's name, or its meaning changes — what
  changes is what it can see.

### Documentation

- **The database drop contract now says to issue the drop once.**
  `DELETE /v1/db/{db}` drops the collections the receiving member holds at the
  moment it is applied, and that is what replicates. Sending it to every member
  at once is a race each peer can lose: a collection still in flight arrives
  after that peer's local drop and recreates the database there. A
  vector-enabled collection makes it easy to hit, since the owner's shadow
  replicates a moment behind. Drop on one member and confirm on the others.

## 0.23.1 - 2026-09-05

### Fixed

- **The runtime-stall gauge no longer latches on a deployment that reads its
  telemetry through a collector.** `kimmy_runtime_stall_seconds` is a
  high-water mark that clears when it is read, and 0.23.0 bridged it to OTLP
  through the shared, non-clearing read every other instrument uses. So the
  `/metrics` scrape cleared the mark and the collector's export did not: with
  both surfaces in use the two reported windows neither had measured, and with
  **only** a collector — the deployment the bridge exists for — nothing ever
  cleared it, so `kimmy.runtime.stall` climbed to the worst stall since process
  start and stayed there. A gauge that only ever rises cannot answer the
  question it exists for, which is whether a worker thread is blocked *now*.
  Each surface now keeps its own mark, fed by the same observation, so each
  reports the worst stall since **that surface** last reported one: the same
  meaning on both, over different intervals, and neither able to consume the
  other's. The two will not print the same number at the same moment, and
  [Operations](docs/operations.md) now says so — it is the one bridged series
  that is measured per surface rather than shared, because it is the only one
  whose read clears what it read. Nothing on `/metrics` changes: same name,
  same value, same reset-on-scrape behaviour it has always had.

## 0.23.0 - 2026-09-05

### Added

- **The divergence gauge now says whether it looked.**
  `kimmy_sync_divergence_checks_total{outcome}` counts contacts with a peer in
  which the cross-member divergence check `ran`, and contacts whose round
  completed without it because the pull was truncated by the batch cap
  (`skipped`). `kimmy_sync_divergent_collections` reading 0 meant two
  different things — *checked, and the peers agree* and *not checked at
  all* — with nothing scrapable to tell them apart, which left the documented
  alert ("gauge above 0") silent in exactly the state it most needs to speak:
  a sustained backlog silences the check, and a sustained backlog is when a
  divergence is most plausibly being created. Alert on the pair — the gauge
  above 0, **and** `ran` failing to increase on a node that has peers — and
  read `ran` flat with `skipped` rising as *unknown* rather than clean. A
  round that failed outright is counted in `kimmy_sync_failures_total` and in
  neither outcome, so `ran` + `skipped` + failures is every contact a node
  made. No peer name, node id or collection name appears in either series;
  the check itself, what it compares and when it runs are all unchanged.
  ADR-135.

- **A round whose pull the batch cap truncated still skips the check, and now
  there is a worked example of why.** A three-member cluster produced a
  245-second window that looked like a divergence the gauge had slept
  through, and was a backlog: five collections created on one member reached
  the other two at 219 s and 227 s and the gap closed by itself. On a
  truncated pull "the peer holds a collection I lack" and "I have not applied
  the entry that creates it here yet" are the same observation, and the
  two-contact confirmation — about 10 s at the default sync interval — is far
  too fast to filter a window of that length. Checking anyway would have held
  the gauge above zero for minutes on a healthy cluster, on every wave of a
  bulk load. The behaviour does not change; the operations guide now states
  the correlation plainly instead of leaving it to be inferred. ADR-135.

### Changed

- **Breaking, `kimmy-api` API: a failure's log level is a three-variant type,
  so one below `INFO` cannot be asked for.** `ErrorCode::log_level` and
  `ApiError::log_level` return `Option<LogLevel>` — `Error`, `Warn`, `Info` —
  `ApiError::at_level` takes one, and the public `ApiError::level_override`
  field holds one, all in place of `tracing::Level`. `at_level` is public and
  used to accept all five `tracing` levels while the log site handled three,
  with a fallback that fired `debug_assert!(false)` and then logged at `INFO`:
  `at_level(Level::DEBUG)` panicked a debug build and, in a release build,
  logged the failure one level louder than asked. Narrowing the type removes
  the state rather than the guard, and the match at the log site is now
  exhaustive over three arms with no fallback to get wrong. It also makes the
  rule the two entries below rest on structural rather than asserted — `None`
  is "not a log event" and there is no variant quieter than `INFO`, so neither
  can be written down incorrectly. **Nothing on the wire moves**: the status,
  the `error` code, the `retry` class, the message, the `request failed` event
  message and every code's published level are all unchanged, and the two
  overrides in the server pass `LogLevel::Error` where they passed
  `Level::ERROR`. ADR-137, amending ADR-136.

- **A node that cannot serve local embeddings now says so at `ERROR`.** It
  answers `501 not_implemented`, the same code as a caller asking for a
  reserved capability, so it used to be indistinguishable in the log from
  somebody else's request. It is not the same thing: it is a member
  provisioned unlike the rest of its cluster, every search of that collection
  landing on it fails, and behind a load balancer the other members hide it.
  It is raised above its code's level at the point it is constructed, so the
  lowering below does not bury it. ADR-136.

- **A failed request is logged at a level chosen by who has to fix it, not by
  its HTTP status.** Every 5xx used to write an `ERROR` line, which meant one
  client sending requests the reference documents as refusals could make a
  member look unhealthy: a test round produced seven `ERROR` lines, and all
  seven were refusals a passing test case had asked for on purpose. The level
  is now a property of the error code — `internal`, `misconfigured` and
  `snapshot` at `ERROR`; `timeout` and `provider_error` at `WARN`;
  `not_implemented` at `INFO` — and 4xx codes are still not logged at all, now
  because the code says so rather than because the status did. **Alert on
  `ERROR` is meant to be correct as written on an untuned node**; the levels
  and the list of codes that never log are published in
  [Operations](docs/operations.md#logs) and held to the server's own enum by a
  test, so they cannot drift apart. This changes what a node logs and nothing
  else — the status, the `error` code, the `retry` class and the message in
  the response body are all untouched, as is the `request failed` event
  message, which is the same on every level so one query still finds them all.
  ADR-136.

- **Breaking, `kimmy-client` API: `ErrorCode` knows `timeout`, and `Unknown`
  keeps the code it was handed.** The client's mirror of the server's code set
  had no `Timeout` variant and no `"timeout"` arm, so a `503 timeout` — a code
  the server has sent since ADR-099 — parsed as the unknown fallback, and a
  caller branching on the code could not see a timeout at all. Separately,
  `Unknown` was documented as keeping the string the server sent and did not:
  every unrecognized code became the literal `unknown`, so the one situation
  the variant exists for — a code newer than the client — was the one it made
  undiagnosable. `ErrorCode::Unknown` now carries an `Arc<str>` rather than a
  `&'static str`, which means `ErrorCode` is no longer `Copy` — though cloning
  one is an `Arc` bump and not a copy of the string. `Error::code()` still
  returns an owned `Option<ErrorCode>`; `Error::code_str()` is new and borrows
  the code's wire string rather than allocating one, as does the now-public
  `ErrorCode::as_str()`, which is what `Display` writes. Every comparison
  against a named variant is unchanged. The list of documented codes the client's
  round-trip test checks had also fallen two behind the server and now names
  all nineteen.

- **Documentation only: `docs/testing.md` states what is enforced instead of
  counting.** Its header claimed `1398 tests passing` over a per-crate column
  that summed to 874, against a workspace that measures 1947 — three numbers,
  no two agreeing, none checked by anything. Four measurements taken hours
  apart on one afternoon gave 1933, 1937, 1939 and 1947, each correct for the
  crate set it covered, which is the case against the count rather than against
  whoever last updated it. The header now names the four commands CI runs on
  every pull request instead of a result this file cannot assert, the table
  keeps what each crate is *for*, which does not go stale, and `kimmy-egress`
  and `kimmy-client` are in it for the first time.

### Fixed

- **Every `/metrics` series a collector can be given now reaches the OTLP
  bridge, and a test keeps it that way.** Twelve series were on `/metrics` and
  on no OTLP instrument, so a deployment that reads telemetry through a
  collector could not see them at all: the whole embedding worker
  (`kimmy_embed_documents_total`, `_chunks_`, `_deferred_`,
  `_skipped_not_owned_`, `_failures_`, `_provider_requests_`,
  `_provider_tokens_`, and `_provider_errors_total{kind}`),
  `kimmy_runtime_stall_seconds`, and — the pair an operator is told to alert
  on — `kimmy_sync_divergent_collections` with
  `kimmy_sync_divergence_checks_total{outcome}`. That last one matters more
  than its size: ADR-135 exists because the gauge reading `0` means *checked
  and agreed* or *not checked at all*, and the counter is what separates them,
  so a collector receiving the gauge without the counter is in exactly the
  state the operations guide says not to reason from. Eleven of the twelve are
  bridged here. Labelled series become one instrument per label value
  (`kimmy.sync.divergence_checks.ran` and `.skipped`), which is how every
  labelled series on this bridge was already carried. Nothing on `/metrics`
  changes — same names, same values, same scrape.

  `kimmy_request_duration_seconds` is the twelfth and is deliberately not
  bridged yet: every instrument here is observable, read by a callback when a
  collector asks, and OpenTelemetry has no observable histogram. Bridging it
  means recording at each request rather than adding a callback, which is its
  own change with its own bucket design. It is named, with that reason, in a
  `NOT_BRIDGED` list that the new test reads — so the exception is written
  down rather than silent, which is how the other twelve went missing.

- **Documentation only, no behaviour change:** the hardcoded counts of error
  codes are corrected, and most of them are gone rather than corrected. The
  server's `ErrorCode` enum has grown twice since the counts were written —
  `stale` with conditional writes, `timeout` with the request deadline — and
  three places still stated the old size with nothing checking them: the module
  documentation in `crates/kimmy-api/src/error.rs` said a new failure "cannot
  invent an eighteenth code", the M10 task table in
  [Roadmap](docs/roadmap.md) said "the seventeen codes are closed by the
  compiler", and ADR-057 said `wait` was two codes and `no` was twelve, where
  they are now three and thirteen. **The number is removed wherever the
  sentence did not need one** — "the code set is closed by the compiler" is the
  same claim without a figure that can rot — and ADR-057, being a settled
  record, is amended in place rather than rewritten: the amendment marks the
  bullets as an M10 snapshot, names the two codes that joined since, and points
  at `ErrorCode::retry()` and the contract test that already holds every code's
  retry class to the enum, instead of writing a fresh count for someone to find
  wrong later. `crates/kimmy-api/src/error.rs` also now records beside the enum
  that adding a variant means editing `kimmy-client` too, which shares no code
  with the server by design and which no test points an author at. The
  mutation-pass finding in [Testing](docs/testing.md) keeps its "thirteen of
  the seventeen" — it is the dated record of a run against a set that really
  did hold seventeen codes, and updating the figure would falsify it.

- **Documentation only, no behaviour change:** ADR-123 now carries a forward
  marker to ADR-132. ADR-132 withdrew one of ADR-123's promises — that two
  members creating one index name with different definitions each keep their
  own and the refusal is counted in `kimmy_sync_ddl_refused_total` — for the
  case where both definitions carry a creation stamp, and said so in its own
  record; ADR-123 said nothing, so a reader landing there read a promise that
  is no longer true for that case with nothing pointing forward. The marker
  sits at the head of ADR-123 and beside the paragraph it amends, and states
  the scope in both places: the counter does not rise where **both**
  definitions carry a stamp, that case settles on the later stamp, and
  ADR-123 holds exactly as written where either carries none. The 0.22.0
  entry below, which stated the new rule without that scope, is corrected in
  place for the same reason: on an upgraded cluster every index that already
  exists is unstamped, so a reader of that entry alone had the wrong model for
  the whole population they were about to roll.

- **The aggregation reference now says what `$count` and `$group` do over an
  empty input stream.** Nothing changed in either stage; what was missing was
  any way to derive their answers from the documentation, and they differ.
  `$count` computes one number over the whole stream and that number is
  defined when the stream holds nothing, so it emits **one document holding
  `0`** however the stream came to be empty — a `$match` that selected
  nothing, an `$unwind` that dropped every row, a `$skip` past the end, an
  explicit `$limit: 0`, all answer `[{"n": 0}]`. `$group` over that same
  stream emits **nothing at all**, `{"_id": null}` included, because it
  produces one row per distinct key and an empty stream has no keys. The two
  are the same rule asked different questions, but the difference decides the
  read: a total taken from `$count` is always present, where a pipeline
  ending in `$group` can legitimately answer `[]` and `{"_id": null}` is not
  a promise of one row. `docs/aggregation.md` states both, in the stage table
  and beside the accumulators' own "nothing to work on" case, which is a
  different one — a group that exists and had nothing usable in it, not the
  absence of any group. Both behaviours are now held by tests.

- **`$count` is documented as blocking, which it always was.** The
  aggregation reference defined blocking as a stage that cannot emit until it
  has consumed everything, then listed only `$group` and `$sort` — `$count`
  meets the same definition and was named nowhere. The term was also used in
  the stage table and defined 550 lines below it with no link between the
  two; the `$sort`, `$group` and `$count` rows now link to the definition.
  Nothing about the memory ceiling changes: a pipeline's input is materialised
  before the first stage and every stage works on that whole set.

- **The error table in `docs/http-api.md` is now held to the server's code
  set.** A contract test already pinned `docs/openapi.yaml`'s `ErrorCode`
  schema to the enum, but nothing checked the prose table — the one a client
  author actually reads — so a new code could go missing from it silently. The
  new test asserts the table's codes are exactly the served set and that its
  `retry` column matches what the server sends for each one. The table was in
  fact complete and correct; what was missing was anything that would notice
  when it stopped being.

## 0.22.0 - 2026-09-04

### Added

- **A cross-member check makes a silent divergence alertable.** Every
  anti-entropy round whose pull reaches the peer's true tail — nothing left
  to pull, or this round's own batch was not truncated by the cap — now also
  asks that peer what it holds: every collection id, and one collection's
  live document count, chosen in turn so no round pays for more than one
  collection's scan. `kimmy_sync_divergent_collections`, a gauge, moves once
  a collection is found disagreeing twice running: two consecutive contacts
  with the same peer for a collection-existence finding, two consecutive
  probes of that specific collection against that peer for a document-count
  finding, since only one collection is probed per contact and those are not
  the same two contacts once more than one collection is in rotation — each
  half clears independently the moment its own next relevant check no longer
  sees it. It moves for the exact condition below: a member missing a
  collection or a run of documents a peer holds, with
  `kimmy_replication_lag_seconds` at 0 and `kimmy_sync_failures_total`,
  `kimmy_sync_peers_backing_off` and `kimmy_sync_ddl_refused_total` all
  unmoved, because nothing about it fails a round. Runs on the existing
  `cluster.sync_interval_secs` cadence, so it still runs during a modestly
  busy cluster and not only an idle one; a backlog that never drains under
  the batch cap on any round leaves the check unrun and the gauge holding
  its last value, which is *not checked*, not *not divergent*. A peer that
  has simply not pulled this node's own recent writes is not flagged on
  that lag alone. A peer that answers an empty batch while claiming its
  tail was not reached — which a correct peer cannot produce — fails the
  round rather than being read as a clean or skipped check. It reports; it
  does not repair — see the operations guide for what it cannot catch. No
  collection or database name appears in the metric. A second wire change
  this release, alongside `Entries`': old and new peers cannot exchange
  this check, and the cutover has its own rollout shape in the operations
  guide. ADR-133.

- **`$unwind`'s `includeArrayIndex` is implemented rather than dropped.** The
  named field carries the position of the array element that produced each
  row, and `null` on a row no fan-out produced. A name beginning with `$`, or
  equal to `path`, is refused: this language reads `"$name"` as the field
  `name`, so a field named that way could never be read back, and the second
  would overwrite the element just placed there. ADR-129.

### Changed

- **Breaking, stored format and cluster wire: an index carries the stamp of its
  creation.** `IndexMeta` gained `created`, recorded in the collection metadata
  and carried in the `CreateIndex` entry, and it is what the two fixes above
  compare against. There is no shim and no migration — pre-1.0 the format is
  changed outright. **An index that already exists on disk carries no stamp,
  and reads as older than every drop and every rival**: a replayed drop removes
  it and a rival definition is refused and counted, which is exactly the
  behaviour of 0.21.0. It also heals on its own where it can: a member holding
  the same definition *with* a stamp hands it over on the next round. Where no
  member has one — an index every member created before this release — drop and
  recreate it **on one member** and let that replicate, rather than recreating
  it on each, if you want its name settled rather than counted. Nothing is
  added to `/metrics`, to the index listing on `/v1`, or to
  `docs/openapi.yaml`. ADR-132.

- **Two members that create the same index definition independently now agree
  on when it was created**, not merely on what it is. The creation stamp is
  what decides whether a replayed drop applies, so one definition under two
  stamps answered one drop two ways — one member keeping the index, the other
  losing it, permanently, with `kimmy_sync_ddl_refused_total` still at 0 and
  the lag gauge at 0. The later of the two stamps now stands on both, and it
  only ever moves forward. ADR-132.

- **Breaking, cluster wire: a batch answer now carries where the window
  ended.** `Message::Entries` gained `scanned_to` (the last stamp the sender's
  scan examined, an entry it withheld included) and `exhausted` (whether it
  stopped there because the oplog ran out), and changed from a newtype variant
  to a struct variant to do it. There is no compatibility shim and no version
  negotiation — pre-1.0 the cluster wire is changed outright — so a node of
  this version and a 0.21.0 node **cannot replicate with each other in either
  direction**: the round fails as a malformed frame and `kimmy_sync_failures_total`
  rises on both. Roll every member. Nothing on disk changes, and no client-facing
  route, response or `/v1` promise is affected. ADR-127.

- **Breaking: an explicit `null` where a field is optional is refused rather
  than read as absent.** A key present with a `null` value reached the server
  as though it had never been sent, so `{"if_stamp": null}` on `update`,
  `delete` or `find_and_modify` performed the write **unconditionally** — the
  conditional-write guard of ADR-084 silently removed by the value it was
  given — and `{"filter": null, "multi": true}` on `delete` matched every
  document and **emptied the collection**, the one request whose typo is
  indistinguishable from its intent. Such a body now answers `422` naming the
  field, in the same envelope and the same wording a wrongly-typed value
  already produced: `if_stamp: invalid type: null, expected a non-null value`.
  **Omitting the key is unchanged** and still means absent; only an explicit
  `null` is refused. It applies to every optional field of every closed
  request shape — `find`, `update`, `delete`, `find_and_modify`, index
  creation, webhook registration, both searches and `POST .../vector` — and to
  the matching MCP tool arguments, since `delete`'s `filter` reaches the same
  code either way. The tools' advertised `inputSchema` no longer offers `null`
  as a valid value or as the default, which is what a model reads before it
  calls one. Nothing stored or replicated is affected: the refusal sits on a
  request-only mirror of the vector configuration, so records that have always
  serialized an unset field as a literal `null` still load and still
  replicate. **Breaking for a client that sends `null` for a field it means to
  omit**, and a `0.MINOR` bump for it. ADR-128.

- **A `byo` provider configuration carrying a field that does not exist is now
  refused instead of accepted.** `{"kind":"byo","nosuch":1}` on
  `POST .../vector` answered `200` and configured the collection, having read
  `nosuch` and dropped it; it now answers `422` naming the field, the way
  `open_ai`, `ollama`, `custom_http`, `cohere`, `gemini`, `local` and `profile`
  always have. `byo` is the default provider, so this was the kind most likely
  to be configured and the only one that was open — its variant carried no
  fields at all, and the attribute that refuses unknown fields does not reach a
  variant with no body. The same key under a `[vector.providers.<name>]`
  profile now stops the node at startup rather than being ignored, and
  `kimmyd check-config` reports it without starting anything. A `byo`
  configuration with no stray key is unaffected in either place. **Breaking for
  a client that sends a field beside `"kind":"byo"` the provider does not
  define, and for an operator whose configuration file carries one — that node
  refuses to start on a file it started on before** — and a `0.MINOR` bump for
  it. **The encoded form is unchanged** — `{"kind":"byo"}` in JSON, a one-key
  document in the BSON that goes on the replication wire and to disk,
  `kind = "byo"` in TOML, all identical before and after — so nothing stored
  needs rewriting, there is no shim, and members may be rolled in any order.
  The keys that used to be accepted were never stored, so no existing record
  carries one. ADR-134.

### Fixed

- **A member no longer silently and permanently loses committed documents to a
  peer.** A node catching up by more than one batch could discard the remainder
  of a sync window and mark it seen, so no later round ever re-served it. Two
  causes, both closed. First, the 1,024-entry batch cap was spent *before* the
  `UniqueViolation` entries a peer never ships were filtered out, so a window
  truncated at the cap could return 1,019 entries with the peer's tail nowhere
  near reached; the cap is now spent on the entries that actually ship, so a
  batch shorter than the limit means what the receiver believes it means.
  Second, the receiver read any short batch as "the peer's whole tail" and
  absorbed the peer's entire version vector — witnessing every entry behind the
  window without applying one of them; it now takes the peer's own report of
  where the window ended instead of deducing it from a count, so no future
  filter can reopen the same hole.

  **How it looked.** Nothing failed, so nothing said so:
  `kimmy_replication_lag_seconds` 0 on every member, `kimmy_sync_failures_total`
  0, `kimmy_sync_peers_backing_off` 0, `/v1/topology` all live, and not one log
  line on the members that lost the data. Observed on a three-member cluster
  running 0.21.0: two collections held documents on the member that created
  them and answered `404` on both peers forty-five minutes later, and a full
  `_id` comparison found a 2,018-document collection holding 1,518 on one
  member and 1,501 on another — a contiguous run of 500 ids missing from both,
  plus a further 17 missing from the second. One bulk insert, discarded from
  the window remainder by both pullers. The preconditions are ordinary: a
  member more than one batch behind, and one cross-member unique collision
  anywhere inside the window.

  **If you have been running a cluster under load with unique indexes, assume
  members may already disagree.** This release stops the divergence; it does not
  repair one that has already happened. Compare collection lists and document
  counts across members directly — lag 0 and quiet counters are precisely this
  defect's signature, not evidence of convergence. Expect the losses to
  **overlap** rather than be disjoint: one lost window on an origin is missing
  from every member that was behind it, so a member-by-member count is not the
  sum of what went missing, and repairing from a member that has one run does
  not tell you the others are whole. Compare `_id` sets, not counts alone.
  ADR-126, ADR-127.

- **`$unwind` over a path that crosses an array emitted duplicate,
  unchanged rows instead of unwinding anything — and, depending on what the
  crossed element happened to hold, could instead emit one unchanged row
  that looked correctly unwound but was not.** `$unwind: "$x.b"`, where `x`
  itself holds an array (at any length, including one), read a value at the
  path to decide what to do and then failed silently to write each element
  back through the array — there is no single place to put it, whatever is
  found there. Both symptoms are now the same `400`, naming `$unwind` and
  the path, decided by whether the path crosses an array **by a named
  field**, never by what is sitting at the far end of it: `items.sku` over
  an array of `{sku, qty}` now refuses too, where it previously passed the
  document through unchanged. A **numeric** segment after the array is
  still not refused and is unaffected — `$unwind: "$a.0.b"` addresses a
  position, not a name, and reads it the way every field-path option here
  reads a numeric segment: as an index and as a field literally named that
  number, not only the latter the way a computed expression would. **Whether
  a pipeline is even legal now depends on the documents it meets, not on the
  pipeline text** — the same pipeline can run correctly for months and then
  refuse the day an ordinary write adds one document shaped this way, and
  one such document fails the whole request. A path that never crosses an
  array by a named field is unaffected. The refusal names the concrete fix,
  not just the problem: `$unwind: "$items.sku"` is told to unwind `$items`
  first and read `sku` on each resulting row, rather than being handed
  internal path-traversal vocabulary. ADR-130.

- **A misspelled key inside a pipeline stage was ignored, so the stage ran
  with the option silently off.** `{"$unwind": {"path": "$a",
  "preserveNullAndEmptyArrays": true}}` written one character short kept every
  document with an empty array out of the result and answered `200`, and the
  same held for `$lookup`'s and `$replaceRoot`'s operands. A stage whose keys
  are **vocabulary** — `$unwind`'s document form, `$lookup` in both forms,
  `$replaceRoot` — now refuses a key it does not define, `400`, naming the key
  and listing the ones the stage takes; the same closure covers
  `$dateToString`'s `format` and `$switch`'s `branches`, where a typo had
  quietly returned the default format in every row. A closed key's **value**
  is checked too: `"preserveNullAndEmptyArrays": "true"` or `: 1` reverted the
  option to `false` and is now refused by name. **`$match`, `$project`,
  `$sort` and a `$group`'s output names are deliberately left open and always
  will be** — their keys are field names the caller chose, not words this
  database defines, so there is nothing to check a key against. **Breaking for
  a pipeline that carried an unknown key in one of those stages**, which until
  now ran with that key discarded. ADR-129.

- **`explain: true` on `update` and `delete` performed the write it was asked
  to describe.** Inspecting a `multi: true` delete before running it deleted
  every matching document — the request an operator makes precisely to avoid
  that. Both routes now run the same read-only scan `find` and `count` use:
  nothing is written, no write transaction opens, and the engine's commit
  counter does not move. The write-outcome fields keep their names and their
  meaning — `matched`, `modified`, `deleted` and `commits` report `0` and
  `stamp` is absent, identical to a write that matched nothing — while what
  the write *would* touch is reported as `explain.documentsMatched`, the field
  `find`'s own `explain` has always carried. **`explain` combined with
  `if_stamp` is now refused `400`**: a plan checks no version, so it cannot
  honestly answer whether a conditional write would land, and the document it
  reports as matched may be the one the real write refuses `409 stale`.
  ADR-131.

- **`find` with `limit: 0` returned one document instead of an empty page**,
  on the unsorted path and on a `sort: {"_id": 1}` request, whenever the
  first document the scan examined happened to match the filter — an empty
  filter over a non-empty collection always qualifies. `limit: 0` is a
  documented, legal request for an empty page (`FindRequest.limit` has
  `minimum: 0`), and the sorted paths already honoured it; the unsorted scan
  handed a match to the page before checking whether the page's bound had
  already been reached. The bound is now checked first.

- **A replayed index drop no longer deletes a newer index of the same name.**
  An index that is created, dropped and created again derives the same id each
  time, and anti-entropy re-serves overlapping windows as a matter of course —
  so the drop between the two creations arrived again after the recreation and,
  with nothing to compare it against, removed an index nobody had dropped. It
  now leaves its tombstone and leaves the index alone. Observed on a
  three-member cluster running 0.21.0: a collection listed **no indexes on any
  member**, though three stood on all three an hour earlier. This also closes
  the half that could not repair itself — where the newer creation is the
  member's own, no peer can re-serve it, so the index stayed dropped there for
  good. ADR-132.

- **Two members that create one index name with different definitions now
  converge instead of staying divergent.** ADR-123 left both standing, each
  member keeping its own and counting the refusal; the 0.21.0 round watched two
  collections sit that way, with `kimmy_sync_ddl_refused_total` at 9 / 15 / 9
  and no path back to one schema. **Where both definitions carry a creation
  stamp**, the later of the two now wins on every member, which is how two
  concurrent writes to one document already settle. The member whose
  definition loses logs a warning naming the index and what differed, and
  rebuilds the name under the winner. Where either definition carries none —
  and on an upgraded cluster **every index that already exists carries
  none** — the 0.21.0 behaviour stands unchanged; the creation-stamp entry
  under *Changed* above says what that means and how to settle such a name.

  `kimmy_sync_ddl_refused_total` therefore **no longer rises for that case** —
  the stamped one. It is unchanged for the case that still needs an operator —
  a definition a member's own documents cannot be built under — and, where the
  winning definition cannot be built on the receiving member, that member keeps
  the index it already had rather than ending with neither. Creating a
  conflicting index through the API is unaffected: a client is still refused
  `409`, naming what differs. ADR-132.

## 0.21.0 - 2026-09-03

A minor when it ships, not a patch. Nothing changes on the wire between
members and nothing on disk that a 0.20.0 node cannot read: the one new
table is additive, and a node of this version and a 0.20.0 node replicate to
each other. But one thing a 0.20.0 node accepted is refused now — a query
string on a route that reads none — and the pre-1.0 policy puts that behind
a `0.MINOR` bump. The other entries fix what a test round against a
three-member cluster running 0.20.0 found: a replayed index definition that
stopped replication for ever, and a per-document commit on every member
that the one-transaction sync batch of 0.20.0 had moved rather than removed.

### Added

- **Three new series make a wedged peer visible.** `kimmy_sync_failures_total`
  counts anti-entropy rounds that failed, any cause; `kimmy_sync_peers_backing_off`
  is how many peers this node is currently leaving alone after failures;
  `kimmy_sync_ddl_refused_total` counts replicated schema changes this node
  skipped. All three are pushed by the replication loop after every tick,
  reached peers or not, through a new `on_round` hook beside `on_lag`, and
  bridged to OpenTelemetry. `kimmy_sync_failures_total` rising while
  `kimmy_replication_lag_seconds` sits at 0 is exactly the shape of the wedge
  under *Fixed* below; the operations table says to alert on it, and its lag
  row now says that a failing round leaves the gauge where the last good
  round put it. `on_lag` is unchanged: an unreachable cluster still has
  unknown lag, not zero (ADR-122), which is why the counter and not the gauge
  carries this signal. ADR-123.

### Changed

- **A replica builds a replicated unique index over data that already
  violates it, and records the collision, rather than refusing the
  definition.** A unique index created on one member and replayed on another
  whose documents already share a key used to be refused by the backfill —
  and, because that refusal failed the sync round, refused for ever. The
  replica now builds the index in full, every entry added, so index-backed
  queries stay complete and every later local write through it is checked,
  and records one violation per shared key naming every holder, the way a
  merged write's collision is recorded: counted in `kimmy_unique_violations`,
  logged at warning, minted as a `UniqueViolation` entry for change streams
  and reported by the violations route. This is ADR-020's rule applied to a
  definition rather than a document; a *local* create over violating data is
  still refused, because the client is there to be told. ADR-123.

- **A query string on a route that takes no query parameters is refused.**
  Only the routes that declare a parameter — `GET .../docs`, `PUT` and
  `DELETE .../docs/{id}`, `GET .../describe`, `GET .../violations`,
  `DELETE .../vector`, `GET .../watch` — ever read their query string, so
  the closure of 0.20.0 reached them and nothing else: `POST
  .../update?if_stamp=<stale stamp>` answered `200` and rewrote the
  document, because `if_stamp` is a body field there and the parameter was
  never read, while the same misspelling on `GET .../docs` was the
  documented `400`. `?bogus=1` on `find`, `count` and `bulk`, and
  `?multi=true` on `update`, answered `200` having ignored it. Every REST
  route now refuses any query string it does not read, `400 bad_request` in
  the envelope naming the first parameter and saying the route takes none;
  the routes that read one still refuse an unknown parameter by name as
  before. A bare `?` with nothing after it is not refused. `/mcp` is a
  separate transport and is unaffected. **Breaking for a client that sends
  a query parameter to a route that takes none** — including a health probe
  with a cache-busting parameter — and a `0.MINOR` bump for it. The Rust,
  Python and Go clients, the CLI, the conformance scenarios, the examples
  and every documented request were audited and send none. Found by a test
  round against a three-member cluster running 0.20.0. ADR-124.

- **`describe?sample=0` is refused.** `GET .../describe` answered `sample=0`
  with `200 {"sampled": 1}`, clamping a value the specification declares
  `minimum: 1` where every other query parameter a route cannot honour is a
  `400`. It is now `400 bad_request` naming the parameter and the minimum. A
  value above 1,000 is still clamped to 1,000, which the reference and the
  specification now both say. Found by a test round against a three-member
  cluster running 0.20.0.

- **A projection operator is refused by name.** `{"items": {"$slice": 3}}`
  or `$elemMatch` in a projection was refused with `projection value for
  "items" must be 0 or 1`, which told a reader porting a query nothing about
  why. The message now names it — `projection operator $slice is not
  supported for "items"; a projection value must be 0 or 1` — and the
  reference states the rule the value is read by. Status and code are
  unchanged. Found by the same test round.

### Fixed

- **A replayed index definition the replica could not build wedged
  replication with that peer permanently, with the lag gauge at 0 and every
  member live.** Observed on a three-member cluster running 0.20.0. A
  compound index over two array fields was created on a collection with no
  document holding both — accepted — and dropped 28 seconds later; both
  replicated. A document with arrays at both paths was then inserted, which
  is legal once the index is gone. Every later anti-entropy round that
  re-served the window holding the `CreateIndex` entry (windows overlap by
  design) rebuilt the index, and the rebuild's backfill met that document and
  failed. The error was not the one the round knew how to skip, so the round
  failed, its witnessed vector was discarded, coverage never advanced, and
  the same window was re-requested for ever with backoff to 300 s; the
  `DropIndex` behind it in the window was never reached. A second instance
  minutes later replayed a unique index whose backfill collided on the
  puller's copy. Writes on one member never reached the others again. Two
  fixes. Dropping an index now leaves a tombstone (`indexes_dropped`, under
  the drop's originating stamp, kept for `tombstone_retention_secs`, carried
  by backups, recorded even on a member that never held the index), and a
  creation older than the tombstone is history and is not applied — the rule
  collections have had since ADR-034. And a replicated schema change this
  node's own data refuses — an index its documents cannot be built under, an
  index name already taken here by a different definition, an enforcement
  mode this build does not implement — is skipped, logged once at warning
  with the database, collection, index and reason, counted in
  `SyncOutcome::ddl_refused`, and witnessed so it is not re-served; the
  entries behind it arrive. The snapshot route — served to exactly the peer
  most likely to hold such documents — classifies a definition the same way,
  so a refused index on a snapshot page is skipped and counted while the
  documents restore. Any other error still fails the round. Defended
  by `a_replayed_index_that_cannot_be_built_does_not_stop_the_entries_behind_it`,
  `a_dropped_index_never_comes_back_through_a_replayed_create`,
  `a_peer_that_receives_the_drop_before_the_create_never_builds_the_index`
  and their counterparts over real sockets. ADR-123.

- **The embedding worker checkpoints its oplog position by deadline, not
  once per entry.** The worker runs on every member and consumes the member's
  own arrival index, so it sees every write — its own and every replicated
  one — and it recorded its position after each entry it had nothing to do
  with, in a write transaction of its own, which under the default `durable`
  class is an fsync of its own. Measured on a three-member cluster running
  0.20.0: a 1,000-document bulk insert into a collection with no vector
  configuration converged on every member in 3–5 s, and then all three
  members, the writer included, kept committing at a steady ~18/s for about
  75 s until each had added roughly 1.2–1.3 commits per document — about
  1,320 commits per member for 1,000 documents; small bulks cost a replica
  exactly n + 1. The one-transaction sync batch of 0.20.0 had moved the
  per-document commit one step downstream rather than removing it, and the
  replication lag gauge read hundreds of seconds on the replicas while the
  trickle ran. The position is now held with the batches and written by the
  same flush — with a batch, at stream end, before a reconfiguration's
  backfill, or after at most one second when there is nothing to embed — so
  a replicated batch published in one burst, or a local bulk with no
  vectors, costs one position write however many entries it holds, a lone
  entry's position lands within about a second, and a steady trickle costs
  at most one checkpoint a second. The single-node form of the same cost was
  the measured two-commits-per-insert write gap in
  [Benchmarks](docs/benchmarks.md); it is closed by the same change. What an
  operator sees: `kimmy_commits` and `kimmy_fsyncs` no longer rise one for
  one with documents on any member, and a restart re-processes up to a
  second of the stream, which is safe because embedding is idempotent and
  every other outcome is re-derived from what is stored. ADR-125.

- **`DELETE .../docs/{id}` reports the tombstone's stamp.** Every write
  reports the version it produced, and `POST .../delete` of one document
  did, but the by-id delete answered `{"deleted": 1}` alone — the one write
  a client could not follow with the version it had just made. It now
  carries `stamp` exactly when `deleted` is 1; a missing document is still
  `{"deleted": 0}`. Additive, so a client that ignores the field sees no
  change. Found by a test round against a three-member cluster running
  0.20.0.


## 0.20.0 - 2026-09-02

A minor when it ships, not a patch. Nothing changes on the wire between
members, a 0.19.1 node reads everything this one writes, and the two replicate
to each other. But three things a 0.19.1 node accepted are refused or answered
differently now, and the pre-1.0 policy puts them behind a `0.MINOR` bump: a
request body or query string carrying a field the route does not define is
refused; a `$$variable` string in a `$lookup` sub-pipeline `$match` is refused
instead of matching nothing; and JSON object key order now survives the
boundary, so update operators apply in the order written and a document keeps
the field order it was stored with. The replication lag gauge also changes
what it measures, which matters to anyone alerting on it. All five entries
below come from one test round against a three-member cluster running 0.19.1.

### Changed

- **A request body with a field the route does not define is refused.**
  `find`, `count`, `aggregate`, `update`, `delete`, `find_and_modify`, index
  creation, collection creation, login, the user, role and webhook routes,
  vector upload and both searches all accepted `{"limitt": 5}`, or
  `{"explain": true}` on `aggregate`, and answered `200` having ignored it;
  only the vector configuration route refused. Every request shape is closed
  now: a field the reference does not list, at the top level or inside a
  nested shape such as an index's `fields` entry or a search's `weights`, is
  `422 bad_request`, and the message names the field and lists the ones the
  route takes. A grant inside a user or role body is closed too, and that is
  the case that mattered most: `collection` defaults to `*`, so a misspelt
  `colection` was not dropped but widened the grant to every collection in
  the database. Query strings are held to the same rule at `400`: `?limt=5`
  on `GET .../docs` is refused by name, and `?limit=abc` — which was bare
  text with no `error` code — is now in the envelope. Document bodies —
  insert, replace, bulk — are content and take any field, as before.
  **Breaking for a client that sends a field or query parameter the route
  does not define.** The Rust, Python and Go clients, the CLI, the
  conformance scenarios and every documented example were audited and send
  none. The MCP tools refuse an unknown argument the same way, as the tool's
  error result naming it, and their schemas declare
  `additionalProperties: false`. No switch to accept and ignore is offered: a
  field the server does not read is a request it cannot honour, and answering
  as if it had is the bug this removes. ADR-121.

- **`kimmy_replication_lag_seconds` now measures how far behind in time a
  node is, not the width of the history it lacks.** It used to be the span of
  origin timestamps between the newest entry this node had applied and the
  newest a peer held, worst origin. A bulk insert mints all its stamps within
  a fraction of a second, so a replica minutes into draining one read 0 the
  whole way through; on a three-member cluster the gauge stayed at 0 across a
  78-second, 1,000-document backlog and a 492-second, 4,000-document one, and
  only climbed once writes were spread over many minutes. It is now the
  seconds since the newest entry this node has applied from any origin a peer
  holds newer entries of, worst peer in the last round: about 30 seconds
  into a backlog, climbing until it drains, 0 when caught up. What an
  operator watching it will see differently: a backlog now shows up and grows
  while it lasts, so an alert threshold is reached by any backlog that
  outlives it rather than only by long-spread writes; the caught-up reading,
  the hold-last-value-through-an-outage behaviour and the stale-rejoiner
  verdict are unchanged. One reading is worth knowing: an origin quiet for
  hours that then writes once shows the length of that silence for a single
  round on each peer, until the entry is pulled — the old measure spiked the
  same way. The `# HELP` text on `/metrics` and the OpenTelemetry description
  say the new thing. ADR-122.

### Fixed

- **A replica applies a sync batch in one transaction, not one per
  document.** A client bulk insert was one commit on the node that accepted
  it and about one commit *per document* on every node that replicated it,
  plus two for the batch's bookkeeping, each an fsync under the default
  `durable` class. Measured on a three-member cluster running 0.19.1:
  `kimmy_commits` and `kimmy_fsyncs` rose one for one per replicated
  document, a 1,024-entry sync batch took about 145 s to apply, and
  replication ran at 8–13 documents a second — 1,000 documents converged in
  78.7 s, 4,000 in 492.6 s. Consecutive document entries in a batch now share
  one write transaction, and the witnessed and coverage vectors are raised in
  that same transaction, so a batch with no schema change in it is one commit
  and one fsync; a schema change in a batch ends the transaction and the
  documents after it start another. Replication throughput is no longer bound
  by one fsync per document. What an operator sees: on replicas
  `kimmy_commits` and `kimmy_fsyncs` rise per batch rather than per document,
  so a dashboard that read a replica's commit rate as its document rate reads a
  much smaller number — `kimmy_replication_lag_seconds` and the `cluster.sync`
  span's `applied` are the document-rate figures — and a local write on a
  replica may wait for a whole batch to apply rather than for one entry. A
  replica also publishes a batch to change streams in one burst after it
  commits, and the live feed's ring holds 1,024 events — the same as a full
  batch — so a full batch that also mints a unique-violation entry can overrun
  a subscriber that is not keeping up in one go; it resumes from the oplog and
  loses nothing. ADR-119.

- **A `let` variable written into a `$lookup` sub-pipeline `$match` is now
  refused instead of silently matching nothing.** `docs/aggregation.md` said
  a `$$oid` in a sub-pipeline `$match` was refused; in fact
  `{$match: {_id: "$$oid"}}` was read as the literal five-character string,
  matched no document, and the join came back as an empty array on every
  input with a 200 — an empty result indistinguishable from a real one. Any
  string value beginning with `$$`, at any depth of a sub-pipeline `$match`
  (a plain equality, `$in`, `$elemMatch`, `$and`/`$or`/`$nor`, a `$regex`
  pattern), is now a 400 `bad_request` naming the variable, with or without a
  `let`, and in a nested `$lookup` too. The subtree under `$expr` is unchanged
  — it already refused an unknown variable. A top-level `$match` and a `find`
  filter are unchanged as well: there `"$$oid"` is the literal string a
  stored document may hold. Write the correlation as the docs show — bind the
  variable in an `$addFields` stage and `$match` on the computed field.
  `docs/deviations.md`.

- **Update operators apply in the order the request wrote them, and a
  document's fields are stored in the order they arrived.** The query
  language promised that two operators on one path apply in the order
  written and the last one wins, and its parser did that — but every request
  body was decoded into a JSON map that sorted its keys, so over HTTP and MCP
  the operators ran alphabetically whatever the body said: `{"$set": {"a":
  1}, "$inc": {"a": 5}}` on `a: 0` left `1` when it should leave `6`, and
  `{"$min": {"a": 3}, "$max": {"a": 10}}` on `a: 5` left `3` instead of `10`.
  The same sort was applied to every stored document's fields, so `{"zeta":
  1, "alpha": 2}` read back as `{"alpha": 2, "zeta": 1}`, and to a sort
  document's keys, so `{"sort": {"b": 1, "a": 1}}` sorted by `a` first. The
  JSON boundary now keeps the key order it is given, as MongoDB does. What
  changes for a client: an update that names one path twice gets the order it
  wrote, not the alphabetical one; a multi-key sort written out of
  alphabetical order now means what it says; documents written from this
  release on keep their field order, with `_id` first whatever the body said
  (a replace used to put it last), while documents stored earlier keep the
  sorted order they were written with until they are rewritten; and an
  inclusion projection answers in the document's field order, as MongoDB
  does, rather than the projection's. Whole-document comparison — `{"a":
  {"x": 1, "y": 2}}` as an equality filter, `$in` over documents — is
  field-order sensitive as it always was in the comparator and as it is in
  MongoDB; before, both sides had been sorted, so it was order-insensitive by
  accident, and a document stored before this release matches such a filter
  only when the filter is spelled in alphabetical order.
  A client whose JSON encoder does not preserve insertion order gets whichever
  order its encoder emitted; `docs/deviations.md` says how to serialise
  deliberately. ADR-120.

## 0.19.1 - 2026-09-02

### Changed

- **`DEEPINFRA_API_KEY` is on the default provider key allowlist.** 0.19.0's
  `vector.provider.allowed_key_env` defaulted to the three dialects' own
  variables and `KIMMY_PROVIDER_*`, which left out the variable the docs use
  for DeepInfra — a documented host of the `open_ai` dialect — so a collection
  configured the way `docs/vectors.md` shows was refused by name unless the
  operator listed it. The default now admits it too. Operators who set
  `allowed_key_env` explicitly are unaffected; the setting replaces the
  default rather than extending it. ADR-115, amended.

### Fixed

- **The server is no longer several times slower under concurrent clients
  than the same code linked against glibc.** Every release binary is a static
  musl build, and since 0.17.0 the container ships that same file; a musl
  binary that sets no allocator runs on musl's malloc, which serialises every
  allocation on one lock. Measured against the glibc build the container
  shipped before 0.17.0, at eight clients a paged `find` ran at 477 requests a
  second instead of 6,381, `count` at 19 instead of 171 and point reads at
  18,345 instead of 45,116, with p99 latencies three to eight times longer;
  at one client the gap was within 30%. `kimmyd` now sets mimalloc as its
  global allocator on every target, which recovers all of it and passes the
  glibc figures (8,289, 201 and 48,217 in the same cells). The cost is
  resident memory: under a burst of concurrent writes the node's peak was
  about twice the glibc binary's and four times the musl binary's, retained
  rather than in use, so a container sized tightly to the old peak wants
  headroom — `docs/operations.md` has the figure. ADR-117; the table and its
  conditions are in `docs/benchmarks.md`.

## 0.19.0 - 2026-09-01

A minor when it ships, not a patch. Nothing changes on the wire, on disk or
in the `/v1` API, members of this version and 0.18.0 replicate to each other,
and the roll is an ordinary one — but two things a 0.18.0 node accepted are
refused or answered differently now, and the pre-1.0 policy puts both behind
a `0.MINOR` bump. A vector configuration that names a private endpoint, or a
key variable the node has not listed, is refused; that reaches anyone running
Ollama or llama.cpp on `localhost` or the LAN, and the fix is one line of
`kimmy.toml`. And an aggregation expression path that crosses an array now
returns the array MongoDB returns, not its first element.

### Security

- **An embedding provider can no longer be pointed at the node's own secrets
  or its own network.** A collection's vector configuration names an endpoint
  and the environment variable holding its key, and the provider sent that
  variable's value to that endpoint; the only check was the URL's scheme. So
  anyone holding `ddl` on a collection could name `KIMMY_JWT_SECRET` as the
  key, point the endpoint at themselves, insert one document, and be sent the
  node's token-signing secret — or reach anything on the node's network, the
  hole the webhook address policy closed for webhooks and never applied here.
  ADR-115.

  Three rules now, all the operator's. Every `KIMMY_*` variable other than
  `KIMMY_PROVIDER_*` is refused as a provider's key by name, and no setting
  can allow one — listing one stops the node at startup. Every other variable
  must be in `vector.provider.allowed_key_env` (exact names or a prefix with
  one trailing `*`; default `OPENAI_API_KEY`, `COHERE_API_KEY`,
  `GEMINI_API_KEY`, `KIMMY_PROVIDER_*`). The endpoint is held to the webhook
  address policy under `vector.provider.allowed_hosts`, resolved and every
  address checked, and the provider's client checks again at connect time and
  follows no redirect. A refused configuration is a `400` naming the variable
  or the host, never a value, and the same checks run again when the worker
  builds the provider, because a configuration also arrives by replication.

  **The one thing to change: an Ollama or llama.cpp on `localhost` or a LAN
  address now needs its host in `vector.provider.allowed_hosts`.** Hosted
  providers at their default endpoints under their documented variables need
  nothing. Configuring or disabling embeddings now writes an audit record with
  the provider kind, the endpoint host or profile name, and the key variable's
  name. Operators who granted `ddl` to anyone but themselves should rotate
  `KIMMY_JWT_SECRET` (`auth.jwt_previous_secret` makes that a rolling change)
  and `KIMMY_CLUSTER_SECRET` after upgrading.

### Changed

- **Providers can be defined server-side, and a node can insist on them.**
  `[vector.providers.<name>]` in `kimmy.toml` defines a provider — endpoint,
  model, key variable — and a collection uses it as
  `{"kind": "profile", "name": "<name>"}`, carrying the name and nothing else.
  `vector.provider.endpoints_locked = true` makes `profile`, `byo` and `local`
  the only kinds a collection may configure, so the places a node sends text
  are exactly the ones its operator wrote down. Profiles are held to the same
  key and address rules at startup and by `kimmyd check-config`. ADR-115.
- The egress address policy moved from `kimmy-api` into its own crate,
  `kimmy-egress`, so webhooks and providers share one denylist. No change to
  webhook behaviour; a refusal now says which subsystem and which setting.

### Fixed

- **A field path through an array is now the array of what it found.**
  `"$items.sku"` over `items: [{sku: "a"}, {sku: "b"}]` evaluated to `"a"`;
  it is `["a", "b"]` now, as it is in MongoDB, in every place an expression
  is taken: `$project`, `$addFields`, `$replaceRoot`, a `$group` key or
  accumulator argument, `$expr`, and a path into `$$ROOT` or a variable.
  Elements that are not documents or lack the field are skipped, each array
  crossed adds one level (`$a.b` over `a: [{b: [1, 2]}, {b: 3}]` is
  `[[1, 2], 3]`), and a numeric segment names a field, never a position.
  **Results change for any pipeline that read such a path and relied on
  getting one value:** `$group: {_id: "$items.sku"}` buckets by the whole
  array rather than by the first element's sku, `$push` of it collects
  arrays, and `{$size: "$items.sku"}` — which was an error — counts. The
  `$map` form the docs recommended still works and returns the same thing.
  `$unwind`, `$sort` and `$lookup`'s `localField`/`foreignField` name a field
  rather than compute one and are unchanged. ADR-116; the deviations entry
  that recorded the old behaviour is gone.
- A client that keeps sending an oversized request body now receives the
  `413 payload_too_large` refusal instead of a connection reset. The server
  used to close on the unread remainder of the body, and the reset that
  answered the bytes still in flight could discard the response before the
  client read it — on macOS it did. The remainder is now read and discarded
  first, bounded by the declared `Content-Length`, a 4 MiB cap and two
  seconds; a client further over than that is closed on as before.

## 0.18.0 - 2026-08-31

A minor rather than a patch, and one that only ever gives an operator back
something the last release took away. 0.17.0 began refusing federated access
tokens that live longer than fifteen minutes; this removes that refusal and the
setting behind it. Nothing on the wire, on disk or in the `/v1` API moved,
0.18.0 and 0.17.0 members replicate to each other, and the roll is an ordinary
one.

**No token that worked under 0.17.0 is refused now, and nothing needs changing
at your identity provider.** The one thing to check before upgrading is where
you set `max_token_lifetime_secs`, if you set it at all: in `kimmy.toml` or on
the command line it has to come out or the node will not start, while
`KIMMY_OIDC_MAX_TOKEN_LIFETIME_SECS` in an environment block can be left where
it is and is ignored. The entry below says why the three differ, and why
clearing the variable *before* you upgrade is the one thing not to do.

If you are upgrading from 0.16.x, read 0.17.0's notes as well — its two startup
refusals still apply, and its third item does not.

### Removed

- **The maximum federated token lifetime, and everything that served it.**
  `auth.oidc.max_token_lifetime_secs`, `--oidc-max-token-lifetime-secs`,
  `KIMMY_OIDC_MAX_TOKEN_LIFETIME_SECS`, the two 401 refusals and the startup
  range check are all gone. **A federated access token of any lifetime now
  verifies, including one with no `iat`.** ADR-112 supersedes ADR-096, which
  shipped one release ago and is marked superseded in place.

  The refusal turned a correctly configured identity provider into a total
  authentication outage on upgrade. Okta, Google and Entra ID default access
  tokens to about an hour and Auth0 defaults an API's to a day — every one of
  them over the 900-second default. Worse, nothing could predict it: the
  lifetime belongs to the provider and arrives only with a real token, so
  `check-config` passed, the node started, the startup summary said nothing,
  and then every federated request was refused. And the thing being refused was
  the operator's own provider configuration, which is theirs to set and which
  serves their other systems too. No comparable product does this.

  The window ADR-096 was bounding is real and unchanged: a revocation at the
  provider is honoured here when the token expires, so **your provider's
  access-token lifetime is your revocation window**. That is now stated in
  [docs/security.md](docs/security.md) and set where it belongs, at the
  provider, where it also protects everything else those tokens reach.

  **Upgrading — the three ways of setting it behave differently, so there is no
  single instruction:**
  - A `kimmy.toml` that sets `max_token_lifetime_secs` **will not start**; the
    key must be removed. Configuration denies unknown fields.
  - A `--oidc-max-token-lifetime-secs` flag in a unit file or entrypoint
    **will not start**; it must be removed.
  - `KIMMY_OIDC_MAX_TOKEN_LIFETIME_SECS` **may be left set** and is ignored.
    Nothing declares the variable any more, so nothing reads it. Do not clear
    it *before* upgrading: on the previous release that reverts the node to the
    900-second default and causes the outage this release removes.

  Nobody needs to change anything at their provider, and no token that worked
  before is refused now.

### Fixed

- **The 0.17.0 upgrade note undercounted what to check.** It named two
  configurations refused on upgrade and the token lifetime was a third, with
  the distinction that matters left unsaid: the other two are refused at
  startup and `check-config` catches them, while this one is refused at
  request time and no check on this node can see the provider's lifetime in
  advance. 0.17.0's section below says so now — which matters to anyone
  upgrading from 0.16.x, who reads it on the way past.
- **Seven rows in [docs/threat-model.md](docs/threat-model.md) still marked
  controls "next release" that shipped in 0.17.0** — the HS256 32-byte floor
  and the placeholder-secret refusal (ADR-093), `auth.jwt_previous_secret`
  (ADR-101), `auth.local.login` and `auth.oidc.subject_claim` (ADR-100), the
  request limits (ADR-099) and `auth.oidc.max_token_lifetime_secs` (ADR-096).
  A marker left up reads as a control the operator does not have yet, which is
  the wrong direction for a threat model to be wrong in. ADR-110 already said
  the markers had to be swept on release; a test now refuses a dated release
  that still carries one, so the convention is checked rather than remembered.

## 0.17.0 - 2026-08-31

A minor when it ships, not a patch. Nothing changes on the wire, on disk or
in the `/v1` API, members of this version and 0.16.x replicate to each other,
and the upgrade is an ordinary rolling one — but three configurations that
worked under 0.16.x are refused now, and the pre-1.0 policy puts a refusal of
that kind behind a `0.MINOR` bump.

**Two are refused at startup, and `kimmyd check-config` against the new binary
answers both without starting anything:** a `jwt_secret` of 16 to 31 bytes must
be replaced with one of 32 or more (rotating it ends every session once, on
every node at the same time), and a node reachable from the network must not be
running on one of this repository's own example secrets.

**The third is refused at request time, and no check on this node can see it in
advance.** `auth.oidc.max_token_lifetime_secs` (below) refuses any federated
access token whose own `exp − iat` exceeds 900 seconds. The lifetime is the
*provider's* to choose and arrives only with a real token, so `check-config`
passes, the node starts, the startup banner says nothing, and every federated
request 401s from the first one. A provider minting hour-long tokens — the
default for Okta, Google and Entra ID, and a day for Auth0 — turns a working
federation into a total authentication outage on upgrade. **Before upgrading,
read the access-token lifetime your provider mints for this resource** and
either shorten it there or set `KIMMY_OIDC_MAX_TOKEN_LIFETIME_SECS` to
something that admits it. This is the one item in the release that a careful
operator can do everything else right and still be caught by.

**If you are upgrading past 0.17.0, skip this paragraph:** the setting and the
refusal were removed in the next release, so there is nothing to configure. See
that release's notes for what to do with a value you have already set.

Nothing else asks anything of an operator: the rest is additive query, search
and configuration surface — the entries below say what — along with settings
whose defaults are meant to be left alone, and documentation corrections.

### Added

- **`$setOnInsert`.** Fields written only to the document an upsert creates,
  and left alone on a match — the created-at idiom `{$setOnInsert: {created:
  t}, $inc: {n: 1}}` on `find_and_modify` with `upsert: true`. The inserted
  document is the filter's equalities, then `$setOnInsert`, then the other
  operators. A `$setOnInsert` path that another operator in the same update
  also writes — the same path, a prefix or an extension of it, or a
  `$rename`'s destination — is refused with a `400`, as MongoDB refuses it.
  Other operator pairs are still applied in order rather than checked; the
  reason that stays a non-change is in `docs/deviations.md`.
- **`$push` modifiers `$position`, `$sort` and `$slice`** alongside `$each`,
  applied in that order: insert at an index (negative from the end), order
  whole elements by `1` / `-1` or document elements by a `{field: direction}`
  specification using the engine's canonical comparison, then keep the first
  `n` or last `-n`. `{$each: [x], $sort: {t: 1}, $slice: -100}` is a capped,
  ordered history in one write. A modifier without `$each`, or a clause
  `$push` does not know, is an error rather than a value pushed literally.
  `$addToSet` takes `$each` and refuses the other three, which have no
  meaning on a set.
- **`[vector.batch]`** — `max_chunks` (32), `max_tokens` (32768, estimated)
  and `max_wait_ms` (100) bound one embedding provider call. Process-level
  rather than per collection, because they describe the round trip this
  node makes and not the collection; documented in `kimmy.example.toml` and
  [docs/operations.md](docs/operations.md#settings).
- **`$expr` in filters.** `{$expr: <aggregation expression>}` is a filter
  clause everywhere a filter is taken — `find`, `count`, `update`, `delete`,
  `find_and_modify`, `$match`, the vector pre-filter and the MCP tools. The
  expression is evaluated against the whole document and the clause matches
  when the result is truthy, so `{$expr: {$gt: ["$spent", "$budget"]}}` is
  the over-budget query that previously needed an aggregation, and `{$expr:
  {$gt: [{$multiply: ["$qty", "$price"]}, 100]}}` computes on the way. The
  whole expression operator set is available. Comparison operators inside
  `$expr` are the *expression* ones — canonical cross-type order, arrays
  compared whole — which differ from the filter operators in the cases
  `docs/query-language.md` sets side by side. `$expr` is never indexable; an
  indexable clause beside it still plans, and `explain` reports which. An
  expression that cannot be evaluated for a particular document (a type
  violation) makes that document a non-match rather than failing the request;
  that and one deliberate leniency are recorded in `docs/deviations.md`.
  ADR-106.
- **Variables in expressions.** `$$ROOT` and `$$CURRENT` name the document;
  `{$let: {vars: {...}, in: <expr>}}` binds names for its body; `$$name.path`
  reads into a variable's value. Scopes nest and shadow lexically. A `$$name`
  that nothing binds is a `400` at parse time, before any document is read,
  rather than a null in every row.
- **Array expression operators.** `$size`, `$arrayElemAt` (negative from the
  end, null out of range), `$first`, `$last`, `$slice` (two- and three-argument
  forms), `$concatArrays`, `$in` (expression form, `[value, array]`),
  `$indexOfArray`, `$isArray`, `$reverseArray`, `$range` (capped at 100,000
  elements), and the three that iterate with a bound variable: `$filter`
  (`input`, `as`, `cond`, optional `limit`), `$map` (`input`, `as`, `in`) and
  `$reduce` (`input`, `initialValue`, `in` with `$$value` and `$$this`). Null or
  a missing field yields null throughout; any other non-array is an error.
  Four places where that is more lenient than MongoDB are recorded in
  `docs/deviations.md`.
- **`$lookup` `let`/`pipeline` form.** `{$lookup: {from, let: {name: <expr over
  the local document>}, pipeline: [<stages over the foreign collection>], as}}`.
  The `let` is evaluated per input document and visible as `$$name` in every
  stage of the sub-pipeline, `$$ROOT` there is the foreign document, and a
  nested `$lookup` inside the sub-pipeline sees the outer `let` too. The
  foreign collection is read once; a leading `$match` in the sub-pipeline is
  applied once before the per-document loop. This form is O(local × foreign)
  by construction — `docs/aggregation.md` says when to prefer the
  `localField`/`foreignField` form, which remains a single pass. Authorized
  against `from` exactly as the equality form is. Carrying both forms in one
  stage is refused.
- **`weights` and `min_overlap` on `hybrid_search`.** Measured on a corpus of
  short conversational documents, hybrid search recalled roughly a third less
  than plain vector search on the same queries, for every one of eight
  embedding models. The lexical half ranks by term overlap, and on documents
  of a sentence or two nearly every candidate shares a word with the query,
  so that ranking is close to random — and equal-weight reciprocal rank
  fusion gave it the same say as the dense rank. `weights`
  (`{ "dense": w, "lexical": w }`, both `>= 0`, not both zero) scales each
  half's contribution; `min_overlap` (`>= 1`) is the number of distinct query
  terms a chunk must contain before it counts as lexical evidence, and a
  document it removes from the lexical half keeps whatever the dense half
  gave it. Defaults are `{1, 1}` and `1`: plain RRF, as before. The MCP
  `hybrid_search` tool and `kimmy hybrid-search` (`--dense-weight`,
  `--lexical-weight`, `--min-overlap`) take the same controls. ADR-094.
- **`auth.oidc.max_token_lifetime_secs`** (`KIMMY_OIDC_MAX_TOKEN_LIFETIME_SECS`),
  default `900`: a federated token whose own `exp − iat` exceeds it is refused,
  as is one with no `iat`. A federated principal's role membership is frozen in
  its access token, so a revocation at the provider was honoured only when the
  token expired — for however long the provider had chosen (ADR-073). This
  bounds that window from this side. The refusal is a 401 whose
  `WWW-Authenticate` challenge names the limit in seconds and nothing about the
  token. **Before upgrading, check the access-token lifetime your provider
  mints for this resource:** at or below 900 seconds nothing changes; above it,
  shorten it at the provider or raise the limit knowingly. Refused outside
  1–86400; a raised limit is printed in the startup summary. ADR-096.
- **`server.request_timeout_secs`** (default `30`, `KIMMY_REQUEST_TIMEOUT_SECS`).
  A deadline on every REST route that answers with a document. A request
  still *waiting* at the deadline — for the rest of its body, or for an
  embedding provider — is abandoned and answered **`503`** with the new error
  code **`timeout`** (`retry: wait`). It does not cut short storage work
  already running: a scan, a bulk insert, an index backfill or a database drop
  completes and is answered with its result, so it is not a query timeout and
  none of those needed an exemption. The change-stream upgrade
  (`/v1/db/{db}/coll/{coll}/watch`) and `/mcp` answer with a connection and
  carry no deadline.
- **`server.max_body_bytes`** (default `2097152`, `KIMMY_MAX_BODY_BYTES`). The
  request body ceiling, previously axum's fixed 2 MiB, as a setting. Over it,
  `413 payload_too_large` as before. `/mcp` keeps rmcp's own 4 MiB limit.
- **`server.rate_limit.per_principal`** and **`per_principal_window_secs`**
  (defaults `0` — off — and `60`; `KIMMY_RATE_LIMIT_PER_PRINCIPAL`,
  `KIMMY_RATE_LIMIT_PER_PRINCIPAL_WINDOW_SECS`). A second token bucket, keyed
  on the authenticated principal — a local user by name, a federated identity
  by issuer and subject — checked after the token is verified on every route
  that takes one, REST, `/mcp` and change streams alike. Over it, `429` with
  `Retry-After`, the same response the login limiter gives; a client should
  now treat a `429` as possible on any authenticated call. The key map shares
  `max_tracked_keys` with the login limiters. The documentation offers `3000`
  over `60` seconds as a starting point and says what it is relative to.
- **`kimmy_rate_limited_principal_total`** on `/metrics` (and
  `kimmy.rate_limited.principal` over OTLP): the part of
  `kimmy_rate_limited_total` refused by the per-principal limit. A sibling
  series rather than a label, so the existing series is byte-for-byte what it
  was.
- `check-config` refuses `request_timeout_secs = 0`, `max_body_bytes = 0`, and
  a per-principal window of `0` with a non-zero burst, by name.
- **`auth.local.login`** (`KIMMY_LOCAL_LOGIN`, `--local-login`): `always`,
  `loopback_only` or `disabled`. Under `loopback_only`, `POST /v1/auth/login`
  and `POST /v1/auth/refresh` answer only a connection whose peer address is
  loopback and refuse everyone else with a 403 carrying the `forbidden` code;
  under `disabled` both answer 404. The mode governs the *minting* of local
  tokens: a local token already issued keeps verifying under every mode,
  federated tokens are untouched, and the break-glass root stays reachable
  from the node's own host under `loopback_only`. Startup refuses `disabled`
  unless `auth.oidc` is configured, because a node with neither could
  authenticate nobody. The startup summary and `check-config` name the mode;
  `kimmy login <user>` explains a 403 or 404 from the login route rather than
  printing it bare.
- **`auth.oidc.subject_claim`** (`KIMMY_OIDC_SUBJECT_CLAIM`,
  `--oidc-subject-claim`): a claim — `preferred_username`, `email`, `upn` —
  whose string value is carried as a federated principal's *display* name.
  `GET /v1/auth/whoami` gains a required `display` field (the claim's value,
  or the subject when the claim is absent, not a string, or not configured),
  and an audit record gains a `display` field when the value differs from
  `user`. Display only: `sub` remains the identity for authorization, role
  resolution, rate limiting and every comparison the server makes, because
  an email is mutable and not unique across providers. A subject whose email
  changes keeps its roles.
- **`auth.jwt_previous_secret` (`KIMMY_JWT_PREVIOUS_SECRET`,
  `--jwt-previous-secret`): a two-key window for rotating `jwt_secret`.**
  Tokens are always signed with `jwt_secret`; a token is accepted if either
  secret verifies it, the current one tried first. Rotate by moving the old
  value to `jwt_previous_secret`, putting the new one in `jwt_secret`, rolling
  every node, waiting one `token_ttl_secs`, and removing the previous secret.
  The node logs an `info` at startup naming that deadline and one `warn` when
  it passes (counted from process start; not persisted across restarts). The
  previous secret is held to the same length floor as the current one and to
  the same placeholder refusal off loopback, must
  differ from it, and every refusal is `check-config`'s too; the startup summary
  says `jwt_previous_secret=set` and never the value. Rotation does not revoke
  — the token version still does that, identically for a token the previous
  secret verified — and `cluster_secret` is not covered. ADR-101; procedure in
  `docs/security.md`.
- **`describe` reports the node's durability class.** `GET
  /v1/db/{db}/coll/{coll}/describe` — and through it the MCP
  `describe_collection` tool — carries `nodeDurability`: `durable` or
  `coalesced`, the same value `GET /v1/version` reports as `durability`
  (ADR-088). It is a fact about the node that answered, not about the
  collection, which is what the name says; it is on `describe` because that
  is the one call a client makes before writing, and it said everything
  about a collection except what an acknowledged write to it means.
- **Positional update paths and `arrayFilters`.** An update path may contain
  `$[]` (every element) and `$[<identifier>]` (the elements an `arrayFilters`
  entry on the request selects): `{"$set": {"items.$[line].shipped": true}}`
  with `"arrayFilters": [{"line.sku": "gasket"}]` marks one line item shipped
  and touches nothing else, so a concurrent update to another field survives
  where a whole-document replacement would have lost it. A filter takes any
  filter operator, tests several fields of the same element, addresses scalar
  elements through a bare identifier, and nests
  (`orders.$[o].items.$[i].qty`). Every operator that takes a path accepts
  one except `$rename`; `$unset` of an element leaves `null` in its place.
  The field is `arrayFilters` on `update` and `find_and_modify`, on the MCP
  `update` tool, `kimmy update --array-filters`, `UpdateOptions` in the Rust
  and Go clients and `array_filters=` in Python; the conformance suite gains a
  scenario for it. MongoDB's `$` — the element the query matched — is not
  implemented and is refused with a message naming the replacement; ADR-104
  and `docs/deviations.md` say why.

- **Build provenance for the container image, verifiable with `gh`.** The
  release workflow records a signed SLSA provenance attestation against the
  image manifest's digest — keyless, through Sigstore, under the workflow
  run's own identity — so that
  `gh attestation verify oci://ghcr.io/titusai-io/kimmydb:<version> -R titusai-io/kimmydb`
  proves the image was built by this repository's release workflow at the
  tagged commit. One caveat, stated plainly: GitHub issues attestations for a
  private repository only on an Enterprise Cloud plan, so the step is
  conditional on the repository being public, no release made before that
  carries one, and the release archives are switched on at the same moment
  (`dist-workspace.toml`). The commands, and what a successful verification
  shows, are in [Verifying a release](docs/operations.md#verifying-a-release).
  The dependency policy (`deny.toml`), the update cadence, and the licensing
  boundary check are described under
  [Supply chain](docs/security.md#supply-chain); ADR-108 records the decision.
- **`vector.index_cache.max_bytes`** bounds the HNSW graphs a node keeps in
  memory across its vector collections — 512 MiB by default, `0` for the old
  unbounded behaviour. A graph costs about `dim × 4 + 5,000` bytes per chunk
  (6.5 KB at 384 dimensions, 11 KB at 1,536 — measured, and about twice what
  the vector alone suggests) and was kept for the life of the process once its
  collection had been searched. Past the budget the least recently searched
  graphs are evicted and their collections pay a snapshot reload, or a
  rebuild, on their next search; a single graph larger than the whole budget
  is held anyway, with a warning, because a search is never refused over
  memory. `/metrics` gains `kimmy_vector_index_cache_bytes`, the resident
  total by the same estimate (ADR-103).


- **A CycloneDX SBOM per binary per target, on every release.** Beside each
  `kimmyd-<target>.tar.xz` and `kimmy-cli-<target>.tar.xz` on the Release
  page is a `<name>.cdx.json` (CycloneDX 1.5) listing every crate compiled
  into that binary — versions, licences, package hashes and the dependency
  graph — with a `.sha256` beside it. Feed it to whatever already scans your
  dependencies (`grype sbom:…`, `osv-scanner --sbom …`, Dependency-Track)
  without pulling the image or building from source. Generated from
  `Cargo.lock` at the release commit by `scripts/sbom.sh` inside the release
  workflow (ADR-110); [Security › Software bill of materials](docs/security.md#software-bill-of-materials)
  says how to verify and consume one.
- **A written threat model**, [`docs/threat-model.md`](docs/threat-model.md):
  the assets, every trust boundary with its threats and the control in place
  for each — naming the file the control lives in — what is explicitly not
  defended against, and the operational assumptions the controls rest on.
  Nothing in it is new behaviour; it is the security model written down in
  one place and checked against the code. The FIPS position is stated there
  too: `aws-lc-rs` has a validated mode, this build uses `ring`, and no FIPS
  claim is made.
- **Type conversion expressions.** `$convert: {input, to, onError?, onNull?}`
  with `to` as a type name or numeric BSON code, and the shorthands
  `$toString`, `$toInt`, `$toLong`, `$toDouble`, `$toBool`, `$toDate` and
  `$toObjectId`. Null or missing input is null (or `onNull`); a value with no
  conversion is a `400` naming both types (or `onError`); integers are refused
  out of range rather than wrapped; strings are parsed strictly; a date
  converts to and from epoch milliseconds and to and from ISO 8601 text. The
  two shapes it exists for: a `$lookup` whose keys differ in type across
  collections, and a `$group` by a date that was stored as a string. `decimal`
  is refused, because `Decimal128` has no exact key encoding here (ADR-005).
- **`$mod` in filters.** `{field: {$mod: [divisor, remainder]}}`, with
  MongoDB's rules: numeric values only, doubles truncated toward zero, the
  remainder keeping the dividend's sign, arrays matched element-wise, a zero
  divisor refused at parse. Never index-eligible; an equality or range beside
  it still plans.
- **`$pullAll`.** Removes every element equal to any value in the given list,
  by the same canonical equality `$pull` uses. A missing field is a no-op; a
  non-array field is a `400`.

### Changed

- **The HS256 signing key must be at least 32 bytes; it was 16.** RFC 7518
  §3.2 asks for a key no shorter than the hash's output, 256 bits, and a
  short key shared by every node of a cluster is the one weakness a single
  captured token is enough to attack offline. `TokenIssuer` and
  `check-config` refuse the same values, and the error names the floor.
  A secret of 16–31 bytes has to be rotated before the upgrade; the docs,
  the example configuration and the quick starts say 32 now (ADR-093).
- **Placeholder secrets are refused off loopback.** A node whose HTTP
  listener — or, with clustering on, whose cluster listener — binds anything
  other than a loopback address refuses to start when `auth.root_password`,
  `auth.jwt_secret`, `auth.jwt_previous_secret` or `cluster.cluster_secret`
  is one of the values this
  repository's own examples use: `changeme`, `change-me`, `hunter2`, the
  defaults the compose file used to fall back to, the commented-out lines in
  `kimmy.example.toml`, and the obvious words (`password`, `secret`, `root`,
  …; the full list is `PLACEHOLDER_SECRETS` in `kimmyd`). The error names
  the setting and never the value. On `127.0.0.1` the same values are
  accepted, so local development and the examples still run unchanged
  (ADR-093).
- **`docker-compose.yml` no longer supplies default secrets.** Its
  `KIMMY_ROOT_PASSWORD`, `KIMMY_JWT_SECRET` and `KIMMY_CLUSTER_SECRET` were
  placeholders, and inside a container the node listens on every interface,
  so the file would now start nothing. Compose stops at `up` and names the
  missing variable; a `.env` beside the file (gitignored) or the environment
  supplies them, and the file says how to generate each.
- **Quick starts generate their secrets.** The README and the operations
  guide ran the container with `change-me` and a 20-byte signing key; both
  would be refused now, so the commands generate a root password and a key
  with `openssl rand` and the login line reads the password back from the
  environment.
- **The security guide states what a local token is**: a signed,
  unencrypted, readable grant list. Anyone holding one can read its user,
  grants, roles and expiry without the secret; the secret provides integrity,
  and confidentiality comes from TLS and from handling the token as a
  credential.
- **The embedding worker batches provider calls across documents**
  ([ADR-095](docs/decisions.md)). It used to send one document's chunks per
  call, so a document short enough to be one chunk paid a whole round trip
  by itself: measured against a CPU llama.cpp server, 32 calls of one short
  input took 394 ms and one call of 32 took 18 ms, and a live ingest arriving
  a little faster than that per-document floor grew its backlog without
  bound. The worker now fills a call from consecutive documents of the same
  collection, on the streaming path and in a backfill alike, up to the
  bounds above. The storage write is still one per document, the oplog
  position is still recorded after the work, and a batch that fails
  permanently is taken apart so the one document at fault is skipped and
  named while the rest land. `kimmy_embed_documents_total` and
  `kimmy_embed_chunks_total` count what they always did;
  `kimmy_embed_provider_requests_total` now climbs more slowly than chunks,
  and chunks over requests is the batch size achieved.
- **A sorted `find` holds at most 10,000 documents: `skip + limit`.** Beyond
  that it is refused with `400` and a message saying how to page instead —
  sort by `_id` and follow `nextCursor`, or narrow the filter on the sort
  field to where the last page ended. An unsorted `find`, or one sorted by
  `{"_id": 1}`, holds only its page and keeps its unbounded `skip`. Refused
  rather than clamped because a clamped `skip` would return a different page
  and say nothing.
- **`explain` reports `indexEntriesRead`** when an index answered a read: how
  much of the index the query touched, as distinct from `documentsExamined`.
  Additive; absent for a scan, an `idLookup`, and for filtered writes.
- **A graph is built off the index-cache lock.** It was built while holding
  the lock every collection's entry lives in, so one collection's rebuild —
  4 s at 4,000 vectors, minutes at tens of thousands — stalled vector and
  hybrid search on every collection the node serves for that long, and did so
  on an async worker thread. The lock is now held to look and to install; the
  build runs under a per-collection lock on a thread the runtime is told
  about, as a storage commit's fsync has been since 0.16.2; and concurrent
  searches on the collection being built take the previous graph, or wait for
  that one build rather than starting a duplicate.
- **A build holds less.** It read every chunk record whole — text included —
  and kept the lot until its reachability probe had finished, so a rebuild's
  peak was the graph plus a copy of the shadow collection, every staleness
  window under writes and up to three times when a build was discarded. It
  now reads only keys and vectors, frees each vector as the graph takes it,
  and probes with a 128-vector sample copied out first.
- **A search `filter` uses secondary indexes, and a selective one is joined
  the other way round.** `vector_search` and `hybrid_search` evaluated
  `filter` by scanning the source collection and keeping every matching id,
  whatever indexes existed. The filter now runs through the query planner —
  primary key, secondary index or scan, with the same recheck `find`
  applies — and keeps only the ids, a page at a time. When it admits at most
  1,000 documents the search reads those documents' chunks by key and scores
  them exactly, instead of searching everything and discarding what the
  filter excluded; above that it searches as before and discards. The hits
  are the same either way, and for a selective filter they are now exact
  where the graph walk could previously come back with fewer than `k`.
  [docs/vectors.md](docs/vectors.md#search) describes the rule; ADR-102 the
  reasoning.
- **The exact and lexical search paths hold only the top `k`.** The exact
  vector path — collections under 500 chunks, the `dot` metric, a failed
  graph build — and the lexical half of `hybrid_search` built a hit, text
  included, for every chunk they scored and sorted the lot. Both now keep a
  bounded set of the best `k` as chunks arrive, with the per-document cap
  applied on the way in, so a search over a large shadow collection costs
  memory proportional to `k`. Ties on score are ordered by chunk key rather
  than by scan order: stable either way, but a different order for exact
  ties.
- **Reading one document's vectors no longer scans the shadow collection.**
  A document's chunks are one contiguous run under its id, and
  `GET .../docs/{id}/vectors`, the write of a document's vectors and the
  embedding worker's staleness check now read that run rather than every
  record in the shadow. Same results; the cost is the document's chunk count
  instead of the collection's.
- **A pipeline's leading `$match` uses indexes, and the ceiling is measured
  after it.** `aggregate` now reads its source through the same planner-backed
  scan `find` uses, with the first `$match` stage — or the first several,
  merged — as the scan's filter. An indexed equality, range or `$in` there
  reads its candidates rather than the whole collection, and the
  100,000-document limit applies to what the `$match` admits, so a pipeline
  over a larger collection runs when its leading `$match` is selective enough.
  Only the leading `$match` is pushed down; a `$match` after any other stage
  runs where it was written, on what that stage produced, so results are
  unchanged with or without an index (ADR-109). The refusal for an oversized
  source names the leading `$match` when there is one.

### Fixed

- **`docs/query-language.md` no longer lists the aggregation pipeline and
  index-backed `$in` as planned.** Both have shipped — the pipeline has its own
  page — and the "Not implemented" table had not been updated to say so.
- **A restarted member no longer names its converged peers stale on its
  first round.** On a roll, half a second after a member came back, its
  first sync round logged `peer trails this node by more than tombstone
  retention … a stale rejoiner should be reset, not merged` for *both*
  peers, `behind_secs` reading the time since the previous roll, and
  withdrew it five seconds later. The peers were converged and had never
  been away; the member had just written its topology record after 36
  hours of writing nothing, and the span between that write and the
  previous one is what the measure read. The verdict now also requires that
  the peer lack something retention has removed here at that origin — a
  peer that can still be served every entry it lacks has nothing to
  resurrect. A genuinely stale peer is still named, on the first round that
  sees it, with the same `behind_secs` and the same `staleSince` /
  `behindSecs` on `GET /v1/topology`. If you alert on that line, a roll no
  longer trips it.
- **A restarted member no longer answers `BeyondHorizon` to its first
  puller.** Each time a member came back, the first peer to pull from it
  logged `behind the peer's retention horizon; falling back to a snapshot`
  and transferred the whole store to learn one entry. The puller asked from
  its coverage of the member's origin — the member's previous write, from
  before the silence, collected long ago along with everything around it —
  and by a single horizon stamp that is beyond it. The puller now sends its
  vector with the request, and the member serves it when nothing it lacks
  has been collected, snapshot otherwise. On a large store this was the
  real cost of a routine restart.

Both fixes take full effect from the *second* roll onto this build: a
database an earlier build collected from has no per-origin record of what
was removed, so it is seeded with the old horizon and stays coarse below
it until each member has written and been collected once more. A member
running this build interoperates with one that does not, in either
direction, at the previous behaviour.
- **`count` no longer decodes every match into memory.** It took the length
  of a collected vector, which over a large collection — or a `__vectors`
  shadow, where every document is a vector with its text — cost the memory
  of the collection per request. It now counts as it goes and holds nothing.
- **An index-backed `find` no longer gathers every candidate key before
  reading the first document.** The range's keys were collected, sorted and
  deduplicated up front, so an unselective equality with `limit: 1` held the
  whole range and a `$in` union added a set on top. Candidates now stream out
  of one read transaction and are rechecked as they arrive: an equality on a
  complete key is one run already in `_id` order and stops where the page
  does; a `$in` is a merge of such runs; a range needing `_id` order keeps the
  `skip + limit` smallest keys of a pass. Results and their order are
  unchanged.
- **A sorted `find` no longer collects every match to sort it.** It keeps a
  heap of the `skip + limit` least under the sort, with `_id` ascending as
  the final key — the order a stable sort over an `_id`-ordered scan
  produced, so every page is the page it was.
- **The violations route no longer lists documents whose value has since
  been rewritten.** `GET …/violations` reported a recorded collision as long
  as every document it named still existed, so resolving one by rewriting a
  colliding value — the recipe the documentation gives alongside deletion —
  left it in the report until the oplog entry aged out. Each group is now
  re-evaluated when asked: a member deleted or rewritten so that its index
  keys no longer meet another member's leaves the group, a group with fewer
  than two members left is not reported, and an index that has been dropped
  or made non-unique no longer contributes any. Nothing is stored or written
  by the route; the change-stream event and the `/metrics` count are as
  before.
- **An update can no longer reach under `_id`.** The operators refused `_id`
  itself, but `{"$set": {"_id.x": 1}}` named a path *beneath* it, and setting
  a path beneath a scalar replaces the scalar with a document to make room —
  so `_id: 7` became `_id: {"x": 1}` and the document quietly left every index
  entry and oplog record that named it. `$set`, `$unset`, `$inc` and the rest
  now refuse any path under `_id` with the same `400` the bare name gets, and
  `$rename` refuses to rename onto one. Found by the fuzz harness on its first
  run over its own seeds (ADR-111).
- **A binary value's subtype survives the JSON boundary.** Responses always
  wrote it — `{"$binary": {"base64": …, "subType": "04"}}` for a UUID — but
  the request side ignored it and decoded every binary as generic, so a
  client that read a document and wrote it back changed the value without
  either side noticing. `subType` is now honoured on the way in: absent still
  means generic, and anything other than two hex digits is a malformed
  wrapper (`400`) like any other. Found by fuzzing (ADR-111).

## 0.16.4 - 2026-08-29

A patch: no wire, storage-format or API break; rolling upgrade. Four
additions that fell out of the embedding-model evaluation — two of them the
provider-generality gaps it exposed — and three fixes, one of which changes a
default: redb's page cache is bounded at 256 MiB now instead of 1 GiB, which
is where a node's resident memory was going.

### Added

- **`document_prefix` and `query_prefix` on a vector configuration.** Many
  embedding models are trained to see a task marker in the text — `passage:`
  / `query:` for E5 and BGE, `title: none | text:` for EmbeddingGemma, an
  instruction line for Nemotron and Qwen3 — and rank markedly worse without
  it: on the 2026-08-29 evaluation the two instruction-trained models called
  with bare text were the worst of eight (Nemotron-1B recall 0.40, e5-large
  unable to separate answers from nonsense). The worker now prepends
  `document_prefix` to every chunk it sends and the search path prepends
  `query_prefix` to `query` text; neither is stored or returned, and
  changing either reindexes like any configuration change.
- **`storage.cache_bytes`** bounds redb's page cache, which is most of a
  node's resident memory. It was redb's fixed 1 GiB default; it is 256 MiB
  now. The cache fills with reads and evicts only for room, never on a timer,
  so a node's RSS settles at its busiest period's level — three members
  measured at 460–590 MiB with every collection dropped — and this is the
  setting that decides where that level is.
- **`dimensions` on the `open_ai` provider.** Sent as the OpenAI request
  field of that name, so Matryoshka-trained models return a narrower
  vector than their native width; `dim` must equal it. Until now every model
  was stored at native width with no way to ask for less — and width is the
  real cost of a strong model (a 4096-wide one is four times the vector
  bytes of a 1024-wide one, and several times that on disk).
- **`DELETE /v1/db/{db}`** drops every collection in a database (`ddl` over
  the database; system databases refused).

### Fixed

- **An `open_ai` endpoint that already names the embeddings route is used as
  is.** The dialect appended `/v1/embeddings` to whatever `endpoint` said, so
  a provider that mounts the OpenAI-compatible API under a prefix —
  DeepInfra's documented `…/v1/openai/embeddings`, Azure's
  `…/openai/deployments/<name>/embeddings?api-version=…`, a gateway under a
  path — answered 404 on every call with no way round it. A setting ending in
  `/embeddings` (query string allowed) is now the full URL; a bare base still
  gets the standard suffix.
- **An emptied database no longer lingers in listings.** Creation was
  implicit in the first collection but removal was not implicit in the last,
  so a database whose collections had all been dropped stayed listed on every
  member forever. The last drop now removes it — decided inside the
  replicated drop, so peers converge without a database-drop entry of their
  own.
- **A collection missing on a clustered node says `retry: "elsewhere"`.**
  Created through a per-request load balancer, a collection lands on one
  member and reaches the others a sync round later; a request that arrived
  on another member in between got `404` with `retry: "no"`, which told the
  client that had just created it to give up. A node with peers now answers
  `elsewhere` (another member has it, this one will shortly); a node with no
  peers keeps `no`.

## 0.16.3 - 2026-08-28

A patch: one additive pair of `/metrics` series, nothing else.

### Added

- **`/metrics` reports what the embedding provider was asked and billed for.**
  `kimmy_embed_provider_requests_total` counts provider calls answered, and
  `kimmy_embed_provider_tokens_total` sums the input tokens the provider
  reported (`usage.prompt_tokens` for OpenAI-compatible APIs, Cohere's
  `billed_units.input_tokens`, Ollama's `prompt_eval_count`). Both count
  documents embedded by the worker and queries embedded for a search, so a
  delta across a load is the number a metered provider's invoice is made of
  — per node, per run, without the provider's dashboard.

## 0.16.2 - 2026-08-28

A patch: no wire, storage-format or API change; rolling upgrade. Two
cluster defects found the same day by driving bulk writes through a
per-request load balancer — one of them data loss — and one build change.
Operators of a multi-member cluster should take this one.

### Fixed

- **A replayed `DropCollection` no longer empties a recreated collection.**
  A collection recreated under the same name derives the same id, and peers
  re-deliver overlapping ranges as a matter of course — so a drop from the
  previous incarnation could arrive again after the recreation, resolve to
  the current collection, and delete its documents, indexes and vectors on
  that member, with replication lag still reading 0. Seen on a three-member
  cluster: one member left with 211 of 361 documents and every member
  missing vectors. A replicated drop is now applied only to the incarnation
  it was aimed at — ignored if it predates the create that produced the
  current one, or is at or before the drop that create followed. A
  replicated create now records the entry's own stamp as `created` rather
  than the applying node's clock, which is what makes that comparison safe.
- **Storage commits no longer stall the async runtime.** Every write
  transaction — the wait for redb's single writer lock and the fsync at
  commit — ran inline on a tokio worker thread. A few concurrent writers
  (bulk inserts spread across members through a load balancer, plus
  anti-entropy applying peers' batches, plus the embedding worker) pinned
  every worker: peers' TLS handshakes timed out after 5 s, `/metrics` hung
  for 10 s, SWIM marked the member down and back up, and per-collection
  ownership flapped with it. `Engine::begin_write` and the commit now yield
  the worker (`block_in_place` on a multi-thread runtime; no change off one),
  and `/metrics` gains `kimmy_runtime_stall_seconds` — the worst delay a
  250 ms timer on the runtime saw since the last scrape — so a blocked
  worker is a number before it is a handshake timeout.

### Removed

- **Intel Mac builds.** Releases no longer ship `x86_64-apple-darwin`
  binaries, and the Homebrew formula installs on Apple Silicon only. Linux
  x86_64 and aarch64 (static musl) and macOS aarch64 remain. It was the
  slowest build in every release for a platform no longer sold.

## 0.16.1 - 2026-08-28

A patch: no wire, storage or API change. Both fixes are in the embedding
worker and were found by loading 361 documents into a three-member cluster in
one batch. Rolling member-at-a-time is fine, and there is nothing to migrate.
One behaviour an operator may notice: a write made on a member that does not
own the collection now gets its vectors after one replication round instead
of immediately, because the owner embeds it rather than the writer.

### Fixed

- **Bulk ingest no longer embeds every document twice.** The node a client
  wrote to embedded each document immediately while the collection's
  rendezvous owner, whose deferral grace expired on everything the writer had
  not reached yet, embedded it all again — 570 provider calls for 361
  documents inserted in one batch. Ownership now decides on the streaming
  path too: the owner embeds every write it sees the moment its stream
  delivers it, and a non-owner embeds nothing on the stream, including its
  own writes. A non-owner's deferral is re-armed while the owner is alive and
  taken over only when ownership has moved, so an owner leaving mid-backlog
  is still covered (ADR-077, amended). A write made on a non-owner gets its
  vectors after one replication round instead of immediately.
- **The embedding provider reuses one HTTP client.** Every `embed` call built
  a new `reqwest::Client`, so each document cost a fresh DNS lookup and TCP +
  TLS handshake, and a drain of deferred documents issued those in a burst —
  seen on a three-member cluster as 31 `error sending request` failures in
  seven seconds against a provider that answered every sequential probe. The
  client is now built once per provider with connect (10 s) and request
  (60 s) timeouts, a 30 s pool idle limit and TCP keepalive; a transport
  failure is retried once inside the call before the worker's own 5 s retry.
- **Transport failures say what failed.** The error carries its kind —
  `connect`, `timeout`, `reset`, `other` — and the full cause chain rather
  than reqwest's outer message, and `/metrics` gains
  `kimmy_embed_provider_errors_total{kind}` alongside
  `kimmy_embed_failures_total`.

## 0.16.0 - 2026-08-28

The first release under Titus AI LLC and the first to declare a licence. A
minor rather than a patch for that reason — nothing technical changed: the
protocol, the storage format (schema 3) and the client APIs are those of
0.15.1, members of the two versions replicate to each other, and the upgrade
is an ordinary rolling one.

### Changed

- **Licensing.** KimmyDB is now developed by Titus AI LLC. The server
  (`kimmyd`, the crates it is built from, and the `kimmy` CLI) is licensed
  under the GNU AGPL-3.0; the client libraries (`kimmy-client`, `clients/go`,
  `clients/python`) are Apache-2.0; commercial licenses for the server are
  available from <licensing@titusai.io>. [LICENSING.md](LICENSING.md) has the
  plain-language version. Contributions now go through a [Contributor License
  Agreement](CLA.md), and security reports through [SECURITY.md](SECURITY.md).
- **`docs/handoff.md` is retired.** The running state-of-development note is
  no longer kept in the repository (#158).

## 0.15.1 - 2026-08-27

The first release of the 0.15 line that actually shipped. `v0.15.0` was
tagged, but its release build was cancelled before anything was published
— no GitHub release, no image — so everything listed under 0.15.0 below
arrives with this version. Operators upgrading from 0.14.0 should read the
0.15.0 notes: the replication wire format changed, and the upgrade is not a
rolling one.

### Changed

- **`DocRecord` no longer derives `Serialize`/`Deserialize`.** It was never
  on the wire or in a response — every use goes through the storage codec,
  which writes the body as raw bytes — and the derives were a trap: the
  first code to serialize one would have paid twelve bytes per byte, the
  defect 0.15.0 removed from `OplogEntry` and `SnapshotDoc`. Removing them
  makes that a compile error. No behaviour changes.

## 0.15.0 - 2026-08-27 (tagged, never published; see 0.15.1)

### Changed

- **A replicated document costs about its own size on the wire, not twelve
  times it** — **breaking, and not a rolling upgrade**. `OplogEntry.body` and
  `SnapshotDoc.body` are `Vec<u8>`, which serde encodes as a BSON *array of
  int32s*: one element, with its own index key, per byte. Every replicated
  document and every snapshot page was therefore inflated roughly twelvefold —
  a 1 MiB document became a 12.5 MiB entry, so a sync batch reached the 64 MiB
  frame limit at around 5 MiB of real data. Both fields are now `serde_bytes`,
  which encodes them as binary: measured at 12 520 922 bytes before and
  1 048 784 after for the same 1 MiB document, a **92% reduction** in
  replication and snapshot traffic.

  **Every node must be restarted together.** An un-upgraded peer reads binary
  where it expects an array and cannot decode the frame, so a mixed-version
  cluster does not replicate while the upgrade is in flight. Stop the cluster,
  upgrade it, start it again — the stored data is untouched, because storage
  has always written these bodies as raw bytes through its own codec, so
  nothing is migrated and nothing is at risk.

- **The MCP `list_databases` and `list_collections` tools omit KimmyDB's own
  internals** — the `__kimmy` system database and the `.__vectors` shadow
  collections — as `resources/list` has since ADR-027. A listing is an
  invitation, and an agent shown `notes.__vectors` next to `notes` opens a
  collection of float arrays that says nothing the source does not.
  `describe_collection` already reports whether a collection has vectors, and
  every tool still reaches an internal by name under the ordinary access
  check. The REST listing is unchanged (ADR-092).

## 0.14.0 - 2026-08-27

### Added

- **A `ddl` action, split out of `admin`, and it federates.** Creating and
  dropping collections, creating and dropping indexes, and configuring or
  disabling embeddings now require `ddl` rather than `admin`; `admin` still
  implies it, so no existing grant loses anything. `ddl` implies no data
  access and never reaches the system database. Unlike `admin` it maps freely
  through an identity provider — the case that forced it was an agent over
  MCP, federated through an OAuth server, told by the server's own
  instructions to `create_collection` before inserting and refused because
  the only action that allowed it was the one ADR-067 will not federate.
  The recommended agent role is `read`, `write`, `search`, `ddl` over the
  databases it owns (ADR-090).
- **Every write reports the stamp it produced.** `insert` already did;
  bulk insert now returns `stamps`, positionally parallel to `insertedIds`,
  and the filtered `update` and `delete` return `stamp` when they wrote
  exactly one document without `multi` — the version a conditional write
  (`if_stamp`, ADR-084) needs next. A multi write reports none, because one
  version cannot name several documents.

### Changed

- **Listing the collections of a database that does not exist is a 404.**
  It was `{"collections": []}`, byte-identical to a database in which the
  caller can read nothing and to an empty one, so a mistyped name looked like
  an empty result. A database that exists and hides everything from the
  caller still lists as `[]`: zero grants is not a refusal (ADR-066).
  Breaking for a client that treated the empty list as "no such database";
  it is why this is `0.14.0` rather than a patch.
- **The MCP instructions name the action each tool needs**, and the
  `hybrid_search` description says its scores are rank-fusion values that are
  not comparable with `vector_search` similarities — an agent carrying a
  similarity threshold across the two was dropping every hybrid result.

### Fixed

- **A deleted document could be returned by `vector_search` and
  `hybrid_search`.** Its chunks were removed by the embedding worker when it
  reached the `Delete` entry, not by the delete itself, so for a second or two
  on a healthy owner — and indefinitely if the worker was behind, disabled or
  another node's — the chunks were scored and returned with an `_id` that
  resolved to nothing. Both searches now check every hit against the source
  collection after ranking and drop the ones whose document is gone; a result
  can be shorter than `k` by the number of deletions the worker has not
  caught up with (ADR-091).
- **A `byo` collection's chunks were never removed when the document was
  deleted.** The "nothing to embed" bail sat above the delete branch in the
  worker, so client-supplied vectors stayed searchable until someone called
  the vectors `DELETE` route by hand.
- **An embed that finished after the document was deleted or replaced stored
  its chunks anyway.** The streaming path embedded from the oplog entry's
  image and wrote without re-reading the document; a slow provider call could
  land chunks after the `Delete` entry that should have removed them, with
  nothing left to remove them ever. The worker now re-reads the document's
  stamp after the provider returns and skips the write if it moved.

- **`openapi.yaml` says what a persistent `unknown` node status means.**
  `unknown` was described accurately but without the fact a failover-writing
  client needs: it is not `down`, and for a decommissioned node it is
  permanent until an operator deletes the registry document. Documentation
  only; no behaviour changed.
- **README states the data guarantees and the measured write speeds.** A
  "Data guarantees: ACID where, BASE where" section replaces the
  consistency-model list — per-request ACID on the accepting node, BASE
  across the cluster, the two durability classes, and the benchmark figures
  a reader needs before sizing a write workload — and the roadmap table now
  runs to M11. `compatibility.md` no longer claims that no `set_durability`
  call exists (false since ADR-088), and `benchmarks.md` no longer says the
  API offers no batching.

## 0.13.0 - 2026-08-27

### Removed

- **`kimmy login --client-credentials` and `kimmy token --client-credentials`**
  — **breaking**. The CLI is a tool for people; it no longer runs the OAuth2
  client_credentials grant, reads `KIMMY_OIDC_CLIENT_SECRET`, or uses a
  `client_secret` key in `~/.config/kimmydb/.kimmy`. A script or a service sets
  `KIMMY_TOKEN` (or the settings file's `token`) to a token minted elsewhere —
  with a self-hosted provider, a personal access token from its console,
  audienced at the node.
  A leftover `client_secret` line in the settings file is warned about and
  ignored rather than rejected, and `kimmy init` drops it on rewrite. Migration:
  replace `$(kimmy login --client-credentials)` with a personal access token
  (ADR-089).

### Fixed

- **`openapi.yaml` described `modified` wrongly.** The update response's
  `modified` counts documents *written*, which is every match, so it equals
  `matched`; the contract said it excluded unchanged documents. The other
  documents already said this correctly; the OpenAPI file — the one a client
  author is obliged to read — now agrees with them.

## 0.12.0 - 2026-08-27

### Added

- **`storage.durability`** — `durable` (every commit fsyncs before it
  returns; the default and unchanged behaviour) or `coalesced` (a commit is
  written without its own fsync and waits for the next shared one, one per
  `storage.commit_coalesce_ms` window, so N concurrent writers pay one fsync
  rather than N). Both are durable when the response returns; there is
  deliberately no class that is not. `GET /v1/version` reports the class as
  `durability`; `/metrics` gains `kimmy_fsyncs` and
  `kimmy_commits_grouped_total` (ADR-088).

- **`storage.multi_chunk_docs`** — how many documents a `multi: true` update
  or delete commits per transaction (default 1,000; 1 to 10,000). `update`
  and `delete` responses gain `commits`, the number of chunks the request
  landed (ADR-086).
- **`GET /v1/db/{db}/coll/{coll}/violations`** — the unique-index violations
  still standing on a collection: counts per index, or with `?index=<name>`
  the colliding groups with their documents. Derived from the retained
  oplog's `uniqueViolation` entries, keeping only those whose documents all
  still exist, so deleting one side resolves it. Authorised as `read`;
  `/metrics` keeps its name-free count (ADR-087).

### Changed

- **A `multi: true` update or delete commits in chunks** rather than one
  transaction, releasing the single writer between chunks, and the 10,000-
  match refusal that 0.11.0 introduced for `multi` is gone — any number of
  matches is allowed. Each chunk is still all or nothing; a failure in a
  later chunk leaves the earlier ones committed and answers with an error.
  `find_and_modify` keeps its 10,000-match ceiling, because it sorts the
  whole set before choosing.

## 0.11.0 - 2026-08-26

### Added

- **Conditional writes.** Every write now reports the version it produced
  (`stamp`, an opaque token), `find` returns versions on request
  (`"stamps": true`, a `stamps` array parallel to `documents`), and a read
  by id carries the document's version as its `ETag`. Every single-document
  write — `PUT`/`DELETE` by id (`?if_stamp=`), `update`, `delete` and
  `find_and_modify` (`if_stamp` in the body) — accepts a stamp and lands only
  if the document is still at that version; otherwise **`409 stale`**,
  `retry: no`, and nothing is written. A missing document is stale too,
  `upsert` or not. Check-then-act on one document, node-local, with no
  coordination (ADR-084). Advertised as the `conditional-writes` capability;
  `stale` joins the closed error-code set. The Rust, Python and Go clients
  gain the conditional variants and a typed `stale`, and the conformance
  suite holds all three to it.
- **A stale rejoiner is named.** A peer that comes back trailing this node
  by more than `storage.tombstone_retention_secs` may hold documents the
  cluster deleted and already collected the tombstones for, and merging it
  can resurrect them. The replication loop now logs one `WARN` the round it
  notices, and `GET /v1/topology` shows `staleSince` and `behindSecs` on that
  peer's entry until it is back within the window. Nothing is refused — the
  recommended action (reset the peer's data directory and let anti-entropy
  refill it) is the operator's call (ADR-085).
- **`chunk.max_tokens` on a collection's vector configuration.** `max_chars`
  assumes prose at about four characters per token; code, JSON and CJK run at
  one to two, so a chunk that fit the character budget could exceed the
  provider's input limit and be refused on every scan — seen live as a
  1073-token chunk cut at 2000 characters. When set, a chunk is also cut once
  its estimated token count (one token per two bytes of UTF-8, conservative
  for every common script) reaches `max_tokens`. Absent keeps the character
  rule alone. The permanent-failure `WARN` in the embedding worker now names
  the database, collection and `_id` of the document it skipped.

### Fixed

- **`update` and `delete` by filter no longer lose concurrent writes.** The
  operators ran on an image collected in a read transaction and the result
  was stored in a separate write transaction, so two concurrent `$inc`s on
  one document could both read the same value and one increment was lost —
  on a single node, against the documented per-document atomicity. Four
  writers × 500 increments left the counter at 500. Both routes now match
  and write inside one write transaction, through the same engine body
  `find_and_modify` has always used (ADR-083).
- **`find_and_modify` with a filter on `_id` no longer scans the
  collection** under the writer; it looks the document up directly, as
  `find` and `update` already did.

### Changed

- **`storage.tombstone_retention_secs` shorter than `storage.oplog_retention_secs`
  is refused at startup.** It was the one retention setting under which a
  partition shorter than the oplog window could resurrect deleted documents:
  a peer replays the delete's oplog entry after the tombstone it needs to
  lose against has been collected. Anyone who set retention backwards must
  raise tombstone retention to at least the oplog window; the defaults (24 h
  both) pass unchanged (ADR-085).
- **A `multi: true` update or delete is one transaction.** All of it lands or
  none of it does, and it costs one fsync rather than one per document. The
  writer is held for the whole request, so the request is **refused above
  10,000 matches** — the same ceiling and error as `find_and_modify` —
  where it previously ran to completion one document at a time. Narrow the
  filter or add an index; a request that failed part-way used to leave the
  earlier documents written and now leaves none.

## 0.10.2 - 2026-08-26

### Fixed

- **`kimmy_embed_documents_total`, `kimmy_embed_chunks_total` and
  `kimmy_embed_failures_total` now count the streaming path.** The common
  case on an owner — a document this node wrote, embedded from its own oplog
  entry — was never counted: since 0.5.0 those three series moved only for
  deferred re-checks and scans, so a healthy owner read zero while its
  vectors demonstrably landed, and a provider outage on the live path was
  invisible to the one signature (`failures` climbing while
  `documents_embedded` does not) the counters were added for. Seen on the
  production cluster, where an owner's `documents_embedded` read 1 from a rescan
  and never moved as inserts were embedded within seconds. Failures are
  counted per attempt, retries included, on both paths.

## 0.10.1 - 2026-08-26

### Fixed

- **The embedding worker stopped for good when its position had been
  collected.** The worker resumes from a recorded oplog position, and
  retention collects the oplog: a member down or partitioned for longer than
  `oplog_retention_secs` came back to a position the oplog no longer held,
  `watch` refused it, and the worker returned the error — one `embedding
  worker stopped` warning, then a node with **no embedding worker at all**
  until the next restart hit the same position and stopped again. A stream
  invalidated mid-run ended the worker the same way. Seen on a production
  cluster after a member spent ten hours unable to sync: it came back owning
  a collection and embedded nothing from then on. The worker now recovers:
  it opens a fresh stream from the oldest retained entry, then rescans every
  owned, server-embedded collection for stale or missing vectors, and keeps
  running. Embedding is idempotent, so the overlap costs storage reads, not
  provider calls. The scan's completion line now reads `scanned a
  collection's vectors` with a `reason` field (`a configuration change` or
  `a lost stream position`).

## 0.10.0 - 2026-08-26

### Fixed

- **A snapshot could not carry half of all collections.** The snapshot page
  and cursor named collections by a bare `u64`, and BSON has no unsigned
  64-bit integer, so any collection whose derived id sits above `i64::MAX`
  made the page unencodable: the serving member logged `cannot fit into
  BSON` and closed the connection, and the member asking — one beyond its
  peers' oplog retention horizon, the only case a snapshot serves — never
  caught up. The same defect was fixed for oplog entries in ADR-031; the
  snapshot types kept the raw integer, and the in-process snapshot tests
  never serialised a page. Both fields are now `CollectionId`, which
  encodes as reinterpreted signed bits. Found on a production cluster, where
  every pair went dark 24 h after birth once retention passed the pinned
  floors described below.

- **Anti-entropy could loop on the same window forever.** A member whose
  newest own stamp is one it never ships — a unique-violation record — left
  every peer's resume point pinned below it, and once a full batch of other
  members' entries lay between the two, each sync round re-served that batch
  unchanged: `applied=0, superseded=1021` every five seconds, and
  `kimmy_replication_lag_seconds` reading the cluster's age rather than any
  backlog. A full batch now advances the witnessed vector to the end of the
  window it delivered, for every origin the peer advertised, so each round
  consumes a window and the round that reaches the tail clears the rest
  (ADR-082). Data replication was unaffected — writes still arrived through
  the other peer — so upgrading changes what the gauge and the sync log say,
  not what the members hold. The storage-level `merged a batch from a peer`
  debug line now carries `ddl` and `unknown_collection`, matching the peer
  summary above it.

### Changed

- **Quieter auth flow.** `kimmy init` lost its preamble and closing hints —
  what remains is the question, the discovered answers, and where the file
  landed (the machine-readable line on stdout is unchanged). The device flow
  no longer narrates "Opened." after the fact: silence is confirmation, and a
  browser that failed to open still says so.

## 0.9.0 - 2026-08-26

### Added

- **Every command uses the federated token cache.** After `kimmy login`,
  `kimmy whoami`, `databases`, queries — all of it — just works until the
  token nears expiry: commands with no explicit token fall back to the cache
  login and `kimmy token` write, keyed by issuer, client and resource.
- **The device flow offers to open the browser for you.** Print URL, press
  Enter, default browser opens on the verification page (code pre-filled via
  `verification_uri_complete` when the provider sends one). Skipped entirely
  when stdin or stdout is not a terminal.

### Changed

- **`kimmy login` caches its token by design** (ADR-080): the opt-in from
  ADR-075 lasted exactly as long as nobody read the cache — which was until
  data commands started doing so today, and the documented workflow failed
  with a misleading 401. `--cache-token` is removed along with the opt-in.
- The post-401 hint says `run \`kimmy login\`` first now, instead of pointing
  at flags when the common case has none to set.

### Fixed

- **A recreated collection can no longer inherit its predecessor's documents
  through a stamp tie** (ADR-081). Collections derive their id from `(db,
  name)`, so drop-and-recreate yields the same id, and the only guard against
  a pre-drop document flowing back was a *strict* tombstone comparison — which
  ties when the peer's last write and the drop share a millisecond, as two
  engines on one machine routinely do. Recreations now record the drop's stamp
  as an **incarnation floor** and suppress replicated entries at or below it.
  Collections created without a drop behind them carry no floor and behave
  exactly as before. Surfaced as an intermittent CI failure; the window was
  reachable in production wherever a peer's final pre-drop write tied with the
  millisecond of the drop.

## 0.8.0 - 2026-08-26

### Added

- **`kimmy init` discovers instead of interrogating.** It now asks for one
  thing — the node URL — and reads the RFC 8707 resource identifier and the
  issuer from the node itself, the same document `kimmy login` already
  consumes. The client id shows its registered default (`kimmy-cli`) and Enter
  keeps it; explicit prompts return only for nodes too old to publish
  metadata. Re-runs carry existing secret keys forward rather than dropping
  them, secrets are never read interactively, and the output says what was
  written and what was deliberately left to flags and environment.

### Changed

- **Re-running `kimmy init` no longer panics — and neither does anything
  else.** With a settings file present, every subcommand without OIDC flags
  (`databases`, `ping`, `whoami`, …) aborted with `` "issuer" is not an id of
  an argument`` before doing any work: the dotfile loader asked clap for value
  sources that exist only on `login`/`token`. Those commands now skip the
  provider settings entirely.

## 0.7.0 - 2026-08-26

### Added

- **`kimmy init` and a settings file: `~/.config/kimmydb/.kimmy`.** Init
  prompts once per setting — the current value shown as the default, Enter
  keeps it — then writes the file `0600`; running it again re-prompts and
  overwrites. The file is dotenv-style and covers url, token, password,
  issuer, client_id, client_secret, resource, scope, cache_token. Precedence
  per setting is **flag > environment variable > file**, so it fills gaps and
  never overrides something already said. Unknown keys and broken lines are
  errors naming the line.
- **`kimmy roles …` and `kimmy users …`.** The administrative surface the
  federation round made necessary: stored roles (ADR-073) — the things OIDC
  role mappings point at — can now be created, inspected, granted against and
  revoked from the CLI (`roles grant/revoke` are live edits; they apply on the
  callers' next request), and local accounts can be created, granted,
  disabled and deleted without curl. Disable is new server-side too:
  `POST /v1/users/{name}/disabled` ends every session the account holds and
  refuses new logins while keeping the record — the reversible form of
  deletion, guarded like deletion is (not your own account, not the last
  enabled user).

### Changed

- **Bare `kimmy` shows the help screen** — the same long help `--help`
  prints, on stdout, exit 0. Previously it was a two-line usage error telling
  the user to run again with a flag.
- **The system database never matches a wildcard (ADR-079).** A grant of
  `{db:"*"}` no longer reaches `__kimmy` — whose `__users` collection holds
  password hashes and token versions. Wildcard-granted callers stop seeing it
  in listings and are refused on direct access; two doors remain, holding the
  `admin` action anywhere (root and every admin deployment are untouched) or a
  grant naming `__kimmy` exactly, down to its collection pattern. Anyone who
  was deliberately reading system collections through a wildcard must write
  that exact grant now.

## 0.6.0 - 2026-08-25

### Added

- **`kimmy whoami`.** How the node sees the caller: principal name, local or
  federated, and the grants that identity holds — the one-command answer to
  "I logged in fine, why does everything refuse?"
- **A zero-grant note on empty listings.** `kimmy databases` and
  `kimmy collections` filter through authorization server-side, so an identity
  whose token carries no grants sees what an empty cluster looks like. When a
  listing comes back empty and `/v1/auth/whoami` confirms the caller holds no
  grants at all, the CLI says so on stderr; stdout is unchanged.
- **`KIMMY_OIDC_ROLE_MAPPINGS`: role mappings through an environment
  variable.** A container deployment that configures the node with env vars
  could federate but had no way to say what a federated identity was worth —
  `role_mappings` was TOML-only, so such nodes ran with zero mappings and
  every federated caller held zero grants (empty listings, bare 403s). The
  variable takes one JSON array of mapping objects and **replaces** the file's
  list when set; every startup refusal applies unchanged (ADR-078).
- **`kimmy token`.** Prints the caller's access token again: the cached one
  while it stays fresh, otherwise one federated flow whose result is kept, so
  `$(kimmy token)` costs nothing after the first call until the token nears
  expiry. Caching is implied — invoking the command *is* the asking ADR-075
  requires — and what is stored does not change: access token only, `0600`,
  never a refresh token. Federated flows only; `kimmy login <user>` remains
  how local accounts print a token.

### Changed

- **`kimmy login` federates by default.** With no arguments it runs the device
  flow against the node's identity provider; naming a user (`kimmy login ada`)
  logs into that local account as before. Previously bare `kimmy login`
  refused with "name the user to log in as", so nothing scripted depended on
  the old behaviour — but the flag spellings all still work, including
  `--oidc`, which now only spells the default out.

## 0.5.0 - 2026-08-25

### Added

- **Embedding work is owned per collection.** Backfill scans and deferred
  re-checks run on the node the collection's rendezvous hash assigns — the
  same function webhooks and TTL expiry use — instead of on every member.
  Closes the 3x provider-call amplification measured under replication lag
  (ADR-077).
- **The embedding worker can be turned off per node**: `[vector]
  worker_enabled = false` or the one-way `--disable-vector-worker` flag /
  `KIMMY_DISABLE_VECTOR_WORKER`. A disabled node consumes replicated vectors;
  vector search is unaffected.
- **Embedding observability**: `kimmy_embed_documents_total`,
  `kimmy_embed_chunks_total`, `kimmy_embed_deferred_total`,
  `kimmy_embed_skipped_not_owned_total` and `kimmy_embed_failures_total` on
  `/metrics`.

### Changed

- `/metrics` gained five series; scrapers asserting an exact series set need
  the additions.

## 0.4.0 - 2026-08-24

### Added

- **Roles are first-class stored objects.** A role is one named set of grants
  that several principals point at, managed at `/v1/roles` and assigned with
  `POST /v1/users/{name}/roles`. Both halves of the system can now reach the
  same definition: a local user names a role on its record, and an
  `[[auth.oidc.role_mappings]]` entry can name one with `role = "analyst"`
  instead of repeating its grants inline. Requires `admin` over `*`, the same
  bar as managing users.

  **Role grants are added to a principal's direct grants, never a replacement** —
  effective permission is the union of the two, so a user holding no roles is
  completely unaffected. No storage migration and no on-disk schema bump: roles
  live in an ordinary system collection created on demand, and user records
  written before this decode as holding no roles. See ADR-073.

- **`auth.oidc.allow_federated_admin`**, default `false`. With it off — the
  behaviour that shipped in 0.2.0 — a federated principal can never hold
  `admin`. It exists because that absolute refusal makes a large deployment
  impossible rather than awkward: auditors flag the privileged local accounts
  outside the IdP that the rule requires. Turning it on is announced in the
  startup summary every time. See ADR-074.

### Changed

- **Editing or deleting a role revokes the live tokens of every local user
  holding it**, and the response reports how many accounts that was. Without it
  a *narrowing* edit would take effect only as each token expired. Federated
  principals need no such revocation and are not counted: their grants resolve
  from the role store on every request, so an edit reaches them on their next
  call. Their role *membership* is a different matter — it is frozen in the
  provider's access token until that token expires.

- **The audit record carries the roles a principal held.** The roles held, not
  "the role that decided": grants are a union and more than one role can supply
  the same permission. It matters most for a federated caller, where there is no
  local record to recover the association from afterwards.

- **A role mapping naming neither `role` nor `grants` now stops the node at
  startup.** It could never have granted anything, so it was a typo — and the
  failure it produced instead was a caller who authenticated and was then
  authorized for nothing, with no indication that the configuration was at
  fault.

## 0.3.0 - 2026-08-23

### Added

- **KimmyDB names itself as an OAuth 2.0 protected resource.** When
  `auth.oidc.audience` is written as an `https` URL, a node publishes RFC 9728
  metadata at `/.well-known/oauth-protected-resource` naming that identifier
  and its authorization server. `kimmy login --oidc --url <node>` now needs
  nothing else configured — it reads both values off the node — and a
  conformant MCP client can discover where to authenticate the same way.
- **`kimmy login` sends an RFC 8707 `resource` parameter**, on the device flow
  and the client-credentials flow, via `--resource` / `KIMMY_OIDC_RESOURCE` or
  the node's own metadata. Without it the only audience the CLI could obtain
  was whatever the provider defaulted to, so an audience naming this node
  specifically was unreachable from the tool.
- **`WWW-Authenticate` on every 401 and 403** (RFC 6750 §3), pointing at the
  metadata document when there is one. A request that offered no credentials is
  told how to authenticate and deliberately carries no `error` code; a bad
  token gets `invalid_token`, and a denied request `insufficient_scope`.
  `POST /v1/auth/login` is exempt — it is where a token comes from, not a
  bearer-protected resource.
- `kimmyd check-config` and the startup log now say whether the node publishes
  protected resource metadata, and why not when it does not.
- **`auth.oidc.require_at_jwt`** (`KIMMY_OIDC_REQUIRE_AT_JWT`), default `false`:
  refuse a federated token whose `typ` header is not `at+jwt` (RFC 9068 §4).
  Off by default because providers disagree about stamping it — Entra ID sends
  `typ: JWT` on v2 access tokens — so a strict default would refuse every token
  from a supported provider. Turn it on when yours is known to emit it. If
  `auth.oidc.audience` is an `https` URL you are already covered without it: an
  ID token's audience is a client id and can never be a resource identifier.
- **`kimmy login --cache-token`** (or `KIMMY_TOKEN_CACHE`) reuses the access
  token from a previous login instead of authenticating again. **Off unless
  asked for**, so nothing changes for anyone who does not pass it. The token
  goes in a `0600` file under `$XDG_CACHE_HOME/kimmy` (or `~/.cache/kimmy`),
  keyed by issuer, client and resource, and is reused until it is within a
  minute of expiring. **A refresh token is never requested and never stored**,
  with or without the flag. See [ADR-075](docs/decisions.md).

### Changed

- **`auth.oidc.audience` written as `http://` is now refused at startup**, as
  is one carrying a fragment. Every other audience is accepted exactly as
  before, including opaque strings, Entra ID's `api://<guid>` and `urn:`
  values — those simply publish no metadata. No existing configuration that
  used `https` or an opaque string needs editing. See
  [ADR-071](docs/decisions.md).
- **`kimmy login --client-credentials` now authenticates with HTTP Basic** when
  the provider advertises `client_secret_basic`, falling back to the request
  body only when it advertises the body and not Basic. RFC 6749 §2.3.1 makes
  Basic mandatory for an authorization server and the body optional, so this is
  the method that is always available. The id and secret are form-encoded
  before the header is built, as §2.3.1 requires — which matters whenever a
  secret contains a `:`, a `+`, a space or a non-ASCII character.
- **`kimmy login --client-credentials` no longer requests any scope by
  default.** `--scope` previously defaulted to `openid profile` for both flows,
  but there is no end user in the client-credentials grant, so `openid` asks
  for an ID token that cannot be issued — some providers ignore it, others
  refuse the request. The device flow still defaults to `openid profile`, and
  an explicit `--scope` still wins for either. **If you relied on the old
  default for a service account, pass `--scope` explicitly.**
- **Documentation only, no behaviour change:** [Security](docs/security.md) now
  states where authorization stops and why. The collection is the finest unit
  of protection — no document- or field-level security, no ABAC, no embedded
  policy engine — and **named roles will not change that ceiling** when they
  arrive. It also explains that a trailing `*` on a grant's `db` matches a
  prefix, so `sales*` covers `salesforce` and any database created later with
  that prefix, and records why `/metrics` keeps its unauthenticated place on
  the main listener rather than gaining a second port. See
  [ADR-076](docs/decisions.md).

### Fixed

- **`/mcp` now answers a rejected request with `WWW-Authenticate`, and its
  traffic reaches `/metrics` and tracing.** It was merged onto the router
  *after* the layer that counts, times, traces and adds the challenge, and a
  router merged after a layer keeps its own empty middleware stack — so every
  MCP request skipped all four. The header is the half that matters: an MCP
  client holding no credentials has no other way to discover its authorization
  server, which is the case RFC 9728 exists to serve, so a bare 401 left it
  needing to be configured by hand. Nothing about MCP authorization itself
  changed — a request without a valid token was refused before and is refused
  now — and REST routes were never affected.

### Security

- **`kimmyd check-config` no longer prints `auth.jwt_secret` or
  `auth.root_password`.** It dumped the whole configuration, and those two
  fields with it, to a terminal — and to CI output, and to anything pasted into
  a bug report. `jwt_secret` signs every local token the cluster issues, so
  reading it is enough to mint any principal, `root` included. Both now
  serialize as `<redacted>`, and the documented workflow was the one that leaked:
  the config file keeps both commented out in favour of `KIMMY_JWT_SECRET` and
  `KIMMY_ROOT_PASSWORD`, exactly so the secret lives only in the environment.
  Whether each is *set* is still shown, which is the question `check-config`
  exists to answer. **Rotate `auth.jwt_secret` if its value has been through a
  shared log or a pasted report**; rotating it invalidates every local token in
  issue, which is the intended effect.
- **A federated token's `nbf` is now validated.** `jsonwebtoken` leaves that
  check off by default, so a token stamped as not valid until a future time was
  accepted before it was due (RFC 7519 §4.1.5). The same 60-second leeway `exp`
  gets applies, and a token carrying no `nbf` is unaffected — the claim stays
  optional.
- **A provider's discovery document must now name the issuer it was fetched
  for**, and the `jwks_uri` it names must be `https` (OpenID Connect Discovery
  §4.3, RFC 8414 §3.3). Checked in both places that read one: the node's key
  refresher and `kimmy login`. Without it, anything able to answer for the
  well-known path chose which signing keys a node trusts — or, on the CLI,
  where a client secret is sent. Plain `http` to a loopback address stays
  allowed, so a locally-run provider still works. **An operator whose provider
  publishes an issuer that differs from the configured one — a trailing slash
  is the usual case — will now see a startup failure naming both values.** See
  [ADR-072](docs/decisions.md).

## 0.2.0 - 2026-08-23

### Added

- **Distributed tracing and OTLP metrics.** A node can now export spans and
  counters to an OpenTelemetry collector. Configure it under `[telemetry]` (or
  `KIMMY_OTLP_ENDPOINT`); there is no enable flag, because setting an endpoint
  is what turns it on. Spans cover the HTTP request, the executor operation —
  so REST and MCP produce the same ones — the storage commit that is the fsync,
  anti-entropy rounds, the embedding worker, webhook deliveries and the OIDC
  key refresh. Inbound `traceparent` is honoured and outbound webhook
  deliveries carry one. See [docs/operations.md](docs/operations.md) and
  ADR-068 through ADR-070.
- `--otlp-endpoint`, `--otlp-protocol`, `--otlp-sample-ratio`,
  `--otlp-service-name` and `--telemetry-include-names`, each with a `KIMMY_*`
  environment variable.
- The process counters behind `/metrics` are also reported over OTLP, as
  observable instruments reading the same atomics — bridged, not duplicated, so
  the two surfaces cannot disagree (ADR-070). **`/metrics` itself is
  unchanged**, and is now pinned by a golden test over the whole render plus an
  ordered series-name assertion over the route's body.
- **Enterprise OIDC federation.** A node can now accept tokens from one
  external OpenID Connect provider alongside its own local users. Configure it
  under `[auth.oidc]` (or `KIMMY_OIDC_ISSUER` / `KIMMY_OIDC_AUDIENCE` /
  `KIMMY_OIDC_ROLES_CLAIM`), map claim values to grants with
  `[[auth.oidc.role_mappings]]`, and every route, MCP tool and audit record
  works for a federated caller exactly as it does for a local one.
  RS256/ES256 against the provider's JWKS, with issuer, audience and expiry
  validation and 60 seconds of clock-skew allowance. See
  [docs/security.md](docs/security.md) and ADR-064.
- `kimmy login --oidc` — RFC 8628 device authorization, the flow `gh auth
  login` uses. The code and URL go to stderr and the bare token to stdout, so
  `export KIMMY_TOKEN=$(kimmy login --oidc)` works as it always has. Nothing
  is written to disk, and no refresh token is ever requested or kept.
- `kimmy login --client-credentials` — for a service account. The client
  secret comes from `KIMMY_OIDC_CLIENT_SECRET`; there is deliberately no flag
  for it, for the same reason there is no `--password`.
- `Builder::token_provider` on the Rust client: an async callback that supplies
  a fresh token at connect time, before expiry, and once after a 401. This is
  how a long-lived application plugs in its own OIDC refresh — the client
  library deliberately does not implement OAuth2.
- `kimmy_jwks_refresh_total{outcome}` on `/metrics`. Worth an alert: a node
  that has stopped reaching its provider keeps verifying perfectly until the
  provider rotates its keys, and then refuses every federated caller at once.
- `kimmyd check-config` now performs a live discovery and JWKS fetch when
  `[auth.oidc]` is configured, and fails if the provider cannot be reached.

### Security

- **Telemetry omits names by default.** With `telemetry.include_names = false`
  — the default — a span is named for its route template
  (`/v1/db/{db}/coll/{coll}/docs`) or its operation (`find`, `insert`), neither
  of which is built from anything you stored. Turning it on adds
  `db.namespace`, `db.collection.name` and `url.path`, which publishes your
  schema to whatever holds the traces. **Only spans are exported, never log
  events**, and audit records never reach a collector at any setting. See
  [docs/security.md](docs/security.md) and ADR-068.
- **OTLP over HTTP only, never gRPC**, and `https://` collector endpoints are
  refused at startup rather than failing silently at every export. This keeps
  the build free of a second native dependency stack, so the musl and arm64
  cross-compiles are unchanged (ADR-069). A collector reachable only over gRPC
  or TLS needs an OpenTelemetry Collector in front of it.
- **`admin` cannot be granted through an IdP claim.** A role mapping naming the
  `admin` action stops the node at startup. Administration stays reachable only
  through a local account, so a misconfigured or compromised identity provider
  cannot mint a superuser over the database (ADR-067).
- Federation is refused in combination with `--insecure-no-auth`, where the
  role mappings would enforce nothing while appearing to.
- A non-`https` `auth.oidc.issuer` is refused: the signing keys are fetched
  from that URL, and over plaintext they can be substituted.

### Changed

- `GET /v1/auth/whoami` gained a `federated` boolean. A name cannot answer the
  question — an identity provider is free to assert a subject matching a local
  account — and a federated principal has no local record, so it cannot change
  a password or be revoked from here.
- Audit records carry `federated` alongside `unauthenticated`, so the three
  origins of a principal stay distinguishable in the log.
- The `kimmy` CLI's 401 hint is now issuer-aware: with `KIMMY_OIDC_ISSUER` set
  it points at `kimmy login --oidc` rather than at a local password login the
  caller may not have.
- `POST /v1/auth/refresh` refuses a federated principal with a 400 explaining
  that the identity provider renews that token. Previously it would have
  failed with a misleading "this token is no longer valid".
- The container image is tagged `latest` only for a final release. Previously
  a prerelease tag would have taken over `:latest` if prerelease publishing
  were ever enabled.

### Notes for operators

- **Federated sessions cannot be revoked from KimmyDB.** There is no local
  record to carry a token version, so the session ends when the provider's
  token expires. Keep federated token lifetimes short (ADR-065).
- Role mappings are configuration: changing one is an edit and a restart, and
  in a cluster a rolling one (ADR-066).
- Startup does **not** wait for the identity provider. A briefly unreachable
  provider must not stop a database from restarting, so the key fetch retries
  in the background and local users keep working meanwhile.

## 0.1.0 - 2026-08-23

The first tagged release. Everything below already works and is exercised by
tests and by driving real nodes — see the status table in
[docs/README.md](docs/README.md).

### Added

- JSON document storage on redb: multi-database, Mongo-style queries
  (17 filter operators), update operators, sort, projection, cursor paging,
  and an aggregation pipeline with a hard memory ceiling.
- Change streams over WebSocket on a single node — resumable by token, no
  replica set required.
- Leaderless clustering: SWIM membership over UDP, oplog anti-entropy over
  TCP, DNS/Kubernetes discovery, snapshot resync.
- Secondary indexes: compound, descending, multikey, unique (single-node),
  TTL and partial.
- Vector search and automatic embeddings: per-collection providers, HNSW
  above 500 vectors, hybrid search fused by reciprocal rank fusion.
- An MCP server inside the database at `/mcp`, sharing authorization with
  REST.
- Authentication and RBAC: Argon2id, JWT with sliding refresh, per-collection
  grants, login rate limiting, audit log.
- TLS termination for HTTP/WebSocket/MCP, with hot certificate reload.
- Webhooks with signed deliveries and cluster failover.
- Online backup, offline restore, and point-in-time rewind.
- The `kimmy` CLI, first-party Rust/Python/Go clients, and a conformance
  suite that drives all three against a real server.
- Build identity baked into every artifact: `kimmyd` logs version and commit
  at startup, `kimmy --version` and `GET /v1/version` report the same values,
  and a tarball build without `.git` still compiles (commit `unknown`).
- Release engineering: tag-driven releases via cargo-dist — static musl
  Linux binaries (x86_64, arm64), macOS binaries (x86_64, arm64), SHA256
  checksums, a Homebrew formula for `kimmy`, and a multi-arch container
  image at `ghcr.io/titusai-io/kimmydb`.
