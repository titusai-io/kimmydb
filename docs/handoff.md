# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-07 — M4: version vectors and anti-entropy built

**Branch:** `m4-version-vectors` (ready to merge)
**Gate:** 546 tests · `cargo fmt --all -- --check` clean · `cargo clippy
--workspace --all-targets -- -D warnings` clean

### What this branch did

The replication *core*, deliberately transport-free — convergence is a property
of the merge rules, not of the network, and mixing the two makes failures
ambiguous.

- **`VersionVector`** in `kimmy-core`: `{node → max_hlc}`, with `behind()`
  answering "where must a peer start sending".
- **`oplog_versions`** table, maintained on every append and rebuilt on open if
  it disagrees with the log — derived state, like the arrival index.
- **`entries_for_peer(from, limit)`** — the range to send, excluding
  `OpKind::UniqueViolation`.
- **`apply_batch(entries)`** — merges through `apply_remote`, returning a
  `SyncOutcome` of applied / superseded / unknown-collection.

Tested between engines in one process: two-node convergence, three-node
transitive convergence through a middle peer, conflicting writes agreeing on a
winner, deletes replicating, and a stable second round that transfers nothing.

**A mutation check changed a test rather than the code.** Flipping `min` to
`max` in `behind()` was caught by the unit tests but *not* by any convergence
test — none of them built the case that distinguishes the two. That case now
has its own test, verified to fail under the mutant. Written up in
[Testing](testing.md), because the first version of that test was commented as
verified before it had been.

### Next: DDL replication — needs a decision

**This is the blocker.** A `Collection` oplog entry carries **no payload**: no
name, and no indication of create versus drop. Index creation and vector
configuration are **not logged at all**. So:

- A peer cannot create the collection a replicated entry refers to.
- Indexes and vector settings never propagate.

`SyncOutcome::unknown_collection` counts what had to be skipped, so the gap is
visible rather than looking like convergence — but it is a gap.

The roadmap already flags one half of the question ("whether `drop_collection`
should replicate as a tombstone, since a partitioned peer could otherwise
resurrect a dropped collection"). The other half — what a DDL entry carries and
whether index/vector DDL is logged at all — is undecided.

### Then

- **The transport** — `foca` membership over UDP, TCP for oversized payloads.
- **Full resync** for a peer further behind than `oplog_retention_secs`.

### Worth knowing

- **Anti-entropy uses `read_oplog_from`** (stamp order); change streams use
  `read_arrival_from` (arrival order). Different questions.
- **`apply_remote` is not `exec`.** It serves no principal, so it does not go
  through the authorization path.
- **Retention never collects the newest oplog entry** ([ADR-028](decisions.md)).
- **Collection ids are derived from names** ([ADR-031](decisions.md)), which is
  what makes a replicated entry address the same collection everywhere.

### Open, none blocking

`$in` not using an index; descending-field ranges; one-bound index ranges; HNSW
snapshot persistence; the aggregation pipeline that would give MCP `aggregate`.

## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
