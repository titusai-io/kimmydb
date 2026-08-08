# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-07 — M4 started; collection identity fixed first

**Branch:** `m4-deterministic-collection-ids` (ready to merge)
**Gate:** 525 tests · `cargo fmt --all -- --check` clean · `cargo clippy
--workspace --all-targets -- -D warnings` clean · migration driven against two
real pre-existing databases

### What this branch did

Starting M4's transport turned up a blocker underneath it. **Collection ids came
from a node-local counter**, and every oplog entry names its collection by id —
so two nodes that created the same collections in a different order disagreed
about what an entry referred to, and a replicated write would have landed in the
wrong collection. Confirmed with a throwaway two-engine probe before designing
around it.

Ids are now derived: `FNV-1a-64(db || 0x00 || name)`. Every node computes the
same answer with no coordination, which is the leaderless property the design
rests on. The maintainer chose this over replicating metadata or carrying `(db, name)` in
every entry. [ADR-031](decisions.md).

- **Schema version 2**, separate from the record `codec::FORMAT_VERSION` — a
  record whose bytes still decode does not need rewriting because the meaning of
  a key changed around it.
- **Migration, not refusal.** Schema 1 databases are renumbered on open:
  document keys, index entries, and the collection field of every oplog entry.
  Idempotent. A *newer* schema is still refused.
- **Collision checked** at creation and at migration, since two collections
  sharing storage is unrecoverable.

**One consequence, unavoidable:** dropping and recreating a collection now
reuses its id. "Same name means same id everywhere" and "recreating yields a
fresh id" are contradictory. That makes purging on drop load-bearing rather than
tidy; `drop_collection` already purged, and a test now pins that instead of the
id-uniqueness property it replaced.

### Next: the rest of M4

In order:

1. **Version vectors.** Nothing computes `{node → max_hlc}` yet. The roadmap's
   "missing: the transport" understated what is left. Needs a table maintained
   in `append_oplog`, plus "what do I need from you" comparison.
2. **Anti-entropy** — request an oplog range from a peer, apply with
   `apply_remote`, which now maintains indexes and reports violations.
3. **The transport** — `foca` membership over UDP, TCP for oversized payloads.

I would do 1 and 2 first, transport-free and tested between two in-process
engines, then wire the network. That is the order that keeps the correctness
work separable from the networking.

### Worth knowing before continuing

- **Exclude `OpKind::UniqueViolation` from anti-entropy.** These are a node's own
  observation; replicating them reports one violation once per node.
- **Anti-entropy uses `read_oplog_from`** (stamp order); change streams use
  `read_arrival_from` (arrival order). Different questions — do not merge them.
- **`apply_remote` is not `exec`.** It serves no principal, so it does not go
  through the authorization path. Keep that boundary.
- **`oplog_retention_secs` bounds peer catch-up.** A peer further behind than
  the window needs a full resync, which does not exist.
- **Retention never collects the newest oplog entry** — the clock resumes from
  it ([ADR-028](decisions.md)).

### Open, none blocking

Implementation only: `$in` not using an index; descending-field ranges;
one-bound index ranges; HNSW snapshot persistence; the aggregation pipeline that
would give MCP its `aggregate` tool.

Two questions M4 will raise but has not yet: whether `drop_collection` should
replicate as a tombstone, and how to handle a node whose tombstones were
collected while it was partitioned.

## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
