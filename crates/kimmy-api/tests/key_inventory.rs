//! The inventory of every place a key's bytes live (Decimal128 design 5.1, test plan item 10,
//! v2.4 (f); build review M4).
//!
//! Schema 5 changed the bytes of every number key. A table, a call site or a persisted value
//! that holds key bytes and that nobody classified is the hazard of the A2 pair: a key written
//! in one form and read as the other names **another document** (the schema-5 key of
//! `0x5000008101800000` is the schema-4 key of `0x50007F8081018000`). Each rule below is
//! closed-world: something new fails until it is classified here, and an entry that names
//! nothing any more fails too, so the manifest cannot drift into a list nobody reads.
//!
//! 1. **Tables** ([`TABLES`]). Every `TableDefinition::new("…")` in `kimmy-storage`'s production
//!    code is listed, classed [`Class::ReKeyed`], [`Class::Unchanged`] or
//!    [`Class::LegacyAsReceived`], with the reason. A table named by anything but a literal fails,
//!    because it cannot be listed.
//! 2. **Call sites** ([`CALL_SITES`]). Every production function, in every crate, that makes,
//!    converts or carries a key (`keyenc::`, `encode_compound`, `doc_key(`, `legacy_key(`,
//!    `legacy_of_stored(`, `stored_of_legacy(`, `LegacyKey`) is listed by `(file, function)`
//!    with what the key is for. The encoder's own modules (`kimmy-core/src/keyenc*`,
//!    `legacy_key.rs`) are what is being called and are not listed. This is also the cross-crate
//!    guard on the legacy form: `LegacyKey` has no constructor from raw bytes, and a new caller
//!    of either conversion anywhere fails here.
//! 3. **Persisted values that can hold key bytes.**
//!    - [`PERSISTERS`]: every `kimmy-storage` function that reads `DOCS`, `INDEX_ENTRIES` or the
//!      migration's staging table and, in a write transaction, names another table, with what
//!      reaches that table (a cursor, holder keys, or nothing but stamps and counts).
//!    - [`SLOT_WRITERS`]: every `kimmy-storage` function that writes a slot of a name-keyed
//!      table (`META`, the two marks' tables, `VECTOR_VERIFIED`), with the slots it writes;
//!    - [`SLOTS`]: those slots, every `META_*` constant of `kimmy-storage`, and the rows design 5.1 names
//!      (`LIVE_DIGESTS_THROUGH["walk"]`, `SNAPSHOT_PROGRESS`, `META gc:tombstone_horizon`,
//!      `META ddl_refused:*`, the in-memory `Converge` cursor, `Engine::answered_absent`, the
//!      vector shadow collections), each anchored to the source line that makes it, which
//!      must still exist.
//!
//! **Why it is here.** It reads every crate's source from the repository root, which
//! `documentation_tests_live_in_this_crate` (docs.rs) allows only in this crate.
//!
//! **What it cannot see.** It reads source text. A function is attributed by the nearest
//! `fn` that encloses the line, as rustfmt lays it out (a nested `fn` belongs to the one it is
//! in). A key encoder brought in by a bare name (`use …::keyenc::encode; encode(…)`) is seen
//! at the `use` line, in the file's items, and not at the call. A value assembled in one
//! function and written to a table by another escapes rule 3's first part; the table's own
//! class in rule 1 is the statement for it.

mod source;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// What a table, or a slot of one, holds of a key, and so what schema 5 does to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Class {
    /// Holds document or index keys, which schema 5 rewrites: re-keyed or rebuilt by the
    /// migration, or written in the stored (schema-5) form from the start by it.
    ReKeyed,
    /// Holds no key bytes, or only bytes whose encoding schema 5 does not change (stamps,
    /// node ids, collection ids, names, counts, digests taken over the legacy key), or a
    /// stored key read back only by the build that wrote it.
    Unchanged,
    /// Holds a key in the legacy (wire) form exactly as a peer sent it, kept unconverted and
    /// sent back; only the sender converts it.
    LegacyAsReceived,
}

use Class::{LegacyAsReceived, ReKeyed, Unchanged};

struct Table {
    name: &'static str,
    class: Class,
    why: &'static str,
}

/// Rule 1: every table of the store.
const TABLES: &[Table] = &[
    Table {
        name: "meta",
        class: Unchanged,
        why: "named slots, each classed in SLOTS; none holds a document key",
    },
    Table { name: "databases", class: Unchanged, why: "database names" },
    Table {
        name: "collections",
        class: Unchanged,
        why: "CollectionMeta: names, index definitions, stamps; no key bytes",
    },
    Table {
        name: "docs",
        class: ReKeyed,
        why: "(collection id, document key): every number _id re-keyed per collection-id range",
    },
    Table { name: "live_counts", class: Unchanged, why: "collection id -> live count" },
    Table {
        name: "live_counts_through",
        class: Unchanged,
        why: "the counts' mark: an arrival position and an oplog key (a stamp)",
    },
    Table {
        name: "live_digests",
        class: Unchanged,
        why: "digest rows: XOR of xxh3(legacy key || stamp), so a 0.50 row is a 0.51 row",
    },
    Table {
        name: "live_digests_through",
        class: Unchanged,
        why: "the digests' mark (a stamp) and the open walk's cursor (SLOTS: walk)",
    },
    Table {
        name: "index_entries",
        class: ReKeyed,
        why: "(collection, index, value key, document key): every index drained and rebuilt",
    },
    Table { name: "oplog", class: Unchanged, why: "stamp -> entry; ids by the codec's DocId tags" },
    Table { name: "oplog_arrival", class: Unchanged, why: "arrival position -> stamp" },
    Table { name: "oplog_arrival_seq", class: Unchanged, why: "stamp -> arrival position" },
    Table { name: "collections_dropped", class: Unchanged, why: "collection id -> stamp" },
    Table { name: "indexes_dropped", class: Unchanged, why: "(collection, index id) -> stamp" },
    Table { name: "oplog_versions", class: Unchanged, why: "node id -> stamp" },
    Table { name: "oplog_witnessed", class: Unchanged, why: "node id -> stamp" },
    Table { name: "oplog_held", class: Unchanged, why: "stamps held as state" },
    Table {
        name: "unique_violations",
        class: Unchanged,
        why: "stamp -> detail naming documents by DocId (serde), not by key",
    },
    Table {
        name: "vector_verified",
        class: Unchanged,
        why: "the verified record (I_EPOCH, schema, vectors); no key bytes",
    },
    Table {
        name: "snapshot_progress",
        class: LegacyAsReceived,
        why: "peer -> the pull's cursor as the peer sent it; sent back unchanged (SLOTS)",
    },
    Table { name: "oplog_collected", class: Unchanged, why: "node id -> collected-through stamp" },
    Table {
        name: "schema5_docs_rekeyed",
        class: ReKeyed,
        why: "the migration's staging: (collection id, schema-5 key) -> record, between phases",
    },
    Table {
        name: "schema5_docs_phases",
        class: Unchanged,
        why: "collection id -> phase and row counts; no cursor (the drained rows are gone)",
    },
    Table {
        name: "schema5_index_steps",
        class: ReKeyed,
        why: "an index's step; a refill's cursor is a schema-5 DOCS key, read by the refill only",
    },
    Table {
        name: "schema5_unique_collisions",
        class: ReKeyed,
        why: "(collection, index, value key) -> holders' schema-5 keys, as the refill found them",
    },
    Table {
        name: "schema5_carried_collisions",
        class: ReKeyed,
        why: "(collection, index, the schema-4 value key 3 -> 4 recorded) -> holders converted to \
              schema-5 keys in the first commit; the key is never compared with a schema-5 one \
              (the last commit settles a row by its holders) and the last commit deletes the table",
    },
    Table {
        name: "partial_rebuilt_under_find",
        class: Unchanged,
        why: "the 3 -> 4 markers: (collection, index) -> (); deleted in the first 4 -> 5 commit",
    },
    Table {
        name: "idempotency",
        class: Unchanged,
        why: "(collection id, scope hash) -> record (ADR-240): stamps, counts, a find_and_modify's \
              document as stored; a multi request's cursor is a stored DOCS key, written in the \
              schema-5 form from the start (no build before schema 5 has the table), read back \
              only by the member it names as origin, and only a saving: a resume writes nothing \
              its stamp rule refuses, so a cursor in another form under-applies and never \
              doubles (modify_chunk assembles it, record_done_alone carries it)",
    },
    Table {
        name: "idempotency_by_stamp",
        class: Unchanged,
        why: "stamp -> collection id and scope hash (ADR-240); no key bytes",
    },
    Table {
        name: "idempotency_changes",
        class: Unchanged,
        why: "the node-local change log of idempotency records (ADR-240): a minted stamp -> \
              collection id and scope hash, and where the log begins; no key bytes",
    },
    Table {
        name: "partial_rebuilt_unique_violations",
        class: ReKeyed,
        why: "3 -> 4 holders in schema 4, converted into schema5_carried_collisions in the first \
              commit",
    },
];

/// What a key made or converted at a call site is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Use {
    /// A stored key, read or written in `DOCS` or `INDEX_ENTRIES` or compared inside the
    /// process; it never leaves the engine.
    Stored,
    /// An indexed value's key or a range bound over one; never leaves the engine.
    Index,
    /// A client's cursor token: legacy out, legacy or marked in (design 7.2).
    ClientToken,
    /// A key on a snapshot page or in an uncovered list: legacy on the wire (design 7.1).
    Wire,
    /// A cut-digest term over the legacy key (design 7.3).
    Digest,
    /// The schema 5 migration's own reading and writing of both forms.
    Migration,
    /// A key used in memory as an identity (a `$group` key, a join key), never stored.
    InMemory,
    /// The type's import or re-export, or a test hook compiled into the product.
    Plumbing,
}

struct Site {
    file: &'static str,
    function: &'static str,
    uses: Use,
    why: &'static str,
}

const fn site(file: &'static str, function: &'static str, uses: Use, why: &'static str) -> Site {
    Site { file, function, uses, why }
}

/// Rule 2: every production function that makes, converts or carries a key.
const CALL_SITES: &[Site] = &[
    // kimmy-api
    site(
        "kimmy-api/src/exec.rs",
        "next_cursor",
        Use::ClientToken,
        "the stored key a token is cut from",
    ),
    site("kimmy-api/src/exec.rs", "encode_key", Use::InMemory, "a $lookup hash join's key"),
    site(
        "kimmy-api/src/vectors.rs",
        "filter_ids",
        Use::Stored,
        "the filter's paging bound, stored",
    ),
    // kimmy-core
    site("kimmy-core/src/cursor.rs", "encode", Use::ClientToken, "a token issues the legacy form"),
    site("kimmy-core/src/cursor.rs", "decode", Use::ClientToken, "a token reads back to stored"),
    site("kimmy-core/src/lib.rs", "(items)", Use::Plumbing, "re-exports LegacyKey"),
    // kimmy-fuzz-harness
    site("kimmy-fuzz-harness/src/lib.rs", "key_encoding", Use::Index, "the encoder's fuzz target"),
    // kimmy-query
    site("kimmy-query/src/aggregate.rs", "group_key", Use::InMemory, "a $group identity key"),
    site("kimmy-query/src/plan.rs", "choose_primary_key", Use::Stored, "point lookups by _id"),
    site("kimmy-query/src/plan.rs", "plan_for", Use::Index, "an index plan's range bounds"),
    // kimmy-storage: the wire (design 7.1)
    site("kimmy-storage/src/snapshot.rs", "snapshot_page", Use::Wire, "cursor in, keys out"),
    site("kimmy-storage/src/snapshot.rs", "of_page", Use::Wire, "carried deletes to stored, first"),
    site(
        "kimmy-storage/src/digest_probe.rs",
        "scan_uncovered",
        Use::Wire,
        "uncovered keys, legacy",
    ),
    site("kimmy-storage/src/digest_probe.rs", "received_key", Use::Wire, "a peer's key read back"),
    site("kimmy-storage/src/digest_probe.rs", "digest_answer", Use::Wire, "legacy sets and unions"),
    site(
        "kimmy-storage/src/digest_probe.rs",
        "digest_compare",
        Use::Wire,
        "legacy sets and unions",
    ),
    site("kimmy-storage/src/digest_probe.rs", "excluded_digest", Use::Wire, "stored only to read"),
    site(
        "kimmy-storage/src/digest_probe.rs",
        "(items)",
        Use::Plumbing,
        "imports; Facet's legacy set",
    ),
    // kimmy-storage: the digest (design 7.3)
    site("kimmy-storage/src/digest_probe.rs", "walk_headers", Use::Digest, "the self-check's fold"),
    site("kimmy-storage/src/live_count.rs", "wrote", Use::Digest, "every write's fold"),
    site("kimmy-storage/src/live_walk.rs", "run", Use::Digest, "the open walk's fold"),
    site("kimmy-storage/src/live_digest.rs", "term", Use::Digest, "the term takes a LegacyKey"),
    site(
        "kimmy-storage/src/live_digest.rs",
        "legacy_term",
        Use::Digest,
        "the term of a stored key",
    ),
    site("kimmy-storage/src/live_digest.rs", "flip", Use::Digest, "folds a LegacyKey"),
    site("kimmy-storage/src/live_digest.rs", "wrote", Use::Digest, "a delta over a LegacyKey"),
    site("kimmy-storage/src/live_digest.rs", "fold_walked", Use::Digest, "a walk's LegacyKey"),
    site("kimmy-storage/src/live_digest.rs", "(items)", Use::Plumbing, "imports"),
    // kimmy-storage: stored keys that never leave the engine
    site("kimmy-storage/src/docs.rs", "doc_key", Use::Stored, "the one stored key of an id"),
    site("kimmy-storage/src/docs.rs", "get", Use::Stored, "a point read"),
    site("kimmy-storage/src/docs.rs", "get_stamped", Use::Stored, "a point read"),
    site("kimmy-storage/src/docs.rs", "document_stamp", Use::Stored, "a point read"),
    site("kimmy-storage/src/docs.rs", "check_insert", Use::Stored, "the insert's row"),
    site("kimmy-storage/src/docs.rs", "replace_in_txn", Use::Stored, "the replace's row"),
    site("kimmy-storage/src/docs.rs", "delete_in_txn", Use::Stored, "the delete's row"),
    site(
        "kimmy-storage/src/docs.rs",
        "apply_remote_in_txn",
        Use::Stored,
        "a replicated write's row",
    ),
    site("kimmy-storage/src/docs.rs", "record_stamp", Use::Stored, "a point read"),
    site(
        "kimmy-storage/src/docs.rs",
        "unless_tombstone_gone",
        Use::Stored,
        "a keyed delete's replay reads its tombstone's row (ADR-240)",
    ),
    site(
        "kimmy-storage/src/modify.rs",
        "modify_chunk",
        Use::Stored,
        "a bulk modify's own resume key",
    ),
    site("kimmy-storage/src/modify.rs", "choose", Use::Stored, "the chosen document's row"),
    site("kimmy-storage/src/modify.rs", "write_chosen", Use::Stored, "the chosen document's row"),
    site("kimmy-storage/src/rewind.rs", "rewind_to", Use::Stored, "a rewound document's row"),
    site(
        "kimmy-storage/src/snapshot.rs",
        "apply_converge_page",
        Use::Stored,
        "a held-row check by id",
    ),
    site(
        "kimmy-storage/src/snapshot.rs",
        "apply_carried_document_deletes",
        Use::Stored,
        "the held document's own key against the converted carried one",
    ),
    site("kimmy-storage/src/vectors.rs", "chunk_has_no_live_parent", Use::Stored, "parent's row"),
    site("kimmy-storage/src/lab_hooks.rs", "lab_hook_entry_would_write", Use::Stored, "lab hook"),
    // kimmy-storage: index keys
    site("kimmy-storage/src/index.rs", "document_keys", Use::Index, "an entry's value keys"),
    site("kimmy-storage/src/index.rs", "doc_key_for", Use::Index, "an entry's document key"),
    site(
        "kimmy-storage/src/index.rs",
        "(items)",
        Use::Index,
        "the sentinel runs below LEAST_KEY_TAG",
    ),
    site("kimmy-storage/src/expiry.rs", "date_key", Use::Index, "a TTL index's date bound"),
    site("kimmy-storage/src/vectors.rs", "visit_document", Use::Index, "a chunk id prefix's bound"),
    // kimmy-storage: the migration
    site("kimmy-storage/src/migrate_v5.rs", "stored_of_v1", Use::Migration, "a v1 key re-keyed"),
    site(
        "kimmy-storage/src/migrate_v5.rs",
        "ttl_selection_changes",
        Use::Migration,
        "old entries' schema-4 document keys, matched by the re-keyed row's legacy form",
    ),
    // kimmy-storage: test hooks compiled into the product
    site("kimmy-storage/src/faults.rs", "corrupt_document_for_test", Use::Plumbing, "a test hook"),
    site("kimmy-storage/src/migrate.rs", "make_schema_4_for_test", Use::Plumbing, "a test hook"),
];

/// What can reach the tables a function that reads document or index keys writes.
struct Persister {
    file: &'static str,
    function: &'static str,
    why: &'static str,
}

const fn persister(file: &'static str, function: &'static str, why: &'static str) -> Persister {
    Persister { file, function, why }
}

/// Rule 3, first part: every function that reads `DOCS`, `INDEX_ENTRIES` or the staging table
/// and, in a write transaction, names another table.
const PERSISTERS: &[Persister] = &[
    persister(
        "kimmy-storage/src/backup.rs",
        "restore_with",
        "copies a backup's rows into a new store of the backup's own schema; keys as they were",
    ),
    persister(
        "kimmy-storage/src/engine.rs",
        "open_cleared_forced",
        "the ensure-tables commit: opens every table, writes no key bytes outside DOCS",
    ),
    persister(
        "kimmy-storage/src/engine.rs",
        "create_collection_in_txn",
        "reads whether a drop's rows remain under the id; writes the definition",
    ),
    persister(
        "kimmy-storage/src/engine.rs",
        "purge_chunk",
        "removes rows (the collection's idempotency records stay); forgets the digest row",
    ),
    persister("kimmy-storage/src/index.rs", "drop_index_inner", "removes entries; writes a stamp"),
    persister(
        "kimmy-storage/src/late_drop.rs",
        "remove_stragglers",
        "removes rows a drop left; reads arrival positions",
    ),
    persister(
        "kimmy-storage/src/late_drop.rs",
        "nothing_is_stamped_after",
        "reads the digest row's stamps and the rows; writes nothing",
    ),
    persister(
        "kimmy-storage/src/live_walk.rs",
        "run",
        "counts and digests; the walk cursor holds a stored DOCS key (SLOTS: walk)",
    ),
    persister(
        "kimmy-storage/src/migrate.rs",
        "make_schema_4_for_test",
        "a test hook writing a schema-4 store",
    ),
    persister(
        "kimmy-storage/src/migrate_v5.rs",
        "begin",
        "lists DOCS ranges into phases (counts), carries 3 -> 4 holders converted to schema 5",
    ),
    persister("kimmy-storage/src/migrate_v5.rs", "drain", "an index's step: no key"),
    persister(
        "kimmy-storage/src/migrate_v5.rs",
        "refill",
        "holders' and the refill cursor's keys, schema 5, written after the range is re-keyed",
    ),
    persister("kimmy-storage/src/migrate_v5.rs", "finish", "deletes the work tables"),
    persister(
        "kimmy-storage/src/migrate_v5.rs",
        "settle_carried",
        "carried holders' schema-5 keys, checked against DOCS",
    ),
    persister(
        "kimmy-storage/src/rewind.rs",
        "rewind_to",
        "rewinds rows and oplog tables (stamps); violations name DocIds",
    ),
];

struct Slot {
    /// The table, and the slot or key shape inside it.
    place: &'static str,
    /// A production file and a line of it that makes the slot: it must still be there.
    anchor_file: &'static str,
    anchor: &'static str,
    class: Class,
    why: &'static str,
}

/// Rule 3, third part: every function that writes a slot of a name-keyed table (`META`,
/// `LIVE_COUNTS_THROUGH`, `LIVE_DIGESTS_THROUGH`, `VECTOR_VERIFIED`), with the slots it writes,
/// each of which is a row of [`SLOTS`]. A slot whose name is computed (`vector_renames:<id>`)
/// is not a `META_*` constant, so this is what makes a new one visible.
struct SlotWriter {
    file: &'static str,
    function: &'static str,
    slots: &'static [&'static str],
}

const fn writer(
    file: &'static str,
    function: &'static str,
    slots: &'static [&'static str],
) -> SlotWriter {
    SlotWriter { file, function, slots }
}

const SLOT_WRITERS: &[SlotWriter] = &[
    writer("kimmy-storage/src/backup.rs", "restore_with", &["meta[*], copied by a restore"]),
    writer("kimmy-storage/src/decimal128_gate.rs", "mark_in_txn", &["meta[decimal128:id_present]"]),
    writer("kimmy-storage/src/engine.rs", "flush_holding", &["meta[durability_flush]"]),
    writer("kimmy-storage/src/engine.rs", "load_or_create_node_id", &["meta[node_id]"]),
    writer("kimmy-storage/src/engine.rs", "record_state_held_in_txn", &["meta[state_held:<node>]"]),
    // Reads the rows into a vector; the `.insert` is the vector's.
    writer("kimmy-storage/src/engine.rs", "state_held_in", &["meta[state_held:<node>]"]),
    writer("kimmy-storage/src/gc.rs", "remove_oplog_entries", &["meta[oplog_collected_through]"]),
    writer("kimmy-storage/src/live_digest.rs", "carry_mark", &["live_digests_through[arrival]"]),
    writer("kimmy-storage/src/live_digest.rs", "mark_through", &["live_digests_through[arrival]"]),
    writer(
        "kimmy-storage/src/live_walk.rs",
        "run",
        &[
            "live_counts_through[arrival]",
            "live_digests_through[arrival]",
            "live_digests_through[walk]",
        ],
    ),
    writer(
        "kimmy-storage/src/migrate.rs",
        "make_schema_4_for_test",
        &["meta[format_version]", "vector_verified[verified]"],
    ),
    writer("kimmy-storage/src/migrate.rs", "record_redb_version", &["meta[redb_version]"]),
    writer("kimmy-storage/src/migrate.rs", "write_version", &["meta[format_version]"]),
    writer("kimmy-storage/src/migrate_v5.rs", "begin", &["meta[format_version]"]),
    writer("kimmy-storage/src/refusal_hint.rs", "record_refusal_hint_in", &["meta[ddl_refused:*]"]),
    writer("kimmy-storage/src/rewind.rs", "record_rewind", &["meta[rewound]"]),
    writer(
        "kimmy-storage/src/tombstone_horizon.rs",
        "reset_at_open",
        &["meta[gc:tombstone_horizon]"],
    ),
    writer(
        "kimmy-storage/src/tombstone_horizon.rs",
        "set_at_least_in_txn",
        &["meta[gc:tombstone_horizon]"],
    ),
    writer(
        "kimmy-storage/src/tombstone_horizon.rs",
        "test_set_tombstone_horizon",
        &["meta[gc:tombstone_horizon]"],
    ),
    writer(
        "kimmy-storage/src/vectors.rs",
        "bump_vector_renames",
        &["meta[vector_renames:<shadow>]"],
    ),
    writer(
        "kimmy-storage/src/vectors.rs",
        "put_consumer_position_in_txn",
        &["meta[consumer_position:<consumer>]"],
    ),
    writer(
        "kimmy-storage/src/vectors.rs",
        "put_vector_fingerprint",
        &["meta[vector_config:<collection>]"],
    ),
    writer(
        "kimmy-storage/src/vectors.rs",
        "put_vector_rescan",
        &["meta[vector_rescan:<collection>]"],
    ),
    writer(
        "kimmy-storage/src/vectors.rs",
        "queue_vector_chunk_recheck",
        &["meta[vector_chunk_ids]"],
    ),
    writer("kimmy-storage/src/verified.rs", "write", &["vector_verified[verified]"]),
];

/// Rule 3, second part: every `META_*` slot, and the rows design 5.1 names.
const SLOTS: &[Slot] = &[
    Slot {
        place: "meta[node_id]",
        anchor_file: "kimmy-storage/src/tables.rs",
        anchor: r#"pub const META_NODE_ID: &str = "node_id";"#,
        class: Unchanged,
        why: "16 node-id bytes",
    },
    Slot {
        place: "meta[format_version]",
        anchor_file: "kimmy-storage/src/tables.rs",
        anchor: r#"pub const META_FORMAT_VERSION: &str = "format_version";"#,
        class: Unchanged,
        why: "the schema number",
    },
    Slot {
        place: "meta[next_collection_id]",
        anchor_file: "kimmy-storage/src/tables.rs",
        anchor: r#"pub const META_NEXT_COLLECTION_ID: &str = "next_collection_id";"#,
        class: Unchanged,
        why: "a counter nothing reads",
    },
    Slot {
        place: "meta[state_held:<node>]",
        anchor_file: "kimmy-storage/src/tables.rs",
        anchor: r#"pub const META_STATE_HELD_PREFIX: &str = "state_held:";"#,
        class: Unchanged,
        why: "a stamp per origin",
    },
    Slot {
        place: "meta[state_held;] (the end of the state_held rows)",
        anchor_file: "kimmy-storage/src/tables.rs",
        anchor: r#"pub const META_STATE_HELD_END: &str = "state_held;";"#,
        class: Unchanged,
        why: "a range bound, never written",
    },
    Slot {
        place: "meta[oplog_collected_through]",
        anchor_file: "kimmy-storage/src/tables.rs",
        anchor: r#"pub const META_OPLOG_COLLECTED_THROUGH: &str = "oplog_collected_through";"#,
        class: Unchanged,
        why: "a stamp",
    },
    Slot {
        place: "meta[gc:tombstone_horizon]",
        anchor_file: "kimmy-storage/src/tables.rs",
        anchor: r#"pub const META_TOMBSTONE_HORIZON: &str = "gc:tombstone_horizon";"#,
        class: Unchanged,
        why: "a stamp (design 5.1, m6)",
    },
    Slot {
        place: "meta[decimal128:id_present]",
        anchor_file: "kimmy-storage/src/tables.rs",
        anchor: r#"pub const META_DECIMAL128_ID_PRESENT: &str = "decimal128:id_present";"#,
        class: Unchanged,
        why: "the marker's stamp",
    },
    Slot {
        place: "meta[rewound]",
        anchor_file: "kimmy-storage/src/tables.rs",
        anchor: r#"pub const META_REWOUND: &str = "rewound";"#,
        class: Unchanged,
        why: "a rewind's record: stamps",
    },
    Slot {
        place: "meta[redb_version]",
        anchor_file: "kimmy-storage/src/format.rs",
        anchor: r#"pub(crate) const META_REDB_VERSION: &str = "redb_version";"#,
        class: Unchanged,
        why: "a version string",
    },
    Slot {
        place: "meta[*], copied by a restore",
        anchor_file: "kimmy-storage/src/backup.rs",
        anchor: "simple!(T_META, tables::META,",
        class: Unchanged,
        why: "a restore copies every slot of a backup of its own schema, each classed here",
    },
    Slot {
        place: "meta[durability_flush]",
        anchor_file: "kimmy-storage/src/engine.rs",
        anchor: r#"meta.insert("durability_flush","#,
        class: Unchanged,
        why: "a flush counter",
    },
    Slot {
        place: "meta[vector_chunk_ids]",
        anchor_file: "kimmy-storage/src/vectors.rs",
        anchor: r#"const CHUNK_IDS_KEY: &str = "vector_chunk_ids";"#,
        class: Unchanged,
        why: "the chunk-id format number (ADR-239)",
    },
    Slot {
        place: "meta[vector_renames:<shadow>]",
        anchor_file: "kimmy-storage/src/vectors.rs",
        anchor: r#"format!("vector_renames:{}", shadow.0)"#,
        class: Unchanged,
        why: "a counter per shadow collection id",
    },
    Slot {
        place: "meta[consumer_position:<consumer>]",
        anchor_file: "kimmy-storage/src/vectors.rs",
        anchor: r#"format!("consumer_position:{consumer}")"#,
        class: Unchanged,
        why: "an oplog position (a stamp)",
    },
    Slot {
        place: "meta[vector_config:<collection>]",
        anchor_file: "kimmy-storage/src/vectors.rs",
        anchor: r#"format!("vector_config:{}", collection.0)"#,
        class: Unchanged,
        why: "a vector configuration's fingerprint",
    },
    Slot {
        place: "meta[vector_rescan:<collection>]",
        anchor_file: "kimmy-storage/src/vectors.rs",
        anchor: r#"const RESCAN_PREFIX: &str = "vector_rescan:";"#,
        class: Unchanged,
        why: "a rescan marker per collection id",
    },
    Slot {
        place: "meta[ddl_refused:*]",
        anchor_file: "kimmy-storage/src/refusal_hint.rs",
        anchor: r#"const PREFIX: &str = "ddl_refused:";"#,
        class: Unchanged,
        why: "definition hashes (design 5.1, m6)",
    },
    Slot {
        place: "live_digests_through[walk]",
        anchor_file: "kimmy-storage/src/live_digest.rs",
        anchor: r#"pub(crate) const WALK: &str = "walk";"#,
        class: Unchanged,
        why: "the open walk's cursor, a stored DOCS key, resumed only by the build, commit and \
              schema that wrote it (live_walk.rs is_ours), and the walk runs after the migration: \
              a 0.50 cursor is discarded, never read as schema 5",
    },
    Slot {
        place: "live_digests_through[arrival]",
        anchor_file: "kimmy-storage/src/live_digest.rs",
        anchor: r#"pub(crate) const THROUGH: &str = "arrival";"#,
        class: Unchanged,
        why: "the digests' mark: a format byte, an arrival position, an oplog key",
    },
    Slot {
        place: "live_counts_through[arrival]",
        anchor_file: "kimmy-storage/src/live_count.rs",
        anchor: r#"pub(crate) const THROUGH: &str = "arrival";"#,
        class: Unchanged,
        why: "the counts' mark: an arrival position and an oplog key",
    },
    Slot {
        place: "vector_verified[verified]",
        anchor_file: "kimmy-storage/src/verified.rs",
        anchor: r#"const KEY: &str = "verified";"#,
        class: Unchanged,
        why: "the verified record",
    },
    Slot {
        place: "snapshot_progress[<peer>]",
        anchor_file: "kimmy-storage/src/engine.rs",
        anchor: "pub(crate) fn persist_snapshot_progress_in_txn(",
        class: LegacyAsReceived,
        why: "SnapshotProgress with the cursor as the peer sent it (design 5.1, review B m1)",
    },
    Slot {
        place: "the in-memory Converge cursor",
        anchor_file: "kimmy-storage/src/snapshot.rs",
        anchor: "pub fn of_collection_converging(",
        class: LegacyAsReceived,
        why: "a Converge pull's SnapshotProgress: the received cursor, never persisted",
    },
    Slot {
        place: "Engine::answered_absent",
        anchor_file: "kimmy-storage/src/engine.rs",
        anchor: "pub(crate) answered_absent:",
        class: Unchanged,
        why: "collection ids and coverage in memory; nothing persisted",
    },
    Slot {
        place: "the vector shadow collections",
        anchor_file: "kimmy-core/src/vector_meta.rs",
        anchor: "pub fn shadow_name(collection: &str) -> String {",
        class: Unchanged,
        why: "chunk documents with string ids \"<source>#<n>\", not the parent's key bytes; the \
              parent is found through doc_key (CALL_SITES: chunk_has_no_live_parent)",
    },
];

// -- Reading the source -----------------------------------------------------------------------

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().expect("the repo root")
}

/// Every production source file, by its path under `crates/`.
fn sources() -> BTreeMap<String, String> {
    let crates = root().join("crates");
    source::production_sources(&root())
        .into_iter()
        .map(|(path, body)| {
            let rel = path.strip_prefix(&crates).expect("under crates/").to_string_lossy();
            (rel.replace('\\', "/"), body)
        })
        .collect()
}

/// The code of a line, without its comment.
fn code(line: &str) -> &str {
    line.split("//").next().unwrap_or_default()
}

/// The `fn` this line starts, and the line's indentation.
fn starts_fn(line: &str) -> Option<(String, usize)> {
    let indent = line.len() - line.trim_start().len();
    let mut rest = line.trim_start();
    if let Some(after) = rest.strip_prefix("pub") {
        rest = match after.strip_prefix('(') {
            Some(scoped) => scoped.split_once(')')?.1.trim_start(),
            None => after.strip_prefix(' ')?,
        };
    }
    for qualifier in ["const ", "async ", "unsafe "] {
        rest = rest.strip_prefix(qualifier).unwrap_or(rest);
    }
    if rest.starts_with("extern \"") {
        rest = rest.splitn(3, '"').nth(2)?.trim_start();
    }
    let name: String = rest
        .strip_prefix("fn ")?
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    (!name.is_empty()).then_some((name, indent))
}

/// A file's code by function: `(name, code)`, with everything outside a function under
/// `(items)`. A function ends at the `}` on its own indentation, as rustfmt lays it out, so a
/// nested `fn` belongs to the one it is in.
fn functions(body: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut items = String::new();
    let mut current: Option<(String, usize, String)> = None;
    for line in body.lines() {
        match current.as_mut() {
            None => match starts_fn(line) {
                Some((name, indent)) => {
                    let text = code(line).trim_end();
                    let whole = text.ends_with(';') || (text.contains('{') && text.ends_with('}'));
                    if whole {
                        out.push((name, text.to_string()));
                    } else {
                        current = Some((name, indent, format!("{text}\n")));
                    }
                }
                None => {
                    items.push_str(code(line));
                    items.push('\n');
                }
            },
            Some((name, indent, text)) => {
                text.push_str(code(line));
                text.push('\n');
                if line.len() == *indent + 1 && line.ends_with('}') && line.trim() == "}" {
                    out.push((std::mem::take(name), std::mem::take(text)));
                    current = None;
                }
            }
        }
    }
    if let Some((name, _, text)) = current {
        out.push((name, text));
    }
    out.push(("(items)".to_string(), items));
    out
}

/// What makes, converts or carries a key (rule 2).
const KEY_CALLS: [&str; 7] = [
    "keyenc::",
    "encode_compound",
    "doc_key(",
    "legacy_key(",
    "legacy_of_stored(",
    "stored_of_legacy(",
    "LegacyKey",
];

/// The encoder's own modules, which are what rule 2's calls call.
fn is_the_encoder(file: &str) -> bool {
    file.starts_with("kimmy-core/src/keyenc") || file == "kimmy-core/src/legacy_key.rs"
}

/// Every `TableDefinition` const of `kimmy-storage`'s production code: `(file, const, name)`,
/// and the definitions that do not name their table by a literal.
fn table_definitions(
    sources: &BTreeMap<String, String>,
) -> (Vec<(String, String, String)>, Vec<String>) {
    let mut found = Vec::new();
    let mut unnamed = Vec::new();
    for (file, body) in sources.iter().filter(|(f, _)| f.starts_with("kimmy-storage/")) {
        let text: String = body.lines().map(|l| format!("{}\n", code(l))).collect();
        // Each `::new(` whose statement names `TableDefinition`: `TableDefinition::new(` or
        // `TableDefinition::<K, V>::new(`, after a `const NAME: TableDefinition<K, V> =`.
        let mut from = 0;
        while let Some(found_at) = text[from..].find("::new(") {
            let at = from + found_at;
            from = at + "::new(".len();
            let statement_start = text[..at].rfind(';').map_or(0, |n| n + 1);
            let statement = &text[statement_start..at];
            if !statement.contains("TableDefinition") {
                continue;
            }
            let constant = statement
                .rsplit_once("const ")
                .map(|(_, c)| c.split(':').next().unwrap_or_default().trim().to_string())
                .unwrap_or_default();
            let arg = text[from..].trim_start();
            match arg.strip_prefix('"').and_then(|a| a.split_once('"')) {
                Some((name, _)) => found.push((file.clone(), constant, name.to_string())),
                None => unnamed.push(format!(
                    "{file}: TableDefinition::new({})",
                    arg.split(')').next().unwrap_or_default()
                )),
            }
        }
    }
    (found, unnamed)
}

// -- The rules --------------------------------------------------------------------------------

/// Rule 1: every table is classified, and every classified table exists.
#[test]
fn every_table_is_classified_for_schema_5() {
    let sources = sources();
    let (defined, unnamed) = table_definitions(&sources);
    assert!(
        unnamed.is_empty(),
        "a table named by something other than a literal cannot be held to the manifest: {unnamed:#?}"
    );
    // Premise: the walk found the store's tables, the migration's included.
    for must in ["docs", "index_entries", "snapshot_progress", "schema5_docs_rekeyed"] {
        assert!(defined.iter().any(|(_, _, n)| n == must), "premise: {must} was found");
    }
    let listed: BTreeSet<&str> = TABLES.iter().map(|t| t.name).collect();
    assert_eq!(listed.len(), TABLES.len(), "a table is listed twice");
    let names: BTreeSet<&str> = defined.iter().map(|(_, _, n)| n.as_str()).collect();
    let unlisted: Vec<String> = defined
        .iter()
        .filter(|(_, _, n)| !listed.contains(n.as_str()))
        .map(|(file, constant, name)| format!("{file}: {constant} = {name:?}"))
        .collect();
    let stale: Vec<&str> = listed.difference(&names).copied().collect();
    assert!(
        unlisted.is_empty() && stale.is_empty(),
        "tables no one has classified for schema 5 (does its key or value hold a document or \
         index key? list it in TABLES as ReKeyed, Unchanged or LegacyAsReceived, with why): \
         {unlisted:#?}\nTABLES names tables that no longer exist: {stale:#?}"
    );
    // The classes the design fixes.
    let class = |name: &str| TABLES.iter().find(|t| t.name == name).map(|t| t.class);
    assert_eq!(class("docs"), Some(ReKeyed));
    assert_eq!(class("index_entries"), Some(ReKeyed));
    assert_eq!(class("snapshot_progress"), Some(LegacyAsReceived));
    assert_eq!(class("live_digests"), Some(Unchanged), "digest rows stand (design 7.3)");
}

/// Rule 2: every function that makes, converts or carries a key is listed, in every crate,
/// and every listed function still does.
#[test]
fn every_key_call_site_is_classified() {
    let sources = sources();
    // Premise: other crates were read, and the call sites the rule exists for were found.
    for must in ["kimmy-api/src/exec.rs", "kimmyd/src/main.rs", "kimmy-cluster/src/transport.rs"] {
        assert!(sources.contains_key(must), "premise: {must} was read");
    }
    let mut found: BTreeMap<(String, String), BTreeSet<&str>> = BTreeMap::new();
    for (file, body) in sources.iter().filter(|(f, _)| !is_the_encoder(f)) {
        for (function, text) in functions(body) {
            for call in KEY_CALLS.iter().filter(|c| text.contains(**c)) {
                found.entry((file.clone(), function.clone())).or_default().insert(call);
            }
        }
    }
    assert!(found.contains_key(&("kimmy-storage/src/docs.rs".into(), "doc_key".into())), "premise");
    let listed: BTreeSet<(String, String)> =
        CALL_SITES.iter().map(|s| (s.file.to_string(), s.function.to_string())).collect();
    assert_eq!(listed.len(), CALL_SITES.len(), "a call site is listed twice");
    let unlisted: Vec<String> = found
        .iter()
        .filter(|(at, _)| !listed.contains(*at))
        .map(|((file, function), calls)| format!("{file} {function}: {calls:?}"))
        .collect();
    let stale: Vec<String> = listed
        .iter()
        .filter(|at| !found.contains_key(*at))
        .map(|(file, function)| format!("{file} {function}"))
        .collect();
    assert!(
        unlisted.is_empty() && stale.is_empty(),
        "functions that make, convert or carry a key and are not in CALL_SITES (is the key \
         stored, an index key, a client token, on the wire, a digest term? a key that leaves \
         the engine is legacy, design 7.1 and 7.2): {unlisted:#?}\nCALL_SITES names functions that no longer make a key: {stale:#?}"
    );
}

/// Rule 3, first part: every function that reads document or index keys and writes another
/// table in the same transaction says what reaches it.
#[test]
fn every_writer_that_reads_document_keys_is_classified() {
    let sources = sources();
    let (defined, _) = table_definitions(&sources);
    let reads = ["DOCS", "INDEX_ENTRIES", "DOCS_REKEYED"];
    let others: BTreeSet<&str> = defined
        .iter()
        .map(|(_, constant, _)| constant.as_str())
        .filter(|c| !reads.contains(c))
        .collect();
    let words = |text: &str| -> BTreeSet<String> {
        text.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .filter(|w| !w.is_empty())
            .map(String::from)
            .collect()
    };
    let mut found: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
    for (file, body) in sources.iter().filter(|(f, _)| f.starts_with("kimmy-storage/")) {
        for (function, text) in functions(body).into_iter().filter(|(f, _)| f != "(items)") {
            let named = words(&text);
            let reads_keys = reads.iter().any(|r| named.contains(*r))
                || ["doc_range(", "index_range(", "doc_range_after("]
                    .iter()
                    .any(|r| text.contains(r));
            let writes =
                ["begin_write(", "WriteTxn", "WriteTransaction"].iter().any(|w| text.contains(w));
            let tables: BTreeSet<String> =
                named.iter().filter(|w| others.contains(w.as_str())).cloned().collect();
            if reads_keys && writes && !tables.is_empty() {
                found.insert((file.clone(), function), tables);
            }
        }
    }
    assert!(
        found.contains_key(&("kimmy-storage/src/live_walk.rs".into(), "run".into())),
        "premise: the open walk, which persists a DOCS key as its cursor, was found"
    );
    let listed: BTreeSet<(String, String)> =
        PERSISTERS.iter().map(|p| (p.file.to_string(), p.function.to_string())).collect();
    let unlisted: Vec<String> = found
        .iter()
        .filter(|(at, _)| !listed.contains(*at))
        .map(|((file, function), tables)| format!("{file} {function} writes {tables:?}"))
        .collect();
    let stale: Vec<String> = listed
        .iter()
        .filter(|at| !found.contains_key(*at))
        .map(|(file, function)| format!("{file} {function}"))
        .collect();
    assert!(
        unlisted.is_empty() && stale.is_empty(),
        "functions that read document or index keys and write another table, not in \
         PERSISTERS (does a key's bytes reach that table? then its form must be classified, \
         design 5.1): {unlisted:#?}\nPERSISTERS names functions that no longer qualify: {stale:#?}"
    );
}

/// Rule 3, second part: every `META_*` slot is classified, and every named row's anchor still
/// stands where it says.
#[test]
fn every_slot_is_classified_and_anchored() {
    let sources = sources();
    let mut slots = Vec::new();
    for (file, body) in sources.iter().filter(|(f, _)| f.starts_with("kimmy-storage/")) {
        for line in body.lines() {
            let line = code(line).trim();
            let constant = line
                .strip_prefix("pub const ")
                .or_else(|| line.strip_prefix("pub(crate) const "))
                .or_else(|| line.strip_prefix("const "));
            if let Some(rest) = constant
                && rest.starts_with("META_")
                && rest.contains(": &str")
            {
                slots.push((file.clone(), line.to_string()));
            }
        }
    }
    assert!(slots.len() >= 10, "premise: the META slots were found ({})", slots.len());
    let unlisted: Vec<String> = slots
        .iter()
        .filter(|(file, line)| !SLOTS.iter().any(|s| s.anchor_file == file && s.anchor == line))
        .map(|(file, line)| format!("{file}: {line}"))
        .collect();
    let stale: Vec<String> = SLOTS
        .iter()
        .filter(|s| !sources.get(s.anchor_file).is_some_and(|body| body.contains(s.anchor)))
        .map(|s| format!("{} ({}: {})", s.place, s.anchor_file, s.anchor))
        .collect();
    assert!(
        unlisted.is_empty() && stale.is_empty(),
        "META slots no one has classified (does the value hold a document key? list it in \
         SLOTS): {unlisted:#?}\nSLOTS rows whose anchor is gone: {stale:#?}"
    );
    let class = |place: &str| SLOTS.iter().find(|s| s.place == place).map(|s| s.class);
    assert_eq!(class("snapshot_progress[<peer>]"), Some(LegacyAsReceived));
    assert_eq!(class("live_digests_through[walk]"), Some(Unchanged));
}

/// The function splitter the rules attribute by: a nested `fn` belongs to the one it is in, a
/// one-line `fn` ends on its line, and what is outside every function is the file's items.
#[test]
fn the_splitter_attributes_a_line_to_the_function_it_is_in() {
    let body = "use a::keyenc::encode;\n\
                fn outer() {\n    fn inner() {}\n    doc_key(x);\n}\n\
                impl X {\n    pub(crate) fn method(&self) -> u8 {\n        1\n    }\n    \
                fn short() -> u8 { 0 }\n}\n\
                pub fn last() {\n    legacy_key(y);\n}\n";
    let got: Vec<(String, bool)> = functions(body)
        .into_iter()
        .map(|(name, text)| {
            let keyed = KEY_CALLS.iter().any(|c| text.contains(c));
            (name, keyed)
        })
        .collect();
    let want: Vec<(String, bool)> =
        [("outer", true), ("method", false), ("short", false), ("last", true), ("(items)", true)]
            .into_iter()
            .map(|(n, k)| (n.to_string(), k))
            .collect();
    assert_eq!(got, want);
}

/// Every entry says why, and the manifest's size by class, printed for the review that reads it.
#[test]
fn every_entry_says_why() {
    let mut tables: BTreeMap<Class, usize> = BTreeMap::new();
    for t in TABLES {
        assert!(!t.why.trim().is_empty(), "TABLES: {} says no why", t.name);
        *tables.entry(t.class).or_default() += 1;
    }
    let mut sites: BTreeMap<Use, usize> = BTreeMap::new();
    for s in CALL_SITES {
        assert!(!s.why.trim().is_empty(), "CALL_SITES: {} {} says no why", s.file, s.function);
        *sites.entry(s.uses).or_default() += 1;
    }
    for p in PERSISTERS {
        assert!(!p.why.trim().is_empty(), "PERSISTERS: {} {} says no why", p.file, p.function);
    }
    let mut slots: BTreeMap<Class, usize> = BTreeMap::new();
    for s in SLOTS {
        assert!(!s.why.trim().is_empty(), "SLOTS: {} says no why", s.place);
        *slots.entry(s.class).or_default() += 1;
    }
    eprintln!(
        "tables {tables:?}; call sites {sites:?}; persisters {}; slots {slots:?}",
        PERSISTERS.len()
    );
}

/// Rule 3, third part: every function that writes a slot of a name-keyed table is listed, with
/// slots that are rows of [`SLOTS`], and every listed function still writes one.
#[test]
fn every_slot_writer_is_classified() {
    let sources = sources();
    let named = ["META", "LIVE_COUNTS_THROUGH", "LIVE_DIGESTS_THROUGH", "VECTOR_VERIFIED"];
    let mut found: BTreeMap<(String, String), BTreeSet<&str>> = BTreeMap::new();
    for (file, body) in sources.iter().filter(|(f, _)| f.starts_with("kimmy-storage/")) {
        for (function, text) in functions(body).into_iter().filter(|(f, _)| f != "(items)") {
            let words: BTreeSet<&str> =
                text.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).collect();
            let tables: BTreeSet<&str> =
                named.iter().copied().filter(|t| words.contains(t)).collect();
            if !tables.is_empty() && text.contains(".insert(") {
                found.insert((file.clone(), function), tables);
            }
        }
    }
    assert!(
        found.contains_key(&(
            "kimmy-storage/src/tombstone_horizon.rs".into(),
            "set_at_least_in_txn".into()
        )),
        "premise: the horizon's writer was found"
    );
    let listed: BTreeSet<(String, String)> =
        SLOT_WRITERS.iter().map(|w| (w.file.to_string(), w.function.to_string())).collect();
    let unlisted: Vec<String> = found
        .iter()
        .filter(|(at, _)| !listed.contains(*at))
        .map(|((file, function), tables)| format!("{file} {function} writes {tables:?}"))
        .collect();
    let stale: Vec<String> = listed
        .iter()
        .filter(|at| !found.contains_key(*at))
        .map(|(file, function)| format!("{file} {function}"))
        .collect();
    assert!(
        unlisted.is_empty() && stale.is_empty(),
        "functions that write a slot of a name-keyed table, not in SLOT_WRITERS (name the slot \
         in SLOTS: does its value hold a document key?): {unlisted:#?}\nSLOT_WRITERS names functions that write no slot: {stale:#?}"
    );
    let places: BTreeSet<&str> = SLOTS.iter().map(|s| s.place).collect();
    for w in SLOT_WRITERS {
        assert!(!w.slots.is_empty(), "{} {} names no slot", w.file, w.function);
        for slot in w.slots {
            assert!(
                places.contains(slot),
                "{} {} writes {slot}, which SLOTS lacks",
                w.file,
                w.function
            );
        }
    }
}
