//! redb table definitions.
//!
//! Keys are raw bytes or tuples of them, because ordering is decided by
//! [`kimmy_core::keyenc`] rather than by redb's type system. redb compares
//! `&[u8]` lexicographically and tuples component-wise, which is exactly the
//! behaviour the encoder is built to exploit.

use redb::TableDefinition;

/// Singleton engine state: node id, format version, collection id counter.
pub const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

/// `db_name -> DatabaseMeta` (JSON).
pub const DATABASES: TableDefinition<&str, &[u8]> = TableDefinition::new("databases");

/// `(db_name, collection_name) -> CollectionMeta` (JSON).
pub const COLLECTIONS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("collections");

/// `(collection_id, encoded _id) -> DocRecord`.
///
/// The collection id leads the key so that a whole collection is one
/// contiguous range, making scans and drops a single range operation.
pub const DOCS: TableDefinition<(u64, &[u8]), &[u8]> = TableDefinition::new("docs");

/// `collection id -> live records under that id in DOCS` (ADR-174).
///
/// Kept, not derived on read: moved in the transaction of every write to
/// [`DOCS`], through `live_count::put_record` and `live_count::remove_record`,
/// so the divergence check reads a count instead of walking a collection. A
/// collection with no live records has no row. Node-local and absent from a
/// backup; `Engine::open` rebuilds it when [`LIVE_COUNTS_THROUGH`] says it may
/// be stale.
pub const LIVE_COUNTS: TableDefinition<u64, u64> = TableDefinition::new("live_counts");

/// `"arrival" -> the arrival position the live counts are kept through`.
///
/// Written with every oplog append by a build that keeps [`LIVE_COUNTS`]. Every
/// document write appends, so a mark behind the arrival index's end means a
/// build that does not keep the counts wrote since, and a missing one means no
/// build that does has written here.
pub const LIVE_COUNTS_THROUGH: TableDefinition<&str, u64> =
    TableDefinition::new("live_counts_through");

/// `(collection_id, index_id, encoded key, encoded _id)`.
pub type IndexKey<'a> = (u64, u32, &'a [u8], &'a [u8]);

/// `(collection_id, index_id, encoded key, encoded _id) -> ()`.
///
/// The document id is part of the key rather than the value so that a
/// non-unique index can hold many documents under one value without a
/// multimap, and so that deleting one entry needs no read-modify-write.
pub const INDEX_ENTRIES: TableDefinition<IndexKey<'static>, ()> =
    TableDefinition::new("index_entries");

/// `(hlc || node) -> OplogEntry`.
///
/// A flat byte key rather than a tuple, because the 26-byte concatenation
/// already sorts in exactly the total write order.
pub const OPLOG: TableDefinition<&[u8], &[u8]> = TableDefinition::new("oplog");

/// `arrival sequence -> (hlc || node)`.
///
/// The oplog is keyed by *origin* stamp, because that is what last-writer-wins
/// and anti-entropy compare. A replicated entry therefore lands at its original
/// position, which may be **behind** the local tail — and a change-stream
/// subscriber already past that point would never see it.
///
/// So streams follow this second ordering instead: a monotonic counter assigned
/// when an entry is appended *here*, which is by construction append-only no
/// matter where the entry came from. On a single node the two orderings agree
/// exactly, which is why single-node semantics are unchanged.
///
/// Both this and [`OPLOG_ARRIVAL_SEQ`] are **derived** from [`OPLOG`] — see
/// `Engine::rebuild_arrival_index`. Nothing is lost if they are discarded.
pub const OPLOG_ARRIVAL: TableDefinition<u64, &[u8]> = TableDefinition::new("oplog_arrival");

/// `(hlc || node) -> arrival sequence`. The reverse of [`OPLOG_ARRIVAL`].
///
/// Exists so that resuming a stream stays a point lookup. A resume token names
/// an entry by stamp — it is a public, opaque contract that predates this index
/// and clients hold them across upgrades — and finding where that entry sits in
/// arrival order would otherwise be a scan, since stamp order and arrival order
/// differ precisely when it matters.
pub const OPLOG_ARRIVAL_SEQ: TableDefinition<&[u8], u64> =
    TableDefinition::new("oplog_arrival_seq");

/// `collection id -> the Stamp of the drop that removed it`.
///
/// The collection equivalent of a document tombstone, and it exists for the
/// same reason. Without one, a `DropCollection` entry is the only record of the
/// drop, so once it ages out of the oplog a peer partitioned across that window
/// rejoins still holding the collection — and anti-entropy, seeing entries the
/// dropper lacks, recreates it along with every document in it.
///
/// Keyed by **id**, not by name, so that a replicated *document* entry can be
/// checked against it directly: an entry names its collection by id, and a node
/// that has dropped the collection can no longer resolve that id to a name.
///
/// Collected under `tombstone_retention_secs`, alongside document tombstones,
/// because it answers the same question over the same window.
pub const COLLECTIONS_DROPPED: TableDefinition<u64, &[u8]> =
    TableDefinition::new("collections_dropped");

/// `(collection id, index id) -> the Stamp of the drop that removed the index`.
///
/// The index equivalent of [`COLLECTIONS_DROPPED`], and it exists because the
/// `DropIndex` entry was the only record of the drop. A peer re-serves the
/// window holding the `CreateIndex` entry as a matter of course — windows
/// overlap, because `entries_for_peer` serves in global stamp order from one
/// threshold — and once the drop ages out of the oplog nothing here said the
/// index was gone, so every replay rebuilt it. That is a resurrection, as a
/// replayed `CreateCollection` was before ADR-034, and it was worse than one:
/// the rebuild backfills over *this* node's current documents, which may by
/// then hold what the definition forbids (a document with arrays at two of a
/// compound index's paths, written legally once the index was gone; two
/// documents sharing a key the index calls unique), and the backfill's error
/// aborted the round, so the same window was re-requested for ever. Observed on
/// a three-member cluster running 0.20.0: one member's writes never reached the
/// others again, while the lag gauge read 0 and every member showed as live.
///
/// Keyed by the index **id**, which is derived from the index name
/// (`IndexMeta::derive_id`), so a `DropIndex` entry — which carries only the
/// name — and a `CreateIndex` entry — which carries the definition — compute
/// the same key. Keyed under the collection id for the same reason the
/// collection tombstone is: it is what a replicated entry names.
///
/// Collected under `tombstone_retention_secs`, alongside the other tombstones,
/// because it answers the same question over the same window. See ADR-123.
pub const INDEXES_DROPPED: TableDefinition<(u64, u32), &[u8]> =
    TableDefinition::new("indexes_dropped");

/// `node id (16 bytes) -> highest Hlc held from that node`.
///
/// The version vector, maintained incrementally rather than computed. Deriving
/// it on demand would mean scanning the whole oplog for a max-per-node, which
/// is O(n) for a value read on every gossip round.
///
/// **Rebuilt, but not purely derived.** `Engine::open` raises it to cover the
/// oplog if it does not already, so a lost or stale vector is repaired -- but
/// the rebuild only ever RAISES (ADR-036), and it skips the entries this node
/// holds as state (`OPLOG_HELD`, ADR-160). So the oplog is a lower bound on
/// coverage and not the whole of it: a completed snapshot's grant is not
/// recoverable from the oplog, and discarding this table loses it.
pub const OPLOG_VERSIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("oplog_versions");

/// Newest stamp this node has **processed** per origin, appended or not.
///
/// [`OPLOG_VERSIONS`] answers "what can I serve a peer"; this answers "what
/// have I already seen". They differ for everything a node handles correctly
/// without writing an oplog entry: replicated DDL (deliberately not logged), a
/// document that loses last-writer-wins, and entries skipped by design.
///
/// Treating the first as though it were the second is what made every cluster
/// re-request the same entries on every sync round forever, and pinned
/// `kimmy_replication_lag_seconds` non-zero on a converged cluster. See
/// [ADR-054](../../../docs/decisions.md).
///
/// Always greater than or equal to [`OPLOG_VERSIONS`], because appending an
/// entry raises both.
pub const OPLOG_WITNESSED: TableDefinition<&[u8], &[u8]> = TableDefinition::new("oplog_witnessed");

/// `oplog key -> ()`, for every entry this node appended as state rather than
/// as history: a snapshot document (ADR-152's `Position::Hold`).
///
/// **The one thing the oplog itself cannot say.** An entry records what the
/// change was, never how this node came by it, and a snapshot document is
/// deliberately reconstructed as an ordinary replicated write so that
/// last-writer-wins, the indexes and the unique check all treat it as one. So
/// `Engine::open`, rebuilding the version vector from the oplog, could not
/// tell a document it holds as *state* from one it holds as *history*, and
/// raised its position over both — claiming to be able to serve a contiguous
/// window it cannot serve. See ADR-160.
///
/// A node-local table and **not a flag on the entry**, because Hold-versus-Raise
/// is a property of how THIS node applied the entry and not of the entry: the
/// same entry is Raise at its origin and Hold at a snapshot receiver. A flag
/// would also be a wire and format change needing capability negotiation; this
/// needs neither. `OPLOG_WITNESSED` and `OPLOG_COLLECTED` are the same shape --
/// node-local, absent from the backup, re-derived at open -- so this follows
/// the house pattern rather than introducing one.
///
/// **What removes a mark, and what does not.** A mark goes when the entry stops
/// being state: a completed snapshot's grant releases every stamp it covers, in
/// the transaction that adopts it; the entry arriving in a window contiguous
/// from this node's position releases it, whether that arrival appends the key
/// or is superseded at it (ADR-169); retention collects the mark with the entry
/// it names; and a rewind removes it with the row it discards.
///
/// **The table is not self-emptying, and it is worth being exact about when it
/// is not**, because the ADR-054 repair case turns a lingering mark into a node
/// that under-claims what it can serve:
///
/// * a **scoped** repair (ADR-148) grants no coverage at all, so none of its
///   marks is released by a grant. They go on retention, or when their entries
///   arrive in a window contiguous from this node's position (ADR-169).
/// * a **completed whole-database** snapshot releases only what its grant
///   covers, and the grant is the FIRST page's vector. A document written on
///   the sender after that vector was read, but still ahead of the cursor, is
///   carried at a stamp above it -- so its mark survives a snapshot that
///   completed perfectly.
///
/// So a healthy, caught-up node can hold marks, and on a busy sender it
/// normally will. That is safe -- `Engine::open` merges and never lowers, so a
/// mark can only withhold a raise -- but it is not nothing, and the rebuild is
/// exactly the path that matters when the stored vector has been lost.
pub const OPLOG_HELD: TableDefinition<&[u8], ()> = TableDefinition::new("oplog_held");

/// `peer node id (16 bytes) -> the snapshot pull left to resume with it`.
///
/// A snapshot that does not fit one round already resumes across rounds
/// (ADR-152), but only while the process lives: the progress is held in a map
/// on the cluster transport and dies with it. A member restarted part-way
/// through a large snapshot began again at page one, re-transferring
/// everything it had already applied.
///
/// Keyed by the peer serving the snapshot, and **the key is opaque here**:
/// this module stores the bytes and never asks what they mean. It is the same
/// shape `OPLOG_VERSIONS` and `OPLOG_WITNESSED` already use -- a node id as a
/// table key -- so nothing about a peer crosses into storage that was not
/// already here. See ADR-161.
pub const SNAPSHOT_PROGRESS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("snapshot_progress");

// Keys within the META table.
pub const META_NODE_ID: &str = "node_id";
pub const META_FORMAT_VERSION: &str = "format_version";
/// Left over from counter-allocated collection ids. Ids are derived from the
/// collection name now, so nothing reads this; it is kept so that a database
/// written by an older build still round-trips rather than losing a key.
pub const META_NEXT_COLLECTION_ID: &str = "next_collection_id";

/// The highest `Hlc` retention has removed from the oplog.
///
/// The replication horizon, recorded rather than inferred. The oldest *retained*
/// entry cannot stand in for it: on a node that has never collected anything,
/// that is simply the first write ever made, and a peer asking from before it
/// would be sent a full snapshot it does not need.
///
/// One stamp across every origin, which is why it is coarse: a peer whose
/// coverage of *one* origin sits below it is beyond it, even when nothing of
/// that origin was collected. [`OPLOG_COLLECTED`] is the same fact per origin.
pub const META_OPLOG_COLLECTED_THROUGH: &str = "oplog_collected_through";

/// Every point-in-time rewind this database has taken: its target, and per
/// origin the newest stamp it discarded (`Engine::rewinds`).
///
/// A rewind removes oplog entries a change stream may already have delivered,
/// so a resume token naming one of them describes history that no longer
/// exists and is refused (ADR-173). The record is what tells such a token from
/// one that names an entry this member simply never held. Appended to, never
/// collected: a rewind is a rare, deliberate operator action, and a record per
/// rewind is a few dozen bytes.
pub const META_REWOUND: &str = "rewound";

/// `node id (16 bytes) -> highest Hlc retention has removed from that origin`.
///
/// [`META_OPLOG_COLLECTED_THROUGH`] split by origin. It answers the question
/// the single stamp cannot: *does this peer lack anything I have collected?*
/// A peer trails an origin below the coarse horizon whenever that origin
/// wrote nothing for longer than retention and then wrote once — a member
/// that idles between rolling restarts does exactly that — and the coarse
/// answer is a snapshot, or a stale-rejoiner verdict, for a gap that holds
/// one servable entry. Per origin, the gap is seen to hold nothing collected.
///
/// Maintained by the retention pass in the same transaction as the removal.
/// A database an earlier build collected from has no record here of what
/// that build removed, so `Engine::open` seeds every origin it holds with the
/// coarse horizon: coarse below that point, exact above it (ADR-097).
pub const OPLOG_COLLECTED: TableDefinition<&[u8], &[u8]> = TableDefinition::new("oplog_collected");
