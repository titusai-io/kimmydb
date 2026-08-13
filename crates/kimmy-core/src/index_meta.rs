//! Index definitions.
//!
//! Lives in `kimmy-core` because both `kimmy-storage` (which maintains index
//! entries) and `kimmy-query` (which plans against them) need this shape, and
//! neither should depend on the other.

use bson::Document;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexMeta {
    /// Derived from [`Self::name`], so every node agrees.
    ///
    /// Index entries are keyed by this, and an index definition replicates
    /// between nodes — so a node-local counter would mean node A's index 1 and
    /// node B's index 1 keying the same storage while describing different
    /// indexes. The same failure as node-local collection ids
    /// ([ADR-031](../../../docs/decisions.md)), one level down.
    pub id: u32,
    pub name: String,
    pub fields: Vec<IndexField>,
    #[serde(default)]
    pub unique: bool,
    /// How `unique` is enforced once the node has peers. Ignored when
    /// `unique` is false.
    #[serde(default)]
    pub enforcement: Enforcement,
    /// Whether any indexed field has ever held an array — or a path that fans
    /// out through one — in this collection.
    ///
    /// Observed on the write path and at backfill, in the same transaction as
    /// the index entries. **One-way:** clearing it safely would require proving
    /// no document still holds an array, which is a full scan for the sake of a
    /// planner hint. A node-local observation that converges because the data
    /// does; while nodes disagree, each plans correctly over the documents it
    /// holds.
    ///
    /// Why the planner cares: different array elements may satisfy each end of
    /// a range — `{a: [2, 0]}` matches `{$gte: 1, $lte: 1}` — so a two-sided
    /// key range is only sound when no document contributes more than one key.
    /// `false` here is what licenses using both bounds.
    #[serde(default)]
    pub multikey: bool,
    /// Delete a document this many seconds after the indexed date.
    ///
    /// `None` is an ordinary index. `Some(n)` additionally makes the index a
    /// **TTL** index: a background pass range-scans it for entries older than
    /// `n` seconds and deletes the documents behind them. The index is both the
    /// policy and the mechanism, which is what keeps expiry proportional to the
    /// number of *expired* documents rather than to the size of the collection.
    ///
    /// **`i64` deliberately, not `u64`.** This crosses two format boundaries —
    /// JSON in the collection metadata and BSON in the replicated
    /// [`crate::IndexCreate`] — and BSON cannot hold a `u64` above `i64::MAX`.
    /// Letting the encoder decide has cost this project a replication outage
    /// twice (ADR-031 and the `NodeId` note in [Handoff](../../../docs/handoff.md)),
    /// so the representation is chosen here rather than inherited. Validated
    /// positive where an index is created.
    ///
    /// Only meaningful on a single-field index over a date, which is enforced
    /// at creation.
    #[serde(default)]
    pub expire_after_secs: Option<i64>,
    /// Index only the documents matching this filter.
    ///
    /// `None` indexes everything. `Some(_)` makes it a **partial** index, and
    /// the planner may then use it only for a query provably contained by the
    /// filter — see [`crate::PartialFilter`], whose deliberately small language
    /// is what makes that containment a decision rather than a guess.
    ///
    /// A *sparse* index is this with `{field: {$exists: true}}`, which is why
    /// there is no separate `sparse` flag: MongoDB treats partial as
    /// superseding sparse and so does this.
    ///
    /// Stored as the filter document. Verified to round-trip losslessly
    /// through both boundaries it crosses — canonical Extended JSON in the
    /// collection metadata, BSON in the replicated [`crate::IndexCreate`] —
    /// including dates and integers above 2^53.
    #[serde(default)]
    pub partial_filter: Option<Document>,
}

/// How far a unique constraint reaches.
///
/// Uniqueness is a *global* invariant — deciding whether a write is legal
/// requires knowing what every other node is concurrently doing. It is
/// provably not maintainable without coordination (Bailis et al.,
/// "Coordination Avoidance in Database Systems", VLDB 2014: uniqueness is not
/// I-confluent). So a leaderless, always-available cluster cannot both accept
/// writes on every node during a partition *and* guarantee uniqueness.
///
/// Rather than pretend otherwise, the reach is an explicit per-index choice.
///
/// Note that `_id` needs none of this: two nodes inserting the same `_id`
/// collide on the same key and last-writer-wins converges them to a single
/// document, so primary-key uniqueness holds by construction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Enforcement {
    /// Enforced on the node that accepts the write.
    ///
    /// Two nodes can still accept conflicting writes during a partition. Those
    /// violations are **detected after merge and reported**, not prevented —
    /// which is a real limitation, but a visible one rather than silent
    /// corruption. This is the default because it preserves availability.
    #[default]
    Local,
    /// Enforced cluster-wide by reserving the value at the node that owns its
    /// hash before committing.
    ///
    /// Still leaderless — per-key coordination is not a cluster leader — but
    /// writes to this index become unavailable while its owning node is
    /// unreachable. Planned for M4; rejected at index-creation time until then.
    Coordinated,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexField {
    /// Dot path into the document, e.g. `"address.city"`.
    pub path: String,
    #[serde(default)]
    pub descending: bool,
}

impl IndexField {
    pub fn ascending(path: impl Into<String>) -> Self {
        Self { path: path.into(), descending: false }
    }

    pub fn descending(path: impl Into<String>) -> Self {
        Self { path: path.into(), descending: true }
    }
}

impl IndexMeta {
    /// Whether this index holds only some of the collection.
    pub fn is_partial(&self) -> bool {
        self.partial_filter.is_some()
    }

    /// The parsed partial filter, if there is one.
    ///
    /// Parsed on use rather than stored parsed: the document form is what
    /// serialises, and validation already happened at index creation, so a
    /// failure here means stored metadata was tampered with.
    pub fn partial(&self) -> Option<crate::Result<crate::PartialFilter>> {
        self.partial_filter.as_ref().map(crate::PartialFilter::parse)
    }

    /// Whether this index expires documents.
    pub fn is_ttl(&self) -> bool {
        self.expire_after_secs.is_some()
    }

    /// The dot path a TTL index reads its date from.
    ///
    /// Single-field is enforced where an index is created, so a TTL index has
    /// exactly one field and this is it.
    pub fn ttl_path(&self) -> Option<&str> {
        if !self.is_ttl() {
            return None;
        }
        self.fields.first().map(|f| f.path.as_str())
    }

    /// The id of an index called `name`, on every node, without coordination.
    ///
    /// Same reasoning as [`crate::CollectionId::derive`] and the same hash
    /// family, narrowed to 32 bits because that is the width index-entry keys
    /// use. Index names are unique within a collection, so the input is already
    /// the right identity — the id is only a shorter way to write it.
    ///
    /// 32 bits is a much smaller space than the collection id's 64, but the
    /// population is far smaller too: collisions are between indexes *on one
    /// collection*, where a handful is typical. Checked at creation rather than
    /// assumed, since two indexes sharing entries would be unrecoverable.
    pub fn derive_id(name: &str) -> u32 {
        const OFFSET: u32 = 0x811c_9dc5;
        const PRIME: u32 = 0x0100_0193;

        let mut hash = OFFSET;
        for byte in name.as_bytes() {
            hash ^= u32::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        hash
    }

    /// Conventional Mongo-style name, e.g. `age_1_name_-1`.
    pub fn default_name(fields: &[IndexField]) -> String {
        fields
            .iter()
            .map(|f| format!("{}_{}", f.path, if f.descending { -1 } else { 1 }))
            .collect::<Vec<_>>()
            .join("_")
    }

    pub fn paths(&self) -> impl Iterator<Item = &str> {
        self.fields.iter().map(|f| f.path.as_str())
    }
}
