//! Index definitions.
//!
//! Lives in `kimmy-core` because both `kimmy-storage` (which maintains index
//! entries) and `kimmy-query` (which plans against them) need this shape, and
//! neither should depend on the other.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexMeta {
    pub id: u32,
    pub name: String,
    pub fields: Vec<IndexField>,
    #[serde(default)]
    pub unique: bool,
    /// How `unique` is enforced once the node has peers. Ignored when
    /// `unique` is false.
    #[serde(default)]
    pub enforcement: Enforcement,
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
