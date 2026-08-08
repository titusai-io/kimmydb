//! Identifier types.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Stable identity of a node in the cluster.
///
/// Generated once on first start and persisted, so a restarting node keeps its
/// identity — losing it would make the node a stranger to its own prior writes
/// and break last-writer-wins tiebreaks.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(Uuid);

impl NodeId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Node ids appear in nearly every log line; the full UUID is noise.
        write!(f, "Node({:.8})", self.0.simple())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for NodeId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

/// Internal handle for a collection.
///
/// Collections are addressed by `(database, name)` at the API edge, but keyed
/// by this dense integer on disk so that renames stay cheap and index keys stay
/// short.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct CollectionId(pub u64);

impl CollectionId {
    /// The id of `db.name`, on every node, without coordination.
    ///
    /// Derived rather than allocated. A counter is node-local, so two nodes
    /// that create the same collection in a different order end up with
    /// different ids for it — and since every oplog entry names its collection
    /// by id, a replicated write would then be applied to whichever collection
    /// happened to hold that number locally. Silently, and only for peers whose
    /// creation order differed, which is the worst way for it to fail.
    ///
    /// Deriving the id from the name removes the problem rather than
    /// coordinating it away: no agreement round, no leader, and a node that has
    /// never met a peer computes the same answer.
    ///
    /// # Why FNV-1a
    ///
    /// The hash has to be **stable forever** — it is baked into every on-disk
    /// key. `DefaultHasher` is explicitly not guaranteed stable across Rust
    /// releases, so an upgrade could silently repoint every collection. FNV-1a
    /// is a handful of lines, has no dependency, and is fully specified.
    ///
    /// Collisions are checked at creation time rather than trusted; see
    /// `Engine::create_collection`.
    pub fn derive(db: &str, name: &str) -> Self {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;

        let mut hash = OFFSET;
        // The NUL separator keeps `("a", "bc")` and `("ab", "c")` distinct.
        // Without it they hash identically, which would merge two collections.
        for byte in db.as_bytes().iter().chain(b"\0").chain(name.as_bytes()) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }

        // Zero is reserved so that an uninitialised or defaulted id cannot
        // silently address a real collection.
        Self(if hash == 0 { 1 } else { hash })
    }

    pub const fn to_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

impl fmt::Display for CollectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "coll#{}", self.0)
    }
}

/// A document's `_id`.
///
/// Mongo permits any BSON type here, and so do we — but the value must be
/// encodable as an order-preserving key, which rules out documents and arrays.
/// [`DocId::try_from_bson`] enforces that.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub enum DocId {
    ObjectId(bson::oid::ObjectId),
    String(String),
    Int64(i64),
    Binary(Vec<u8>),
    Uuid(Uuid),
}

impl DocId {
    /// Mint a fresh identifier. Defaults to ObjectId for Mongo familiarity.
    pub fn generate() -> Self {
        Self::ObjectId(bson::oid::ObjectId::new())
    }

    /// Convert from a user-supplied `_id` value, rejecting types that cannot
    /// serve as a primary key.
    pub fn try_from_bson(value: &bson::Bson) -> crate::Result<Self> {
        match value {
            bson::Bson::ObjectId(oid) => Ok(Self::ObjectId(*oid)),
            bson::Bson::String(s) => Ok(Self::String(s.clone())),
            bson::Bson::Int32(i) => Ok(Self::Int64(i64::from(*i))),
            bson::Bson::Int64(i) => Ok(Self::Int64(*i)),
            bson::Bson::Binary(b) => Ok(Self::Binary(b.bytes.clone())),
            other => Err(crate::Error::InvalidDocumentId {
                found: format!("{:?}", other.element_type()),
            }),
        }
    }

    pub fn to_bson(&self) -> bson::Bson {
        match self {
            Self::ObjectId(oid) => bson::Bson::ObjectId(*oid),
            Self::String(s) => bson::Bson::String(s.clone()),
            Self::Int64(i) => bson::Bson::Int64(*i),
            Self::Binary(b) => bson::Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: b.clone(),
            }),
            Self::Uuid(u) => bson::Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Uuid,
                bytes: u.as_bytes().to_vec(),
            }),
        }
    }
}

impl fmt::Display for DocId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ObjectId(oid) => write!(f, "{oid}"),
            Self::String(s) => write!(f, "{s}"),
            Self::Int64(i) => write!(f, "{i}"),
            Self::Binary(b) => write!(f, "bin({} bytes)", b.len()),
            Self::Uuid(u) => write!(f, "{u}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doc_id_rejects_unkeyable_types() {
        let doc = bson::Bson::Document(bson::doc! { "nested": 1 });
        assert!(DocId::try_from_bson(&doc).is_err());
        let arr = bson::Bson::Array(vec![bson::Bson::Int32(1)]);
        assert!(DocId::try_from_bson(&arr).is_err());
    }

    #[test]
    fn doc_id_normalizes_int32_to_int64() {
        let id = DocId::try_from_bson(&bson::Bson::Int32(7)).unwrap();
        assert_eq!(id, DocId::Int64(7));
        // Otherwise `{_id: 7}` inserted as int32 and looked up as int64 would miss.
        assert_eq!(DocId::try_from_bson(&bson::Bson::Int64(7)).unwrap(), id);
    }

    #[test]
    fn node_id_round_trips_through_bytes() {
        let id = NodeId::generate();
        assert_eq!(NodeId::from_bytes(id.to_bytes()), id);
    }
    #[test]
    fn a_collection_id_is_the_same_on_every_node() {
        // The whole point: no coordination, no creation-order dependence.
        assert_eq!(CollectionId::derive("shop", "orders"), CollectionId::derive("shop", "orders"));
    }

    #[test]
    fn different_collections_get_different_ids() {
        let ids = [
            CollectionId::derive("shop", "orders"),
            CollectionId::derive("shop", "customers"),
            CollectionId::derive("hr", "orders"),
            CollectionId::derive("", "orders"),
        ];
        for (i, a) in ids.iter().enumerate() {
            for b in &ids[i + 1..] {
                assert_ne!(a, b, "distinct collections must not share an id");
            }
        }
    }

    #[test]
    fn the_separator_keeps_split_points_distinct() {
        // Without a separator, ("a","bc") and ("ab","c") hash the same input
        // and two unrelated collections would silently become one.
        assert_ne!(CollectionId::derive("a", "bc"), CollectionId::derive("ab", "c"));
        assert_ne!(CollectionId::derive("ab", ""), CollectionId::derive("a", "b"));
    }

    #[test]
    fn zero_is_never_produced() {
        // Reserved, so a defaulted id cannot address a real collection.
        assert_ne!(CollectionId::derive("", "").0, 0);
    }

    #[test]
    fn ids_are_pinned_to_exact_values() {
        // These are on-disk keys. If this test fails, every existing database
        // has just been repointed — the hash is not free to change, and a
        // "harmless refactor" of `derive` is not harmless.
        //
        // Cross-checked against an independent FNV-1a implementation rather
        // than recorded from this one, so the test would also catch this
        // implementation being wrong in a self-consistent way.
        assert_eq!(CollectionId::derive("shop", "orders").0, 0x53ad_8d42_0376_3a3a);
        assert_eq!(CollectionId::derive("__kimmy", "__users").0, 0x1f65_9d82_1893_00f2);
        assert_eq!(CollectionId::derive("a", "bc").0, 0xab40_f682_0d40_b523);
        assert_eq!(CollectionId::derive("ab", "c").0, 0xfd61_c083_ef20_0867);
    }
}
