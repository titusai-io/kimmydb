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
}
