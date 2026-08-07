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

// Keys within the META table.
pub const META_NODE_ID: &str = "node_id";
pub const META_FORMAT_VERSION: &str = "format_version";
pub const META_NEXT_COLLECTION_ID: &str = "next_collection_id";
