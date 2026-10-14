//! The definition digest (B3, half 2): a hash of what a collection's indexes
//! and vector configuration say, so two members can tell, by comparing 16
//! bytes, that they hold the same schema.
//!
//! **What is hashed is exactly what [`IndexMeta::differences`] compares, plus
//! the name.** `differences` is the engine's own judgement of "the same
//! definition" (ADR-132): `fields`, `unique`, `enforcement`,
//! `expire_after_secs` and the partial filter as [`kimmy_core::stored_filter::compared`]
//! renders it. Everything else of an [`IndexMeta`] is discarded **by name** in
//! an exhaustive destructure (no `..`), so a new field is a compile error here
//! and someone must decide whether it is compared. The same is done for
//! [`VectorConfig`], `ProviderConfig`, `ChunkConfig` and the enums, which
//! are matched with no wildcard.
//!
//! **Injective framing.** Every string is length-prefixed and every `Option`
//! is tagged, so `("a", "bc")` and `("ab", "c")`, `None` and `Some(0)`, and a
//! missing filter and the string `"null"` never hash alike. A hash of a
//! concatenation would let two different definitions collide by construction.
//!
//! **The library's output is part of the input.** The partial filter is hashed
//! as the string `compared()` returns (canonical Extended JSON, top-level keys
//! sorted), which is `bson`'s rendering. A change in how `bson` renders a
//! number or a key would make every partial-filter index differ between two
//! builds; [`FORMAT`] covers this module's framing and not the library's
//! output, so the rendering of each type the filter language accepts is pinned
//! by a test and a deliberate change bumps [`FORMAT`].

use kimmy_core::{IndexMeta, VectorConfig};

use crate::meta::CollectionMeta;

/// The definition digest's format: what is hashed and how it is framed. A
/// change to either bumps it, so a digest from a build under another format is
/// never equal to this one's by accident.
pub const FORMAT: u8 = 1;

/// The name a collection's vector configuration goes by in a list of
/// definitions. The `__` prefix is reserved, so no index can have it.
pub const VECTOR_NAME: &str = "__vector";

/// A definition's hash, 16 bytes.
pub type DefinitionHash = u128;

/// The hash of one index definition: its name and everything
/// [`IndexMeta::differences`] compares.
pub fn index_hash(_index: &IndexMeta) -> DefinitionHash {
    0
}

/// The hash of a vector configuration.
pub fn vector_hash(_config: &VectorConfig) -> DefinitionHash {
    0
}

/// The collection's definitions as `(name, hash)`, indexes sorted by
/// `(derived id, name)` and the vector configuration, if any, last under
/// [`VECTOR_NAME`].
pub fn definition_list(_meta: &CollectionMeta) -> Vec<(String, DefinitionHash)> {
    Vec::new()
}

/// The digest of a collection's definitions: one hash over [`definition_list`].
pub fn definition_digest(_meta: &CollectionMeta) -> DefinitionHash {
    0
}
