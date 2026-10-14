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

use kimmy_core::index_meta::stored_filter;
use kimmy_core::{
    ChunkConfig, Enforcement, IndexField, IndexMeta, Metric, ProviderConfig, VectorConfig,
};

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

/// Domain bytes, so the hash of an index, of a vector configuration and of a
/// list can never be equal to one another by construction.
const INDEX: u8 = b'I';
const VECTOR: u8 = b'V';
const LIST: u8 = b'C';

/// Bytes being framed: every variable-length value is length-prefixed and every
/// option is tagged, so no two different inputs frame alike.
struct Frame(Vec<u8>);

impl Frame {
    fn new(domain: u8) -> Self {
        Self(vec![FORMAT, domain])
    }

    fn bool(&mut self, value: bool) {
        self.0.push(u8::from(value));
    }

    fn tag(&mut self, tag: u8) {
        self.0.push(tag);
    }

    fn u64(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }

    fn str(&mut self, value: &str) {
        self.u64(value.len() as u64);
        self.0.extend_from_slice(value.as_bytes());
    }

    fn opt_str(&mut self, value: Option<&str>) {
        match value {
            None => self.tag(0),
            Some(value) => {
                self.tag(1);
                self.str(value);
            }
        }
    }

    fn opt_usize(&mut self, value: Option<usize>) {
        match value {
            None => self.tag(0),
            Some(value) => {
                self.tag(1);
                self.u64(value as u64);
            }
        }
    }

    fn opt_i64(&mut self, value: Option<i64>) {
        match value {
            None => self.tag(0),
            Some(value) => {
                self.tag(1);
                self.i64(value);
            }
        }
    }

    fn finish(self) -> DefinitionHash {
        xxhash_rust::xxh3::xxh3_128(&self.0)
    }
}

/// The hash of one index definition: its name and everything
/// [`IndexMeta::differences`] compares.
///
/// An exhaustive destructure, so a new field is a compile error here: someone
/// must decide whether it is compared. `id` is derived from the name,
/// `multikey` is a node-local observation of the documents and `created` is the
/// stamp that decides a conflict and is not a term in it, so each is discarded
/// by name.
pub fn index_hash(index: &IndexMeta) -> DefinitionHash {
    let IndexMeta {
        // Derived from `name`, so equal by construction.
        id: _,
        name,
        fields,
        unique,
        enforcement,
        // A node-local observation, not part of the definition.
        multikey: _,
        expire_after_secs,
        partial_filter,
        // The stamp that decides a conflict, not a term in it.
        created: _,
    } = index;
    let mut frame = Frame::new(INDEX);
    frame.str(name);
    frame.u64(fields.len() as u64);
    for IndexField { path, descending } in fields {
        frame.str(path);
        frame.bool(*descending);
    }
    frame.bool(*unique);
    frame.tag(match enforcement {
        Enforcement::Local => 0,
        Enforcement::Coordinated => 1,
    });
    frame.opt_i64(*expire_after_secs);
    // The string the comparison itself uses, verbatim: `compared` sorts the top
    // level and renders canonical Extended JSON, so "empty iff equal" holds,
    // `NaN` included (a string is reflexive where `f64` is not).
    frame.opt_str(stored_filter::compared(partial_filter).as_deref());
    frame.finish()
}

/// The hash of a vector configuration.
///
/// What the configuration says, and not the stamp of its last change: a
/// configuration made before 0.46 sits on its origin with no stamp while a
/// replica that applied the same entry stores one, and nothing would ever heal
/// that.
pub fn vector_hash(config: &VectorConfig) -> DefinitionHash {
    let VectorConfig { fields, provider, dim, metric, chunk, document_prefix, query_prefix } =
        config;
    let mut frame = Frame::new(VECTOR);
    frame.u64(fields.len() as u64);
    for field in fields {
        frame.str(field);
    }
    match provider {
        ProviderConfig::Byo {} => frame.tag(0),
        ProviderConfig::OpenAi { model, endpoint, api_key_env, dimensions } => {
            frame.tag(1);
            frame.str(model);
            frame.opt_str(endpoint.as_deref());
            frame.str(api_key_env);
            frame.opt_usize(*dimensions);
        }
        ProviderConfig::Ollama { model, endpoint } => {
            frame.tag(2);
            frame.str(model);
            frame.str(endpoint);
        }
        ProviderConfig::CustomHttp { endpoint, api_key_env } => {
            frame.tag(3);
            frame.str(endpoint);
            frame.opt_str(api_key_env.as_deref());
        }
        ProviderConfig::Cohere { model, endpoint, api_key_env } => {
            frame.tag(4);
            frame.str(model);
            frame.opt_str(endpoint.as_deref());
            frame.str(api_key_env);
        }
        ProviderConfig::Gemini { model, endpoint, api_key_env } => {
            frame.tag(5);
            frame.str(model);
            frame.opt_str(endpoint.as_deref());
            frame.str(api_key_env);
        }
        ProviderConfig::Local { model } => {
            frame.tag(6);
            frame.str(model);
        }
        ProviderConfig::Profile { name } => {
            frame.tag(7);
            frame.str(name);
        }
    }
    frame.u64(*dim as u64);
    frame.tag(match metric {
        Metric::Cosine => 0,
        Metric::Euclidean => 1,
        Metric::Dot => 2,
    });
    let ChunkConfig { max_chars, overlap, max_tokens } = chunk;
    frame.u64(*max_chars as u64);
    frame.u64(*overlap as u64);
    frame.opt_usize(*max_tokens);
    frame.opt_str(document_prefix.as_deref());
    frame.opt_str(query_prefix.as_deref());
    frame.finish()
}

/// The collection's definitions as `(name, hash)`, indexes sorted by
/// `(derived id, name)` and the vector configuration, if any, last under
/// [`VECTOR_NAME`].
///
/// Sorted by the id and then the name because the id is a 32-bit hash of the
/// name and two names can share one; a tie would otherwise order
/// nondeterministically.
pub fn definition_list(meta: &CollectionMeta) -> Vec<(String, DefinitionHash)> {
    let mut indexes: Vec<&IndexMeta> = meta.indexes.iter().collect();
    indexes.sort_by(|a, b| {
        IndexMeta::derive_id(&a.name)
            .cmp(&IndexMeta::derive_id(&b.name))
            .then_with(|| a.name.cmp(&b.name))
    });
    let mut list: Vec<(String, DefinitionHash)> =
        indexes.into_iter().map(|index| (index.name.clone(), index_hash(index))).collect();
    if let Some(config) = &meta.vector {
        list.push((VECTOR_NAME.to_string(), vector_hash(config)));
    }
    list
}

/// The digest of a collection's definitions: one hash over [`definition_list`].
pub fn definition_digest(meta: &CollectionMeta) -> DefinitionHash {
    digest_of_list(&definition_list(meta))
}

/// The digest of a list of definitions, as [`definition_digest`] hashes it: a
/// requester that holds a peer's list can recompute the peer's digest from it.
pub fn digest_of_list(list: &[(String, DefinitionHash)]) -> DefinitionHash {
    let mut frame = Frame::new(LIST);
    frame.u64(list.len() as u64);
    for (name, hash) in list {
        frame.str(name);
        frame.0.extend_from_slice(&hash.to_be_bytes());
    }
    frame.finish()
}
