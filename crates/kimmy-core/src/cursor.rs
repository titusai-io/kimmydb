//! An opaque continuation token for paging a collection.
//!
//! # What it is, and why that is enough
//!
//! The encoded document key of the last row of a page. Nothing else.
//!
//! That works because [`crate::keyenc`] is **order-preserving**: the byte order
//! of two encoded keys is the canonical BSON order of the `_id`s they came
//! from. So "the next page" is "keys strictly greater than these bytes", which
//! is a range bound the storage engine already knows how to take — and the
//! documents table and every index candidate list are already in that order.
//! No new comparison logic, no sorting, no state.
//!
//! # Why it is opaque
//!
//! The same reasoning as [`crate::ResumeToken`], whose convention this follows
//! down to the base64url alphabet: a client treats it as a blob, so the
//! encoding can change without breaking anyone. `keyenc` is deliberately
//! one-way, which makes opacity honest rather than a request.
//!
//! # Why it is portable between nodes
//!
//! It carries no server state — no cursor id, no session, nothing a particular
//! node holds. Two nodes that hold the same document agree on its key, because
//! the encoding is a pure function of the `_id`. So a page fetched from one
//! node continues correctly on another, which matters because a client fails
//! over between nodes and may finish a walk somewhere it did not start
//! ([ADR-055](../../../docs/decisions.md)).
//! Change-stream resume tokens already have this property and it has been
//! verified on a real cluster; this inherits it by construction rather than by
//! arrangement.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use crate::error::{Error, Result};

/// A position in `_id` order, exclusive: paging resumes *after* it.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Cursor(Vec<u8>);

impl Cursor {
    /// Build one from an already-encoded document key.
    pub fn from_key(key: Vec<u8>) -> Self {
        Self(key)
    }

    /// The encoded key, as an exclusive lower bound for the next scan.
    pub fn key(&self) -> &[u8] {
        &self.0
    }

    pub fn encode(&self) -> String {
        URL_SAFE_NO_PAD.encode(&self.0)
    }

    pub fn decode(s: &str) -> Result<Self> {
        let raw = URL_SAFE_NO_PAD.decode(s).map_err(|_| Error::MalformedCursor)?;
        // An empty key would mean "after the beginning", which is just the
        // first page — expressible by sending no cursor at all. Refusing it
        // keeps one way to say one thing.
        if raw.is_empty() {
            return Err(Error::MalformedCursor);
        }
        Ok(Self(raw))
    }
}

impl std::fmt::Display for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.encode())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DocId, keyenc};

    fn key_of(id: &DocId) -> Vec<u8> {
        keyenc::encode(&id.to_bson()).unwrap()
    }

    #[test]
    fn a_cursor_round_trips() {
        for id in [
            DocId::Int64(42),
            DocId::String("abc".into()),
            DocId::ObjectId(bson::oid::ObjectId::new()),
        ] {
            let c = Cursor::from_key(key_of(&id));
            assert_eq!(Cursor::decode(&c.encode()).unwrap(), c);
        }
    }

    #[test]
    fn a_malformed_cursor_is_refused() {
        assert!(Cursor::decode("not base64 !!").is_err());
        assert!(Cursor::decode("").is_err(), "empty means the first page; say that by omitting it");
    }

    #[test]
    fn cursor_order_is_id_order() {
        // The property the whole design rests on: comparing tokens as bytes
        // has to agree with comparing the `_id`s they came from, or paging
        // skips or repeats documents.
        let mut ids: Vec<DocId> = (0..200i64).map(DocId::Int64).collect();
        ids.extend((0..50).map(|n| DocId::String(format!("s{n:03}"))));

        let mut by_id = ids.clone();
        by_id.sort_by(|a, b| crate::canonical_cmp(&a.to_bson(), &b.to_bson()));

        // Compared on the key bytes, which is what the scan bound compares.
        // Base64 of a length-varying byte string is *not* order-preserving,
        // so the token text deliberately is not what gets compared anywhere.
        let mut by_key = ids;
        by_key.sort_by_key(key_of);
        assert_eq!(by_key, by_id, "encoded key order must be _id order");
    }

    #[test]
    fn a_cursor_is_a_pure_function_of_the_id() {
        // What makes it portable between nodes: no node identity, no session,
        // nothing local. The same document yields the same token anywhere.
        let id = DocId::String("shared".into());
        assert_eq!(Cursor::from_key(key_of(&id)), Cursor::from_key(key_of(&id)));
    }
}
