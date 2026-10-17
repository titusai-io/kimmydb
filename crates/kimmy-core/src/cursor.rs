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
//!
//! # Why it is portable between versions
//!
//! Schema 5 changed the stored bytes of every number key (Decimal128 design 7.2),
//! and during a roll a client pages across a 0.50 member and a 0.51 one. So the
//! token carries the frozen **schema-4** form of the key ([`crate::legacy_key`]),
//! for every id that form can represent, and `FF ‖ stored key` for a `Decimal128`
//! id, which has none; and it accepts both. The conversion is made here, at the
//! client boundary, in [`Cursor::decode`] and [`Cursor::encode`], and **never in
//! the executor**: a [`Cursor`] holds the stored key, which is what the scan's
//! bound is, and the executor is also handed stored keys by callers that are not
//! clients (the vector filter's paging). Reading "starts `0x20`" as legacy there
//! would run stored keys through the schema-4 decoder, and some of them decode as
//! another id. That reading is safe at this boundary only because no build
//! issues a stored number key in a token.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use crate::error::{Error, Result};
use crate::keyenc;

/// A position in `_id` order, exclusive: paging resumes *after* it.
///
/// Holds the **stored** key; the token a client sees is its legacy form (module
/// docs).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Cursor(Vec<u8>);

impl Cursor {
    /// Build one from a stored document key.
    pub fn from_key(key: Vec<u8>) -> Self {
        Self(key)
    }

    /// The stored key, as an exclusive lower bound for the next scan.
    pub fn key(&self) -> &[u8] {
        &self.0
    }

    /// The token: the legacy form of the key, so a member of the previous release
    /// can continue it.
    pub fn encode(&self) -> String {
        URL_SAFE_NO_PAD.encode(keyenc::legacy_of_stored(&self.0).as_bytes())
    }

    /// Read a token, legacy (`0x20…`, what every build issues for an integer id)
    /// or marked (`0xFF…`, a `Decimal128` id), into the stored key it names.
    pub fn decode(s: &str) -> Result<Self> {
        let raw = URL_SAFE_NO_PAD.decode(s).map_err(|_| Error::MalformedCursor)?;
        // An empty key would mean "after the beginning", which is just the
        // first page — expressible by sending no cursor at all. Refusing it
        // keeps one way to say one thing.
        let key = keyenc::stored_of_legacy(&raw);
        if key.is_empty() {
            return Err(Error::MalformedCursor);
        }
        Ok(Self(key))
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

    #[test]
    fn a_token_carries_the_legacy_key_and_reads_back_to_the_stored_one() {
        let decimal =
            DocId::try_from_bson(&bson::Bson::Decimal128("1.5".parse().unwrap())).unwrap();
        for id in [
            DocId::Int64(5),
            DocId::Int64(-5),
            DocId::Int64(0x5000_0081_0180_0000),
            DocId::String("abc".into()),
            decimal,
        ] {
            let stored = key_of(&id);
            let token = Cursor::from_key(stored.clone()).encode();
            let raw = URL_SAFE_NO_PAD.decode(&token).unwrap();
            assert_eq!(raw, keyenc::legacy_key(&id).into_bytes(), "{id:?}: issued legacy");
            assert_eq!(Cursor::decode(&token).unwrap().key(), stored, "{id:?}: read stored");
        }
        // A marker with no key behind it says "after the beginning": refused, as empty is.
        assert!(Cursor::decode(&URL_SAFE_NO_PAD.encode([0xFF])).is_err());
    }
}
