//! Each collection's cut digest, kept (B3, half 1).
//!
//! The divergence check's digest half compares what two members hold for a
//! collection, by `(_id, stamp)`, at a point both have processed. Walking a
//! collection for that would read every page of it on every contact, as the
//! count half once did (ADR-174), so the digest is kept the way the count is:
//! one row per collection, moved in the transaction of every write to `DOCS`,
//! through the same two functions ([`crate::live_count::put_record`] and
//! [`crate::live_count::remove_record`]) and written once per transaction by
//! [`crate::live_count::flush`].
//!
//! **The row.** `digest` is the XOR of [`term`] over the collection's *live*
//! records, where the term of a record is `xxh3_128(key || stamp)`. A tombstone
//! contributes nothing, so two members that collected a tombstone at different
//! times still agree. `maxw` is the highest *record* stamp per origin, live or
//! tombstone, and never falls: a replace or a remove does not lower it. It is
//! what says whether a collection is quiet at a cut. `bare_max` is the highest
//! stamp per origin of a tombstone no oplog entry stands behind (ADR-228); a
//! probe defers while one is above the cut.
//!
//! **XOR, so a delta composes.** A write moves the row by the term it removes
//! and the term it adds, and a correction can be applied to a row that has
//! moved since it was computed. Both are the design's reason for XOR.
//!
//! **Its own mark.** [`tables::LIVE_DIGESTS_THROUGH`] holds the same bytes as
//! the counts' mark behind a format number, written and carried with it. A
//! shared mark would let a build that keeps the counts and not the digests keep
//! it current.

use std::collections::BTreeMap;

use kimmy_core::{Hlc, NodeId, Stamp};
use redb::{ReadableTable, WriteTransaction};

use crate::error::{Result, StorageError};
use crate::tables;

/// The digest format: what is hashed, and how a row is laid out. A build that
/// changes either changes this, which moves the mark, so rows written under
/// the old format are rebuilt rather than trusted.
pub const FORMAT: u8 = 1;

/// The key of the digest mark in [`tables::LIVE_DIGESTS_THROUGH`].
pub(crate) const THROUGH: &str = "arrival";

/// The key of the open walk's cursor in [`tables::LIVE_DIGESTS_THROUGH`].
pub(crate) const WALK: &str = "walk";

/// The row layout's own version.
const ROW_VERSION: u8 = 1;

/// A collection's cut digest.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DigestRow {
    /// The XOR of [`term`] over the live records.
    pub digest: u128,
    /// The highest record stamp per origin, live or tombstone. Never falls.
    pub maxw: BTreeMap<NodeId, Hlc>,
    /// The highest stamp per origin of a tombstone written with no oplog entry
    /// behind it.
    pub bare_max: BTreeMap<NodeId, Hlc>,
}

impl DigestRow {
    /// Fold one record in or out of the digest.
    pub fn flip(&mut self, key: &[u8], stamp: &Stamp) {
        self.digest ^= term(key, stamp);
    }

    /// Raise `maxw` to `stamp`, never lowering it.
    pub fn raise_maxw(&mut self, stamp: &Stamp) {
        raise(&mut self.maxw, stamp);
    }

    /// Raise `bare_max` to `stamp`, never lowering it.
    pub fn raise_bare(&mut self, stamp: &Stamp) {
        raise(&mut self.bare_max, stamp);
    }

    /// Merge `other`'s highest stamps into this row's.
    fn raise_from(&mut self, other: &DigestRow) {
        for (node, hlc) in &other.maxw {
            raise(&mut self.maxw, &Stamp::new(*hlc, *node));
        }
        for (node, hlc) in &other.bare_max {
            raise(&mut self.bare_max, &Stamp::new(*hlc, *node));
        }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 16 + 2 + 26 * (self.maxw.len() + self.bare_max.len()));
        out.push(ROW_VERSION);
        out.extend_from_slice(&self.digest.to_be_bytes());
        for map in [&self.maxw, &self.bare_max] {
            out.extend_from_slice(&(map.len() as u16).to_be_bytes());
            for (node, hlc) in map {
                out.extend_from_slice(&node.to_bytes());
                out.extend_from_slice(&hlc.to_bytes());
            }
        }
        out
    }

    pub(crate) fn decode(mut input: &[u8]) -> Result<Self> {
        let version = take(&mut input, 1)?[0];
        if version != ROW_VERSION {
            return Err(StorageError::UnsupportedFormat { found: version, expected: ROW_VERSION });
        }
        let digest = u128::from_be_bytes(take(&mut input, 16)?.try_into().expect("16 bytes"));
        let mut maps = [BTreeMap::new(), BTreeMap::new()];
        for map in &mut maps {
            let n = u16::from_be_bytes(take(&mut input, 2)?.try_into().expect("2 bytes"));
            for _ in 0..n {
                let node = NodeId::from_bytes(take(&mut input, 16)?.try_into().expect("16 bytes"));
                let hlc = Hlc::from_bytes(take(&mut input, 10)?.try_into().expect("10 bytes"));
                map.insert(node, hlc);
            }
        }
        if !input.is_empty() {
            return Err(StorageError::Corrupt("digest row has trailing bytes".into()));
        }
        let [maxw, bare_max] = maps;
        Ok(Self { digest, maxw, bare_max })
    }
}

fn take<'a>(input: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if input.len() < n {
        return Err(StorageError::Corrupt("digest row ended early".into()));
    }
    let (head, tail) = input.split_at(n);
    *input = tail;
    Ok(head)
}

fn raise(map: &mut BTreeMap<NodeId, Hlc>, stamp: &Stamp) {
    let slot = map.entry(stamp.node).or_insert(Hlc::ZERO);
    if stamp.hlc > *slot {
        *slot = stamp.hlc;
    }
}

/// The term one live record contributes to its collection's digest:
/// `xxh3_128(key || stamp)`. The stamp is fixed width and last, so the pair
/// encodes unambiguously.
pub fn term(key: &[u8], stamp: &Stamp) -> u128 {
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    hasher.update(key);
    hasher.update(&stamp.hlc.to_bytes());
    hasher.update(&stamp.node.to_bytes());
    hasher.digest128()
}

/// What one transaction owes one collection's row, gathered as it writes and
/// applied once by [`flush`].
#[derive(Default)]
pub(crate) struct Delta {
    /// The XOR of the terms removed and added.
    xor: u128,
    /// The highest record stamps this transaction wrote.
    raised: DigestRow,
}

impl Delta {
    /// One write: the record that was there (if any) and the one now.
    pub(crate) fn wrote(
        &mut self,
        key: &[u8],
        before: Option<(Stamp, bool)>,
        after: Option<(Stamp, bool)>,
    ) {
        if let Some((stamp, true)) = before {
            self.xor ^= term(key, &stamp);
        }
        if let Some((stamp, live)) = after {
            if live {
                self.xor ^= term(key, &stamp);
            }
            self.raised.raise_maxw(&stamp);
        }
    }

    /// The write just recorded was a tombstone with no oplog entry behind it.
    pub(crate) fn bare(&mut self, stamp: &Stamp) {
        self.raised.raise_bare(stamp);
    }

    fn moves_nothing(&self) -> bool {
        self.xor == 0 && self.raised.maxw.is_empty() && self.raised.bare_max.is_empty()
    }
}

/// Apply what a transaction gathered, in that transaction. Each collection's
/// row is read, moved and written once however many writes it held.
pub(crate) fn flush(
    txn: &WriteTransaction,
    deltas: &std::collections::BTreeMap<u64, Delta>,
) -> Result<()> {
    let deltas: Vec<_> = deltas.iter().filter(|(_, d)| !d.moves_nothing()).collect();
    if deltas.is_empty() {
        return Ok(());
    }
    let mut rows = txn.open_table(tables::LIVE_DIGESTS)?;
    for (collection, delta) in deltas {
        let current = match rows.get(*collection)? {
            Some(raw) => Some(DigestRow::decode(raw.value())?),
            None => None,
        };
        let mut row = current.clone().unwrap_or_default();
        row.digest ^= delta.xor;
        row.raise_from(&delta.raised);
        if current.as_ref() != Some(&row) {
            rows.insert(*collection, row.encode().as_slice())?;
        }
    }
    Ok(())
}

/// The digest mark's bytes for a counts mark: the format number, then the
/// same bytes.
pub(crate) fn mark_bytes(mark: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + mark.len());
    out.push(FORMAT);
    out.extend_from_slice(mark);
    out
}

/// Record, in the transaction of the write that moved it, that the digests are
/// kept through `mark` (the counts' mark bytes).
pub(crate) fn mark_through(txn: &WriteTransaction, mark: &[u8]) -> Result<()> {
    txn.open_table(tables::LIVE_DIGESTS_THROUGH)?.insert(THROUGH, mark_bytes(mark).as_slice())?;
    Ok(())
}

/// Carry the digest mark from `before` to `after` across a removal that
/// appends nothing, **only if it matched `before`** (the counts' rule).
pub(crate) fn carry_mark(txn: &WriteTransaction, before: &[u8], after: &[u8]) -> Result<()> {
    let mut table = txn.open_table(tables::LIVE_DIGESTS_THROUGH)?;
    let current = table.get(THROUGH)?.is_some_and(|stored| stored.value() == mark_bytes(before));
    if current {
        table.insert(THROUGH, mark_bytes(after).as_slice())?;
    }
    Ok(())
}

/// Whether the kept digest mark matches `mark` (the counts' mark bytes now).
pub(crate) fn mark_matches(
    through: &impl ReadableTable<&'static str, &'static [u8]>,
    mark: &[u8],
) -> Result<bool> {
    Ok(through.get(THROUGH)?.is_some_and(|stored| stored.value() == mark_bytes(mark)))
}

/// A collection created again under an id a dropped one held starts with no
/// row.
pub(crate) fn reset(txn: &WriteTransaction, collection: kimmy_core::CollectionId) -> Result<()> {
    txn.open_table(tables::LIVE_DIGESTS)?.remove(collection.0)?;
    Ok(())
}

/// The row of `collection` as kept, in a read transaction.
pub(crate) fn kept_row(
    rows: &impl ReadableTable<u64, &'static [u8]>,
    collection: u64,
) -> Result<Option<DigestRow>> {
    match rows.get(collection)? {
        Some(raw) => Ok(Some(DigestRow::decode(raw.value())?)),
        None => Ok(None),
    }
}

/// Fold a record found by a walk into the rows being rebuilt. A tombstone
/// raises `bare_max` too: the walk sees headers, not whether an oplog entry
/// stands behind a tombstone, so the rebuilt value is the highest tombstone
/// stamp per origin. That defers longer after a rebuild than a maintained row
/// would, never less, and ages out as the cut rises (B3 design 2.1).
pub(crate) fn fold_walked(
    rows: &mut BTreeMap<u64, DigestRow>,
    collection: u64,
    key: &[u8],
    stamp: &Stamp,
    live: bool,
) {
    let row = rows.entry(collection).or_default();
    if live {
        row.flip(key, stamp);
    } else {
        row.raise_bare(stamp);
    }
    row.raise_maxw(stamp);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamp(wall: u64, node: u8) -> Stamp {
        Stamp::new(Hlc::new(wall, 0), NodeId::from_bytes([node; 16]))
    }

    #[test]
    fn a_row_round_trips() {
        let mut row = DigestRow::default();
        row.flip(b"a", &stamp(5, 1));
        row.raise_maxw(&stamp(5, 1));
        row.raise_maxw(&stamp(9, 2));
        row.raise_bare(&stamp(7, 2));
        let back = DigestRow::decode(&row.encode()).unwrap();
        assert_eq!(back, row);
        assert!(DigestRow::decode(&[]).is_err());
        let mut longer = row.encode();
        longer.push(0);
        assert!(DigestRow::decode(&longer).is_err(), "trailing bytes are refused");
        let mut other = row.encode();
        other[0] = 9;
        assert!(DigestRow::decode(&other).is_err(), "an unknown row version is refused");
    }

    #[test]
    fn the_term_depends_on_the_key_and_the_whole_stamp() {
        let base = term(b"k", &stamp(5, 1));
        assert_ne!(base, term(b"j", &stamp(5, 1)));
        assert_ne!(base, term(b"k", &stamp(6, 1)));
        assert_ne!(base, term(b"k", &stamp(5, 2)));
        assert_ne!(base, term(b"k", &Stamp::new(Hlc::new(5, 1), NodeId::from_bytes([1; 16]))));
        assert_eq!(base, term(b"k", &stamp(5, 1)));
        // The stamp is fixed width and last: moving a byte between the key
        // and the stamp cannot give the same input.
        assert_ne!(term(b"ab", &stamp(5, 1)), term(b"a", &stamp(5, 1)));
    }

    #[test]
    fn a_term_folds_in_and_out() {
        let mut row = DigestRow::default();
        row.flip(b"a", &stamp(1, 1));
        row.flip(b"b", &stamp(2, 1));
        let both = row.digest;
        row.flip(b"a", &stamp(1, 1));
        assert_eq!(row.digest, term(b"b", &stamp(2, 1)));
        row.flip(b"b", &stamp(2, 1));
        assert_eq!(row.digest, 0);
        assert_ne!(both, 0);
    }

    #[test]
    fn the_highest_stamps_only_rise() {
        let mut row = DigestRow::default();
        row.raise_maxw(&stamp(9, 1));
        row.raise_maxw(&stamp(3, 1));
        assert_eq!(row.maxw[&NodeId::from_bytes([1; 16])], Hlc::new(9, 0));
    }

    #[test]
    fn a_delta_that_nets_to_nothing_moves_no_row() {
        let mut delta = Delta::default();
        delta.wrote(b"k", None, Some((stamp(1, 1), true)));
        assert!(!delta.moves_nothing());
        let mut delta = Delta::default();
        // Written live and removed again inside one transaction: the digest
        // nets to nothing, but the stamp was still seen.
        delta.wrote(b"k", None, Some((stamp(1, 1), true)));
        delta.wrote(b"k", Some((stamp(1, 1), true)), None);
        assert_eq!(delta.xor, 0);
        assert!(!delta.moves_nothing(), "maxw rose");
    }
}
