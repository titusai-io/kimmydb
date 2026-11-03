//! Canonical bytes, the state hash and the whole-state round trip.
//!
//! The encoding is total and deterministic: every map is written in key order
//! (the `BTreeMap` order, itself total), every length is a big-endian `u64`,
//! integers are fixed width big-endian, doubles are their IEEE bit pattern
//! (`to_bits`, big-endian). It is the binary form of the model's `canon()`.
//!
//! Stability: the bytes of [`MergeState::canonical_bytes`], of
//! [`MergeState::encode`] and the [`MergeState::state_hash`] do not depend on
//! the architecture, the word size, `std::hash`, hash-map order or float
//! formatting. Every multi-byte integer is BIG-ENDIAN (`to_be_bytes`), no
//! `usize` is written (lengths are `u64`), the hash mix reads its input as
//! big-endian `u64` words.
//!
//! Canonical: [`MergeState::decode`] accepts only what [`MergeState::encode`] can write, so
//! `encode(decode(b)) == b` for every accepted `b`: map and set entries must be strictly
//! increasing in key order (no duplicates), and a flag byte is 0 or 1.
//!
//! Versions: [`MergeState::encode`] starts with the 3 bytes `QMS` and ONE format
//! version byte (currently 1), then the canonical body.
//! [`MergeState::canonical_bytes`] is the body alone and carries no tag; the
//! hash covers the body. Mutant memory (`Gen::vv`, `MergeState::gens`) is in neither. The golden vectors in `tests.rs` pin all three: changing
//! the bytes is a format change (a wipe release) and must be deliberate.

use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use super::state::{Gen, Life, LifeKind, MAX_TOTAL, MergeState, ObsKey, Reg, Slot, Vv};
use super::types::{Epoch, LossKey, LossKind, LossValue, MStamp, Value, WriterId};
use crate::hlc::Hlc;
use crate::ids::NodeId;

const MAGIC: [u8; 3] = *b"QMS";
const VERSION: u8 = 1;

/// The deepest nesting of arrays inside a value that `decode` accepts (the BSON depth limit).
pub(super) const MAX_VALUE_DEPTH: usize = 100;

/// A failure to decode an encoded state.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("merge state: truncated input")]
    Truncated,
    #[error("merge state: bad header")]
    BadHeader,
    #[error("merge state: invalid tag {0}")]
    BadTag(u8),
    #[error("merge state: invalid utf-8 string")]
    BadString,
    #[error("merge state: {0} trailing bytes")]
    Trailing(usize),
    #[error("merge state: a counter total is out of range")]
    TotalOutOfRange,
    #[error("merge state: a value is nested too deep")]
    TooDeep,
    #[error("merge state: not canonical (map keys duplicated or out of order)")]
    NotCanonical,
}

// ---- writing ---------------------------------------------------------------

struct W(Vec<u8>);

impl W {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn len(&mut self, n: usize) {
        self.0.extend_from_slice(&(n as u64).to_be_bytes());
    }
    fn i64(&mut self, v: i64) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    fn i128(&mut self, v: i128) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    fn str(&mut self, s: &str) {
        self.len(s.len());
        self.0.extend_from_slice(s.as_bytes());
    }
    fn hlc(&mut self, h: Hlc) {
        self.0.extend_from_slice(&h.to_bytes());
    }
    fn wid(&mut self, w: &WriterId) {
        self.0.extend_from_slice(&w.node.to_bytes());
        self.0.extend_from_slice(&w.epoch.0);
    }
    fn stamp(&mut self, s: &MStamp) {
        self.hlc(s.hlc);
        self.wid(&s.writer);
    }
    fn opt_stamp(&mut self, s: &Option<MStamp>) {
        match s {
            None => self.u8(0),
            Some(s) => {
                self.u8(1);
                self.stamp(s);
            }
        }
    }
    fn value(&mut self, v: &Value) {
        match v {
            Value::Null => self.u8(0),
            Value::Int64(i) => {
                self.u8(1);
                self.i64(*i);
            }
            Value::Double(d) => {
                self.u8(2);
                self.0.extend_from_slice(&d.to_bits().to_be_bytes());
            }
            Value::String(s) => {
                self.u8(3);
                self.str(s);
            }
            // 4 is the array; 5 and 6 are kept for the map markers.
            Value::Array(a) => {
                self.u8(4);
                self.len(a.len());
                for x in a {
                    self.value(x);
                }
            }
            Value::Int32(i) => {
                self.u8(7);
                self.0.extend_from_slice(&i.to_be_bytes());
            }
            Value::Decimal128(d) => {
                self.u8(8);
                self.0.extend_from_slice(d);
            }
            Value::Bool(b) => {
                self.u8(9);
                self.u8(u8::from(*b));
            }
            Value::Date(ms) => {
                self.u8(10);
                self.i64(*ms);
            }
            Value::ObjectId(o) => {
                self.u8(11);
                self.0.extend_from_slice(o);
            }
            Value::Binary { subtype, bytes } => {
                self.u8(12);
                self.u8(*subtype);
                self.len(bytes.len());
                self.0.extend_from_slice(bytes);
            }
            Value::Timestamp { time, increment } => {
                self.u8(13);
                self.0.extend_from_slice(&time.to_be_bytes());
                self.0.extend_from_slice(&increment.to_be_bytes());
            }
            Value::Regex { pattern, options } => {
                self.u8(14);
                self.str(pattern);
                self.str(options);
            }
        }
    }
    fn vv(&mut self, v: &Vv) {
        self.len(v.len());
        for (w, h) in v {
            self.wid(w);
            self.hlc(*h);
        }
    }
    fn obs(&mut self, m: &BTreeMap<ObsKey, i128>) {
        self.len(m.len());
        for ((l, w, g), t) in m {
            self.stamp(l);
            self.wid(w);
            self.opt_stamp(g);
            self.i128(*t);
        }
    }
    fn obs_stamped(&mut self, m: &BTreeMap<ObsKey, (MStamp, i128)>) {
        self.len(m.len());
        for ((l, w, g), (s, t)) in m {
            self.stamp(l);
            self.wid(w);
            self.opt_stamp(g);
            self.stamp(s);
            self.i128(*t);
        }
    }
    fn gen_(&mut self, g: &Option<Gen>) {
        match g {
            None => self.u8(0),
            Some(g) => {
                self.u8(1);
                self.stamp(&g.stamp);
                self.obs(&g.seen);
            }
        }
    }
    fn loss_key(&mut self, k: &LossKey) {
        self.u8(match k.kind {
            LossKind::Clash => 0,
            LossKind::DeleteWon => 1,
            LossKind::InsertLost => 2,
            LossKind::RetiredWriter => 3,
        });
        self.str(&k.path);
        self.stamp(&k.stamp);
    }
    fn loss_value(&mut self, v: &LossValue) {
        match v {
            LossValue::Value(v) => {
                self.u8(0);
                self.value(v);
            }
            LossValue::Total(t) => {
                self.u8(1);
                self.i128(*t);
            }
            LossValue::Fields(f) => {
                self.u8(2);
                self.len(f.len());
                for p in f {
                    self.str(p);
                }
            }
        }
    }
}

impl MergeState {
    fn write_body(&self, w: &mut W) {
        w.len(self.lives.len());
        for (l, v) in &self.lives {
            w.stamp(l);
            w.len(v.seen.len());
            for d in &v.seen {
                w.stamp(d);
            }
            w.len(v.order.len());
            for p in &v.order {
                w.str(p);
            }
            w.u8(match v.kind {
                LifeKind::Insert => 0,
                LifeKind::Upsert => 1,
            });
        }
        w.len(self.dead.len());
        for l in &self.dead {
            w.stamp(l);
        }
        w.len(self.deletes.len());
        for (d, vv) in &self.deletes {
            w.stamp(d);
            w.vv(vv);
        }
        w.len(self.regs.len());
        for ((l, p), r) in &self.regs {
            w.stamp(l);
            w.str(p);
            w.stamp(&r.stamp);
            w.value(&r.value);
            w.obs_stamped(&r.obs);
            w.gen_(&r.generation);
            w.u8(u8::from(r.soi));
        }
        w.len(self.slots.len());
        for ((l, p, wr, g), s) in &self.slots {
            w.stamp(l);
            w.str(p);
            w.wid(wr);
            w.opt_stamp(g);
            w.stamp(&s.stamp);
            w.i128(s.total);
        }
        w.vv(&self.vv);
        w.len(self.losses.len());
        for (k, v) in &self.losses {
            w.loss_key(k);
            w.loss_value(v);
        }
    }

    /// The deterministic encoding of the state: the model's `canon()` made
    /// binary (lives, dead, deletes, registers, slots, version vector, losses).
    /// Mutant memory (`Gen::vv`, `gens`) is not part of it.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut w = W(Vec::new());
        self.write_body(&mut w);
        w.0
    }

    /// A 128-bit digest of [`MergeState::canonical_bytes`]. Not cryptographic:
    /// it is a divergence detector. Two lanes of a multiply-rotate mix over
    /// big-endian 8-byte words (the tail zero-padded, the length folded in at
    /// the end), cross-fed, then each lane passed through the splitmix64
    /// finaliser.
    pub fn state_hash(&self) -> [u8; 16] {
        hash128(&self.canonical_bytes())
    }

    /// The whole state, for a round trip with [`MergeState::decode`]: the magic, the format
    /// version, then [`MergeState::canonical_bytes`].
    pub fn encode(&self) -> Vec<u8> {
        let mut w = W(Vec::new());
        w.0.extend_from_slice(&MAGIC);
        w.u8(VERSION);
        self.write_body(&mut w);
        w.0
    }

    /// The inverse of [`MergeState::encode`].
    pub fn decode(bytes: &[u8]) -> Result<MergeState, DecodeError> {
        let mut r = R(bytes);
        if r.take(3)? != MAGIC || r.u8()? != VERSION {
            return Err(DecodeError::BadHeader);
        }
        let mut s = MergeState::default();
        for _ in 0..r.len()? {
            let l = r.stamp()?;
            let mut seen = BTreeSet::new();
            for _ in 0..r.len()? {
                put_set(&mut seen, r.stamp()?)?;
            }
            let mut order = Vec::new();
            for _ in 0..r.len()? {
                order.push(r.str()?);
            }
            let kind = match r.u8()? {
                0 => LifeKind::Insert,
                1 => LifeKind::Upsert,
                t => return Err(DecodeError::BadTag(t)),
            };
            put(&mut s.lives, l, Life { seen, order, kind })?;
        }
        for _ in 0..r.len()? {
            put_set(&mut s.dead, r.stamp()?)?;
        }
        for _ in 0..r.len()? {
            let d = r.stamp()?;
            put(&mut s.deletes, d, r.vv()?)?;
        }
        for _ in 0..r.len()? {
            let l = r.stamp()?;
            let p = r.str()?;
            let stamp = r.stamp()?;
            let value = r.value()?;
            let obs = r.obs_stamped()?;
            let generation = r.gen_()?;
            let soi = match r.u8()? {
                0 => false,
                1 => true,
                t => return Err(DecodeError::BadTag(t)),
            };
            put(&mut s.regs, (l, p), Reg { stamp, value, obs, generation, soi })?;
        }
        for _ in 0..r.len()? {
            let l = r.stamp()?;
            let p = r.str()?;
            let wr = r.wid()?;
            let g = r.opt_stamp()?;
            let stamp = r.stamp()?;
            let total = r.total()?;
            put(&mut s.slots, (l, p, wr, g), Slot { stamp, total })?;
        }
        s.vv = r.vv()?;
        for _ in 0..r.len()? {
            let k = r.loss_key()?;
            let v = r.loss_value()?;
            put(&mut s.losses, k, v)?;
        }
        if !r.0.is_empty() {
            return Err(DecodeError::Trailing(r.0.len()));
        }
        Ok(s)
    }
}

// ---- reading ---------------------------------------------------------------

/// Insert into a map that is written in key order: a key that is not strictly above the last
/// one is a duplicate or out of order, and the input is not the encoding of any state.
fn put<K: Ord, V>(m: &mut BTreeMap<K, V>, k: K, v: V) -> Result<(), DecodeError> {
    if m.last_key_value().is_some_and(|(last, _)| *last >= k) {
        return Err(DecodeError::NotCanonical);
    }
    m.insert(k, v);
    Ok(())
}

/// [`put`] for a set.
fn put_set<K: Ord>(m: &mut BTreeSet<K>, k: K) -> Result<(), DecodeError> {
    if m.last().is_some_and(|last| *last >= k) {
        return Err(DecodeError::NotCanonical);
    }
    m.insert(k);
    Ok(())
}

struct R<'a>(&'a [u8]);

impl R<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], DecodeError> {
        let (a, b) = self.0.split_at_checked(n).ok_or(DecodeError::Truncated)?;
        self.0 = b;
        Ok(a)
    }
    fn arr<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        Ok(self.take(N)?.try_into().expect("take returns N bytes"))
    }
    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.arr::<1>()?[0])
    }
    /// A length (`u64`). Each counted item takes at least one byte, so a length
    /// larger than the rest of the input is refused before anything is allocated.
    fn len(&mut self) -> Result<usize, DecodeError> {
        let n = u64::from_be_bytes(self.arr()?);
        if n > self.0.len() as u64 {
            return Err(DecodeError::Truncated);
        }
        Ok(n as usize)
    }
    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_be_bytes(self.arr()?))
    }
    fn i128(&mut self) -> Result<i128, DecodeError> {
        Ok(i128::from_be_bytes(self.arr()?))
    }
    /// A counter total: an `i128` of magnitude at most [`MAX_TOTAL`] (see there).
    fn total(&mut self) -> Result<i128, DecodeError> {
        let t = self.i128()?;
        if t.unsigned_abs() > MAX_TOTAL.unsigned_abs() {
            return Err(DecodeError::TotalOutOfRange);
        }
        Ok(t)
    }
    fn str(&mut self) -> Result<String, DecodeError> {
        let n = self.len()?;
        String::from_utf8(self.take(n)?.to_vec()).map_err(|_| DecodeError::BadString)
    }
    fn hlc(&mut self) -> Result<Hlc, DecodeError> {
        Ok(Hlc::from_bytes(self.arr()?))
    }
    fn wid(&mut self) -> Result<WriterId, DecodeError> {
        let node = NodeId::from_bytes(self.arr()?);
        Ok(WriterId { node, epoch: Epoch(self.arr()?) })
    }
    fn stamp(&mut self) -> Result<MStamp, DecodeError> {
        let hlc = self.hlc()?;
        Ok(MStamp { hlc, writer: self.wid()? })
    }
    fn opt_stamp(&mut self) -> Result<Option<MStamp>, DecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.stamp()?)),
            t => Err(DecodeError::BadTag(t)),
        }
    }
    fn value(&mut self) -> Result<Value, DecodeError> {
        self.value_at(0)
    }
    /// A value nested `depth` arrays deep; deeper than [`MAX_VALUE_DEPTH`] is refused, so that
    /// hostile input cannot exhaust the stack.
    fn value_at(&mut self, depth: usize) -> Result<Value, DecodeError> {
        match self.u8()? {
            0 => Ok(Value::Null),
            1 => Ok(Value::Int64(self.i64()?)),
            2 => Ok(Value::Double(f64::from_bits(u64::from_be_bytes(self.arr()?)))),
            3 => Ok(Value::String(self.str()?)),
            4 => {
                if depth >= MAX_VALUE_DEPTH {
                    return Err(DecodeError::TooDeep);
                }
                let mut a = Vec::new();
                for _ in 0..self.len()? {
                    a.push(self.value_at(depth + 1)?);
                }
                Ok(Value::Array(a))
            }
            7 => Ok(Value::Int32(i32::from_be_bytes(self.arr()?))),
            8 => Ok(Value::Decimal128(self.arr()?)),
            9 => match self.u8()? {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                t => Err(DecodeError::BadTag(t)),
            },
            10 => Ok(Value::Date(self.i64()?)),
            11 => Ok(Value::ObjectId(self.arr()?)),
            12 => {
                let subtype = self.u8()?;
                let n = self.len()?;
                Ok(Value::Binary { subtype, bytes: self.take(n)?.to_vec() })
            }
            13 => Ok(Value::Timestamp {
                time: u32::from_be_bytes(self.arr()?),
                increment: u32::from_be_bytes(self.arr()?),
            }),
            14 => Ok(Value::Regex { pattern: self.str()?, options: self.str()? }),
            t => Err(DecodeError::BadTag(t)),
        }
    }
    fn vv(&mut self) -> Result<Vv, DecodeError> {
        let mut m = BTreeMap::new();
        for _ in 0..self.len()? {
            let w = self.wid()?;
            put(&mut m, w, self.hlc()?)?;
        }
        Ok(m)
    }
    fn obs(&mut self) -> Result<BTreeMap<ObsKey, i128>, DecodeError> {
        let mut m = BTreeMap::new();
        for _ in 0..self.len()? {
            let l = self.stamp()?;
            let w = self.wid()?;
            let g = self.opt_stamp()?;
            put(&mut m, (l, w, g), self.total()?)?;
        }
        Ok(m)
    }
    fn obs_stamped(&mut self) -> Result<BTreeMap<ObsKey, (MStamp, i128)>, DecodeError> {
        let mut m = BTreeMap::new();
        for _ in 0..self.len()? {
            let l = self.stamp()?;
            let w = self.wid()?;
            let g = self.opt_stamp()?;
            let s = self.stamp()?;
            put(&mut m, (l, w, g), (s, self.total()?))?;
        }
        Ok(m)
    }
    fn gen_(&mut self) -> Result<Option<Gen>, DecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => {
                let stamp = self.stamp()?;
                Ok(Some(Gen { stamp, vv: Vv::new(), seen: self.obs()? }))
            }
            t => Err(DecodeError::BadTag(t)),
        }
    }
    fn loss_key(&mut self) -> Result<LossKey, DecodeError> {
        let kind = match self.u8()? {
            0 => LossKind::Clash,
            1 => LossKind::DeleteWon,
            2 => LossKind::InsertLost,
            3 => LossKind::RetiredWriter,
            t => return Err(DecodeError::BadTag(t)),
        };
        let path = self.str()?;
        Ok(LossKey { kind, path, stamp: self.stamp()? })
    }
    fn loss_value(&mut self) -> Result<LossValue, DecodeError> {
        match self.u8()? {
            0 => Ok(LossValue::Value(self.value()?)),
            1 => Ok(LossValue::Total(self.total()?)),
            2 => {
                let mut f = Vec::new();
                for _ in 0..self.len()? {
                    f.push(self.str()?);
                }
                Ok(LossValue::Fields(f))
            }
            t => Err(DecodeError::BadTag(t)),
        }
    }
}

// ---- hash ------------------------------------------------------------------

fn splitmix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn hash128(data: &[u8]) -> [u8; 16] {
    const P1: u64 = 0x9E37_79B1_85EB_CA87;
    const P2: u64 = 0xC2B2_AE3D_27D4_EB4F;
    let mut a: u64 = 0x243F_6A88_85A3_08D3;
    let mut b: u64 = 0x1319_8A2E_0370_7344;
    for chunk in data.chunks(8) {
        let mut buf = [0u8; 8];
        buf[..chunk.len()].copy_from_slice(chunk);
        let v = u64::from_be_bytes(buf);
        a = (a ^ v).wrapping_mul(P1).rotate_left(29);
        b = (b.rotate_left(31) ^ v).wrapping_mul(P2);
        a ^= b >> 17;
        b ^= a >> 23;
    }
    let n = data.len() as u64;
    let x = splitmix(a ^ n);
    let y = splitmix(b ^ n.rotate_left(32) ^ x);
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&x.to_be_bytes());
    out[8..].copy_from_slice(&y.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::super::{JoinOptions, Writer};
    use super::*;

    fn stamp(n: u64, c: u8) -> MStamp {
        MStamp {
            hlc: Hlc::new(n, 0),
            writer: WriterId { node: NodeId::from_bytes([c; 16]), epoch: Epoch::from_u64(0) },
        }
    }

    /// A state's bytes with the sections written by hand: `dead`, `deletes` (stamp, empty
    /// vv) and the state version vector, in the order given, everything else empty.
    fn hand(dead: &[MStamp], deletes: &[MStamp], vv: &[(WriterId, u64)]) -> Vec<u8> {
        let mut w = W(Vec::new());
        w.0.extend_from_slice(&MAGIC);
        w.u8(VERSION);
        w.len(0); // lives
        w.len(dead.len());
        for l in dead {
            w.stamp(l);
        }
        w.len(deletes.len());
        for d in deletes {
            w.stamp(d);
            w.len(0);
        }
        w.len(0); // regs
        w.len(0); // slots
        w.len(vv.len());
        for (wr, h) in vv {
            w.wid(wr);
            w.hlc(Hlc::new(*h, 0));
        }
        w.len(0); // losses
        w.0
    }

    #[test]
    fn hand_built_bytes_decode_when_sorted() {
        let (a, b) = (stamp(1, b'A'), stamp(2, b'A'));
        let wa = WriterId { node: NodeId::from_bytes([b'A'; 16]), epoch: Epoch::from_u64(0) };
        let wb = WriterId { node: NodeId::from_bytes([b'B'; 16]), epoch: Epoch::from_u64(0) };
        let bytes = hand(&[a, b], &[a, b], &[(wa, 3), (wb, 4)]);
        let st = MergeState::decode(&bytes).unwrap();
        assert_eq!(st.encode(), bytes);
        assert_eq!((st.dead.len(), st.deletes.len(), st.vv.len()), (2, 2, 2));
    }

    #[test]
    fn duplicate_or_unsorted_entries_are_refused() {
        let (a, b) = (stamp(1, b'A'), stamp(2, b'A'));
        let wa = WriterId { node: NodeId::from_bytes([b'A'; 16]), epoch: Epoch::from_u64(0) };
        let wb = WriterId { node: NodeId::from_bytes([b'B'; 16]), epoch: Epoch::from_u64(0) };
        let no = Err(DecodeError::NotCanonical);
        // a set: duplicate, and out of order
        assert_eq!(MergeState::decode(&hand(&[a, a], &[], &[])), no);
        assert_eq!(MergeState::decode(&hand(&[b, a], &[], &[])), no);
        // a map keyed by stamp
        assert_eq!(MergeState::decode(&hand(&[], &[a, a], &[])), no);
        assert_eq!(MergeState::decode(&hand(&[], &[b, a], &[])), no);
        // a version vector: duplicate writer, entries out of order
        assert_eq!(MergeState::decode(&hand(&[], &[], &[(wa, 1), (wa, 2)])), no);
        assert_eq!(MergeState::decode(&hand(&[], &[], &[(wb, 1), (wa, 2)])), no);
        // and the sorted forms are fine
        assert!(MergeState::decode(&hand(&[a, b], &[a, b], &[(wa, 1), (wb, 2)])).is_ok());
    }

    /// A state whose only register has the given `obs` entries and generation `seen` entries,
    /// written by hand in the order given.
    fn with_obs(obs: &[(MStamp, MStamp, i128)], seen: &[(MStamp, i128)]) -> Vec<u8> {
        let life = stamp(1, b'A');
        let wr = WriterId { node: NodeId::from_bytes([b'A'; 16]), epoch: Epoch::from_u64(0) };
        let mut w = W(Vec::new());
        w.0.extend_from_slice(&MAGIC);
        w.u8(VERSION);
        w.len(0); // lives
        w.len(0); // dead
        w.len(0); // deletes
        w.len(1); // regs
        w.stamp(&life);
        w.str("x");
        w.stamp(&life); // the register's stamp
        w.value(&Value::Int64(1));
        w.len(obs.len());
        for (slot_life, slot_stamp, total) in obs {
            w.stamp(slot_life);
            w.wid(&wr);
            w.opt_stamp(&None);
            w.stamp(slot_stamp);
            w.i128(*total);
        }
        if seen.is_empty() {
            w.u8(0); // no generation
        } else {
            w.u8(1);
            w.stamp(&life);
            w.len(seen.len());
            for (slot_life, total) in seen {
                w.stamp(slot_life);
                w.wid(&wr);
                w.opt_stamp(&None);
                w.i128(*total);
            }
        }
        w.u8(0); // soi
        w.len(0); // slots
        w.len(0); // vv
        w.len(0); // losses
        w.0
    }

    #[test]
    fn unsorted_or_duplicate_obs_and_seen_entries_are_refused() {
        let (a, b) = (stamp(1, b'A'), stamp(2, b'A'));
        let no = Err(DecodeError::NotCanonical);
        // sorted: fine, and canonical
        let good = with_obs(&[(a, a, 1), (b, a, 2)], &[(a, 1), (b, 2)]);
        assert_eq!(MergeState::decode(&good).unwrap().encode(), good);
        // a register's obs: out of order, duplicate
        assert_eq!(MergeState::decode(&with_obs(&[(b, a, 2), (a, a, 1)], &[])), no);
        assert_eq!(MergeState::decode(&with_obs(&[(a, a, 1), (a, b, 2)], &[])), no);
        // a generation's seen totals: out of order, duplicate
        assert_eq!(MergeState::decode(&with_obs(&[], &[(b, 2), (a, 1)])), no);
        assert_eq!(MergeState::decode(&with_obs(&[], &[(a, 1), (a, 2)])), no);
    }

    #[test]
    fn a_flag_byte_other_than_0_or_1_is_refused() {
        let mut w = Writer::new(
            WriterId { node: NodeId::from_bytes([b'A'; 16]), epoch: Epoch::from_u64(0) },
            JoinOptions::default(),
        );
        w.upsert_soi(&[("y", Value::Int64(1))], "x", 1, &[], 0).unwrap();
        let enc = w.state.encode();
        let st = MergeState::decode(&enc).unwrap();
        assert!(st.regs.values().any(|r| r.soi));
        // Every position with a 0 or 1 byte, set to 2: whatever still decodes re-encodes to
        // itself, and at least the flag (and the tags) are refused.
        let (mut bad_tag, mut accepted) = (0, 0);
        for i in 4..enc.len() {
            if enc[i] <= 1 {
                let mut b = enc.clone();
                b[i] = 2;
                match MergeState::decode(&b) {
                    Ok(s) => {
                        accepted += 1;
                        assert_eq!(s.encode(), b, "byte {i}");
                    }
                    Err(DecodeError::BadTag(2)) => bad_tag += 1,
                    Err(_) => {}
                }
            }
        }
        assert!(bad_tag >= 1, "the soi flag byte of 2 is refused");
        assert!(accepted > 0 || bad_tag > 0);
    }
}
