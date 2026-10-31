//! Canonical bytes, the state hash and the whole-state round trip.
//!
//! The encoding is total and deterministic: every map is written in key order
//! (the `BTreeMap` order, itself total), every length is a big-endian `u32`,
//! integers are fixed width big-endian, doubles are their IEEE bit pattern.
//! It is the binary form of the model's `canon()`.

use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use super::state::{Gen, Life, LifeKind, MergeState, ObsKey, Reg, Slot, Vv};
use super::types::{Epoch, LossKey, LossKind, LossValue, MStamp, Value, WriterId};
use crate::hlc::Hlc;
use crate::ids::NodeId;

const MAGIC: [u8; 3] = *b"QMS";
const VERSION: u8 = 1;

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
}

// ---- writing ---------------------------------------------------------------

struct W(Vec<u8>);

impl W {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn len(&mut self, n: usize) {
        self.0.extend_from_slice(&(n as u32).to_be_bytes());
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
            Value::Int(i) => {
                self.u8(1);
                self.i64(*i);
            }
            Value::Double(d) => {
                self.u8(2);
                self.0.extend_from_slice(&d.to_bits().to_be_bytes());
            }
            Value::Str(s) => {
                self.u8(3);
                self.str(s);
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
    fn gen_(&mut self, g: &Option<Gen>) {
        match g {
            None => self.u8(0),
            Some(g) => {
                self.u8(1);
                self.stamp(&g.stamp);
                self.vv(&g.vv);
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
            w.obs(&r.obs);
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
    /// NOTE(model): `canon()` leaves the vestigial `gens` out, so this does too;
    /// [`MergeState::encode`] carries them.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut w = W(Vec::new());
        self.write_body(&mut w);
        w.0
    }

    /// A 128-bit digest of [`MergeState::canonical_bytes`]. Not cryptographic:
    /// it is a divergence detector. Two lanes of a multiply-rotate mix over
    /// little-endian 8-byte words (the tail zero-padded, the length folded in at
    /// the end), cross-fed, then each lane passed through the splitmix64
    /// finaliser.
    pub fn state_hash(&self) -> [u8; 16] {
        hash128(&self.canonical_bytes())
    }

    /// The whole state, including `gens`, for a round trip with [`MergeState::decode`].
    pub fn encode(&self) -> Vec<u8> {
        let mut w = W(Vec::new());
        w.0.extend_from_slice(&MAGIC);
        w.u8(VERSION);
        self.write_body(&mut w);
        w.len(self.gens.len());
        for ((l, p), (s, vv)) in &self.gens {
            w.stamp(l);
            w.str(p);
            w.stamp(s);
            w.vv(vv);
        }
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
                seen.insert(r.stamp()?);
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
            s.lives.insert(l, Life { seen, order, kind });
        }
        for _ in 0..r.len()? {
            s.dead.insert(r.stamp()?);
        }
        for _ in 0..r.len()? {
            let d = r.stamp()?;
            s.deletes.insert(d, r.vv()?);
        }
        for _ in 0..r.len()? {
            let l = r.stamp()?;
            let p = r.str()?;
            let stamp = r.stamp()?;
            let value = r.value()?;
            let obs = r.obs()?;
            let generation = r.gen_()?;
            let soi = r.u8()? != 0;
            s.regs.insert((l, p), Reg { stamp, value, obs, generation, soi });
        }
        for _ in 0..r.len()? {
            let l = r.stamp()?;
            let p = r.str()?;
            let wr = r.wid()?;
            let g = r.opt_stamp()?;
            let stamp = r.stamp()?;
            let total = r.i128()?;
            s.slots.insert((l, p, wr, g), Slot { stamp, total });
        }
        s.vv = r.vv()?;
        for _ in 0..r.len()? {
            let k = r.loss_key()?;
            let v = r.loss_value()?;
            s.losses.insert(k, v);
        }
        for _ in 0..r.len()? {
            let l = r.stamp()?;
            let p = r.str()?;
            let st = r.stamp()?;
            let vv = r.vv()?;
            s.gens.insert((l, p), (st, vv));
        }
        if !r.0.is_empty() {
            return Err(DecodeError::Trailing(r.0.len()));
        }
        Ok(s)
    }
}

// ---- reading ---------------------------------------------------------------

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
    /// A length. Each counted item takes at least one byte, so a length larger
    /// than the rest of the input is refused before anything is allocated.
    fn len(&mut self) -> Result<usize, DecodeError> {
        let n = u32::from_be_bytes(self.arr()?) as usize;
        if n > self.0.len() {
            return Err(DecodeError::Truncated);
        }
        Ok(n)
    }
    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_be_bytes(self.arr()?))
    }
    fn i128(&mut self) -> Result<i128, DecodeError> {
        Ok(i128::from_be_bytes(self.arr()?))
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
        match self.u8()? {
            0 => Ok(Value::Null),
            1 => Ok(Value::Int(self.i64()?)),
            2 => Ok(Value::Double(f64::from_bits(u64::from_be_bytes(self.arr()?)))),
            3 => Ok(Value::Str(self.str()?)),
            t => Err(DecodeError::BadTag(t)),
        }
    }
    fn vv(&mut self) -> Result<Vv, DecodeError> {
        let mut m = BTreeMap::new();
        for _ in 0..self.len()? {
            let w = self.wid()?;
            m.insert(w, self.hlc()?);
        }
        Ok(m)
    }
    fn obs(&mut self) -> Result<BTreeMap<ObsKey, i128>, DecodeError> {
        let mut m = BTreeMap::new();
        for _ in 0..self.len()? {
            let l = self.stamp()?;
            let w = self.wid()?;
            let g = self.opt_stamp()?;
            m.insert((l, w, g), self.i128()?);
        }
        Ok(m)
    }
    fn gen_(&mut self) -> Result<Option<Gen>, DecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => {
                let stamp = self.stamp()?;
                let vv = self.vv()?;
                Ok(Some(Gen { stamp, vv, seen: self.obs()? }))
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
            1 => Ok(LossValue::Total(self.i128()?)),
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
        let v = u64::from_le_bytes(buf);
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
