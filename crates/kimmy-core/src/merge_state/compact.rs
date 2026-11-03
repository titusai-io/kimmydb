//! The compact canonical form, `QMS` version 2.
//!
//! [`MergeState::canonical_bytes`] and [`MergeState::encode`] (version 1, `codec.rs`) write a full
//! 44-byte stamp for every life and every cell, `u64` lengths, and a generation and an `obs` map
//! in each cell: 5 to 9 times the size the design assumes. This is the compact form that
//! replaces them. NOTHING STORES OR HASHES IT YET: the version 1 form, its golden vectors and
//! [`MergeState::state_hash`] are unchanged, and the switch (one format for the hash and for
//! storage, the goldens re-pinned) waits on the rulings for the counter cell layout and the
//! order state. This module is the skeleton those extend.
//!
//! The state hash, the stored form and the wire form will all be [`MergeState::compact_bytes`]
//! (the body) and, with a header, [`MergeState::encode_compact`]: the 3 bytes `QMS`, ONE format
//! version byte (2) and the body. Every map is written in key order. Mutant memory (`Gen::vv`,
//! `MergeState::gens`) is in none of them.
//!
//! # Primitives
//!
//! * a count or length is a LEB128 varint of a `u64`, written in its shortest form;
//! * a counter total is the zigzag of the `i128` as a LEB128 varint of a `u128`, shortest form;
//! * a string is a length and its UTF-8 bytes; a [`Path`] is its segment count (at least 1) and
//!   its segments as strings;
//! * an `Int32`, `Int64` and `Date` and the parts of a `Timestamp` are fixed width big-endian, a
//!   `Double` is its IEEE bit pattern (`to_bits`, big-endian);
//! * a value is a tag byte and its payload: 0 `Unset`, 1 `Int64`, 2 `Double`, 3 `String`,
//!   4 `Array` (a count and the values), **5 and 6 reserved for the explicit and the implicit map
//!   marker**, 7 `Int32`, 8 `Decimal128`, 9 `Bool`, 10 `Date`, 11 `ObjectId`, 12 `Binary`,
//!   13 `Timestamp`, 14 `Regex`, 15 `Null`;
//! * an HLC is its 10 bytes (`Hlc::to_bytes`); a writer is 34 bytes (node 16, epoch 18) in the
//!   writer table and a varint index into it everywhere else;
//! * a stamp is an HLC and a writer index: 11 bytes for a table of up to 128 writers;
//! * a loss kind is a byte: 0 clash, 1 deleteWon, 2 insertLost, 3 retiredWriter, 4 typeClash,
//!   5 arrayClash.
//!
//! # Body
//!
//! A body is a sequence of sections in a fixed order. A later format adds a section at the end
//! (or a field to an existing section) and raises the version byte; this version accepts exactly
//! these nine:
//!
//! 1. the **writer table**: a count, then every [`WriterId`] the state mentions, in strictly
//!    increasing order;
//! 2. the **life table**: a count, then every stamp the state uses as a life (a key of `lives`,
//!    a member of `dead`, the life of a register or a counter slot, the life in an `obs`
//!    or `seen` key), as stamps, in strictly increasing order. A life is written as a varint
//!    index into this table everywhere else, which is what makes a cell cheap: its life is one
//!    byte, not 44;
//! 3. `lives`: a count; each is a life index, the count and the full stamps of the deletes its
//!    creator had seen, the field order as a count of paths, and a kind byte (0 insert,
//!    1 upsert);
//! 4. `dead`: a count and life indexes;
//! 5. `deletes`: a count; each a full stamp and a version vector;
//! 6. `regs` (the cells): a count; each a life index, a path, a flags byte, the register
//!    stamp unless it is the life's, the value, the `obs` map, and the generation: its stamp
//!    unless it is the register's, and its `seen` totals. Flags: bits 0 and 1 are the
//!    generation (0 none, 1 started by this register's own stamp, 2 started by another stamp),
//!    bit 2 is `soi`, bit 3 says the register stamp is the life's, and the other bits are 0;
//! 7. `slots`: a count; each a life index, a path, a writer index, a generation byte (0 none,
//!    1 and then its stamp), the stamp, and the total;
//! 8. the **version vector**: a count; each a writer index and an HLC;
//! 9. `losses`: a count; each a kind byte, a path, a stamp and the lost value.
//!
//! `obs` keys are a life index, a writer index and a generation as in `slots`; a register's
//! `obs` entry adds the slot's stamp and total, a generation's `seen` entry the total.
//!
//! # Extension points
//!
//! * **Order state** (the `Ord` of option A: floors, per-writer firsts, last, level floor): a
//!   section 10 after `losses`, keyed `(life index, path)` like `regs`, with the version byte
//!   raised. Its candidate stamps use the same stamp and writer index primitives.
//! * **Counter cell** (the rulings on the `absorbed` rule and the cell layout): replaces the
//!   `obs` map of a register (section 6) and the layout of `slots` (section 7); until then both
//!   are the version 1 data in compact spelling.
//! * **Map markers and type clashes**: value tags 5 and 6 and loss kinds 4 and 5 are already
//!   assigned.
//!
//! # Canonicity
//!
//! The bytes do not depend on the architecture, the word size, `std::hash`, hash-map order or
//! float formatting. [`MergeState::decode_compact`] accepts only what
//! [`MergeState::encode_compact`] can write, so `encode_compact(decode_compact(b)) == b` for every
//! accepted `b`: map and set entries are strictly increasing in key order, both tables are exactly
//! the set of writers and lives the sections use (an unused entry is refused), every varint is in
//! its shortest form, a flag byte has no stray bit, and a choice between two spellings (a register
//! stamp written out when it is the life's, a generation stamp written out when it is the
//! register's) takes the short one only.

use std::collections::{BTreeMap, BTreeSet};

use super::codec::{DecodeError, MAGIC, MAX_VALUE_DEPTH, hash128, put, put_set};
use super::state::{Gen, Life, LifeKind, MAX_TOTAL, MergeState, ObsKey, Reg, Slot, Vv};
use super::types::{Epoch, LossKey, LossKind, LossValue, MStamp, Path, Value, WriterId};
use crate::hlc::Hlc;
use crate::ids::NodeId;

/// The format version byte of this form.
const VERSION: u8 = 2;

// ---- writing ---------------------------------------------------------------

/// The two passes of the writer. The first walks the state with `scan` set and records the
/// writers and lives it mentions (the bytes it writes are thrown away); the second writes the
/// tables and then the same walk with their indexes. One walk, so the passes cannot disagree.
struct W {
    out: Vec<u8>,
    scan: bool,
    seen_writers: BTreeSet<WriterId>,
    seen_lives: BTreeSet<MStamp>,
    writers: BTreeMap<WriterId, usize>,
    lives: BTreeMap<MStamp, usize>,
}

impl W {
    fn scanning() -> Self {
        Self {
            out: Vec::new(),
            scan: true,
            seen_writers: BTreeSet::new(),
            seen_lives: BTreeSet::new(),
            writers: BTreeMap::new(),
            lives: BTreeMap::new(),
        }
    }

    fn u8(&mut self, v: u8) {
        self.out.push(v);
    }
    fn varint(&mut self, mut v: u128) {
        while v >= 0x80 {
            self.out.push(v as u8 | 0x80);
            v >>= 7;
        }
        self.out.push(v as u8);
    }
    fn len(&mut self, n: usize) {
        self.varint(n as u128);
    }
    fn i64(&mut self, v: i64) {
        self.out.extend_from_slice(&v.to_be_bytes());
    }
    /// A counter total: zigzag, then a varint.
    fn total(&mut self, v: i128) {
        self.varint(((v << 1) ^ (v >> 127)) as u128);
    }
    fn str(&mut self, s: &str) {
        self.len(s.len());
        self.out.extend_from_slice(s.as_bytes());
    }
    fn path(&mut self, p: &Path) {
        self.len(p.segments().len());
        for s in p.segments() {
            self.str(s);
        }
    }
    fn hlc(&mut self, h: Hlc) {
        self.out.extend_from_slice(&h.to_bytes());
    }
    /// A writer: its index in the table (second pass), or noted (first pass).
    fn wid(&mut self, w: &WriterId) {
        if self.scan {
            self.seen_writers.insert(*w);
        } else {
            self.len(self.writers[w]);
        }
    }
    /// A life: its index in the life table (second pass), or noted (first pass).
    fn life(&mut self, l: &MStamp) {
        if self.scan {
            self.seen_lives.insert(*l);
            self.seen_writers.insert(l.writer);
        } else {
            self.len(self.lives[l]);
        }
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
            Value::Unset => self.u8(0),
            Value::Null => self.u8(15),
            Value::Int64(i) => {
                self.u8(1);
                self.i64(*i);
            }
            Value::Double(d) => {
                self.u8(2);
                self.out.extend_from_slice(&d.to_bits().to_be_bytes());
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
                self.out.extend_from_slice(&i.to_be_bytes());
            }
            Value::Decimal128(d) => {
                self.u8(8);
                self.out.extend_from_slice(d);
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
                self.out.extend_from_slice(o);
            }
            Value::Binary { subtype, bytes } => {
                self.u8(12);
                self.u8(*subtype);
                self.len(bytes.len());
                self.out.extend_from_slice(bytes);
            }
            Value::Timestamp { time, increment } => {
                self.u8(13);
                self.out.extend_from_slice(&time.to_be_bytes());
                self.out.extend_from_slice(&increment.to_be_bytes());
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
    fn obs_key(&mut self, (l, w, g): &ObsKey) {
        self.life(l);
        self.wid(w);
        self.opt_stamp(g);
    }
    fn obs(&mut self, m: &BTreeMap<ObsKey, i128>) {
        self.len(m.len());
        for (k, t) in m {
            self.obs_key(k);
            self.total(*t);
        }
    }
    fn obs_stamped(&mut self, m: &BTreeMap<ObsKey, (MStamp, i128)>) {
        self.len(m.len());
        for (k, (s, t)) in m {
            self.obs_key(k);
            self.stamp(s);
            self.total(*t);
        }
    }
    fn loss_key(&mut self, k: &LossKey) {
        self.u8(match k.kind {
            LossKind::Clash => 0,
            LossKind::DeleteWon => 1,
            LossKind::InsertLost => 2,
            LossKind::RetiredWriter => 3,
            LossKind::TypeClash => 4,
            LossKind::ArrayClash => 5,
        });
        self.path(&k.path);
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
                self.total(*t);
            }
            LossValue::Fields(f) => {
                self.u8(2);
                self.len(f.len());
                for p in f {
                    self.path(p);
                }
            }
        }
    }
}

/// The flags byte of a register: see the module documentation.
const FLAG_GEN_OWN: u8 = 1;
const FLAG_GEN_OTHER: u8 = 2;
const FLAG_GEN_MASK: u8 = 3;
const FLAG_SOI: u8 = 4;
const FLAG_STAMP_IS_LIFE: u8 = 8;

impl MergeState {
    /// Sections 3 to 9, with the tables of the pass `w` is in.
    fn write_sections(&self, w: &mut W) {
        w.len(self.lives.len());
        for (l, v) in &self.lives {
            w.life(l);
            w.len(v.seen.len());
            for d in &v.seen {
                w.stamp(d);
            }
            w.len(v.order.len());
            for p in &v.order {
                w.path(p);
            }
            w.u8(match v.kind {
                LifeKind::Insert => 0,
                LifeKind::Upsert => 1,
            });
        }
        w.len(self.dead.len());
        for l in &self.dead {
            w.life(l);
        }
        w.len(self.deletes.len());
        for (d, vv) in &self.deletes {
            w.stamp(d);
            w.vv(vv);
        }
        w.len(self.regs.len());
        for ((l, p), r) in &self.regs {
            w.life(l);
            w.path(p);
            let mut flags = if r.soi { FLAG_SOI } else { 0 };
            if r.stamp == *l {
                flags |= FLAG_STAMP_IS_LIFE;
            }
            flags |= match &r.generation {
                None => 0,
                Some(g) if g.stamp == r.stamp => FLAG_GEN_OWN,
                Some(_) => FLAG_GEN_OTHER,
            };
            w.u8(flags);
            if flags & FLAG_STAMP_IS_LIFE == 0 {
                w.stamp(&r.stamp);
            }
            w.value(&r.value);
            w.obs_stamped(&r.obs);
            if let Some(g) = &r.generation {
                if flags & FLAG_GEN_MASK == FLAG_GEN_OTHER {
                    w.stamp(&g.stamp);
                }
                w.obs(&g.seen);
            }
        }
        w.len(self.slots.len());
        for ((l, p, wr, g), s) in &self.slots {
            w.life(l);
            w.path(p);
            w.wid(wr);
            w.opt_stamp(g);
            w.stamp(&s.stamp);
            w.total(s.total);
        }
        w.vv(&self.vv);
        w.len(self.losses.len());
        for (k, v) in &self.losses {
            w.loss_key(k);
            w.loss_value(v);
        }
    }

    /// The body: both tables, then the sections.
    fn write_compact_body(&self) -> Vec<u8> {
        let mut scan = W::scanning();
        self.write_sections(&mut scan);
        let (writers, lives) = (scan.seen_writers, scan.seen_lives);
        let mut w = W {
            out: Vec::new(),
            scan: false,
            seen_writers: BTreeSet::new(),
            seen_lives: BTreeSet::new(),
            writers: writers.iter().enumerate().map(|(i, x)| (*x, i)).collect(),
            lives: lives.iter().enumerate().map(|(i, x)| (*x, i)).collect(),
        };
        w.len(writers.len());
        for x in &writers {
            w.out.extend_from_slice(&x.node.to_bytes());
            w.out.extend_from_slice(&x.epoch.0);
        }
        w.len(lives.len());
        for l in &lives {
            w.stamp(l);
        }
        self.write_sections(&mut w);
        w.out
    }

    /// The compact body of the state: the model's `canon()` made binary (lives, dead, deletes,
    /// registers, slots, version vector, losses) behind its writer and life tables. Mutant
    /// memory (`Gen::vv`, `gens`) is not part of it.
    pub fn compact_bytes(&self) -> Vec<u8> {
        self.write_compact_body()
    }

    /// A 128-bit digest of [`MergeState::compact_bytes`], with the same mix as
    /// [`MergeState::state_hash`]. Nothing uses it yet: the differential test pins it against
    /// the reference model's own encoder until the hash is switched to the compact form.
    pub fn compact_hash(&self) -> [u8; 16] {
        hash128(&self.compact_bytes())
    }

    /// The whole state in the compact form, for a round trip with
    /// [`MergeState::decode_compact`]: the magic, the format version 2, then
    /// [`MergeState::compact_bytes`].
    pub fn encode_compact(&self) -> Vec<u8> {
        let mut out = Vec::from(MAGIC);
        out.push(VERSION);
        out.extend_from_slice(&self.write_compact_body());
        out
    }

    /// The inverse of [`MergeState::encode_compact`].
    pub fn decode_compact(bytes: &[u8]) -> Result<MergeState, DecodeError> {
        let mut r = R::new(bytes);
        if r.take(3)? != MAGIC || r.u8()? != VERSION {
            return Err(DecodeError::BadHeader);
        }
        r.tables()?;
        let mut s = MergeState::default();
        for _ in 0..r.len()? {
            let l = r.life()?;
            let mut seen = BTreeSet::new();
            for _ in 0..r.len()? {
                put_set(&mut seen, r.stamp()?)?;
            }
            let mut order = Vec::new();
            for _ in 0..r.len()? {
                order.push(r.path()?);
            }
            let kind = match r.u8()? {
                0 => LifeKind::Insert,
                1 => LifeKind::Upsert,
                t => return Err(DecodeError::BadTag(t)),
            };
            put(&mut s.lives, l, Life { seen, order, kind })?;
        }
        for _ in 0..r.len()? {
            put_set(&mut s.dead, r.life()?)?;
        }
        for _ in 0..r.len()? {
            let d = r.stamp()?;
            put(&mut s.deletes, d, r.vv()?)?;
        }
        for _ in 0..r.len()? {
            let l = r.life()?;
            let p = r.path()?;
            let flags = r.u8()?;
            if flags & 0xf0 != 0 || flags & FLAG_GEN_MASK == 3 {
                return Err(DecodeError::BadTag(flags));
            }
            let stamp = if flags & FLAG_STAMP_IS_LIFE != 0 {
                l
            } else {
                let st = r.stamp()?;
                if st == l {
                    return Err(DecodeError::NotCanonical);
                }
                st
            };
            let value = r.value()?;
            let obs = r.obs_stamped()?;
            let generation = match flags & FLAG_GEN_MASK {
                0 => None,
                g => {
                    let gs = if g == FLAG_GEN_OWN {
                        stamp
                    } else {
                        let gs = r.stamp()?;
                        if gs == stamp {
                            return Err(DecodeError::NotCanonical);
                        }
                        gs
                    };
                    Some(Gen { stamp: gs, vv: Vv::new(), seen: r.obs()? })
                }
            };
            let soi = flags & FLAG_SOI != 0;
            put(&mut s.regs, (l, p), Reg { stamp, value, obs, generation, soi })?;
        }
        for _ in 0..r.len()? {
            let l = r.life()?;
            let p = r.path()?;
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
        r.finish()?;
        Ok(s)
    }
}

// ---- reading ---------------------------------------------------------------

struct R<'a> {
    rest: &'a [u8],
    writers: Vec<WriterId>,
    lives: Vec<MStamp>,
    writer_used: Vec<bool>,
    life_used: Vec<bool>,
}

impl R<'_> {
    fn new(rest: &[u8]) -> R<'_> {
        R {
            rest,
            writers: Vec::new(),
            lives: Vec::new(),
            writer_used: Vec::new(),
            life_used: Vec::new(),
        }
    }
    fn take(&mut self, n: usize) -> Result<&[u8], DecodeError> {
        let (a, b) = self.rest.split_at_checked(n).ok_or(DecodeError::Truncated)?;
        self.rest = b;
        Ok(a)
    }
    fn arr<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        Ok(self.take(N)?.try_into().expect("take returns N bytes"))
    }
    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.arr::<1>()?[0])
    }
    /// A varint of at most `max` bytes in its shortest form (no trailing zero group).
    fn varint(&mut self, max: usize) -> Result<u128, DecodeError> {
        let mut v: u128 = 0;
        for k in 0..max {
            let b = self.u8()?;
            let bits = u128::from(b & 0x7f);
            // The last group may not overflow the width.
            if k == max - 1 && (b & 0x80 != 0 || bits >> (if max == 10 { 1 } else { 5 }) != 0) {
                return Err(DecodeError::NotCanonical);
            }
            v |= bits << (7 * k);
            if b & 0x80 == 0 {
                if k > 0 && b == 0 {
                    return Err(DecodeError::NotCanonical);
                }
                return Ok(v);
            }
        }
        Err(DecodeError::NotCanonical)
    }
    /// A count or length (a `u64` varint). Each counted item takes at least one byte, so a
    /// length larger than the rest of the input is refused before anything is allocated.
    fn len(&mut self) -> Result<usize, DecodeError> {
        let n = self.varint(10)?;
        if n > self.rest.len() as u128 {
            return Err(DecodeError::Truncated);
        }
        Ok(n as usize)
    }
    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_be_bytes(self.arr()?))
    }
    /// A counter total: a zigzag `u128` varint, of magnitude at most [`MAX_TOTAL`] (see there).
    fn total(&mut self) -> Result<i128, DecodeError> {
        let z = self.varint(19)?;
        let t = ((z >> 1) as i128) ^ -((z & 1) as i128);
        if t.unsigned_abs() > MAX_TOTAL.unsigned_abs() {
            return Err(DecodeError::TotalOutOfRange);
        }
        Ok(t)
    }
    fn str(&mut self) -> Result<String, DecodeError> {
        let n = self.len()?;
        String::from_utf8(self.take(n)?.to_vec()).map_err(|_| DecodeError::BadString)
    }
    fn path(&mut self) -> Result<Path, DecodeError> {
        let n = self.len()?;
        let mut segments = Vec::new();
        for _ in 0..n {
            segments.push(self.str()?);
        }
        Path::new(segments).map_err(|_| DecodeError::BadPath)
    }
    fn hlc(&mut self) -> Result<Hlc, DecodeError> {
        Ok(Hlc::from_bytes(self.arr()?))
    }
    /// The writer and life tables (sections 1 and 2), each strictly increasing.
    fn tables(&mut self) -> Result<(), DecodeError> {
        for _ in 0..self.len()? {
            let node = NodeId::from_bytes(self.arr()?);
            let w = WriterId { node, epoch: Epoch(self.arr()?) };
            if self.writers.last().is_some_and(|last| *last >= w) {
                return Err(DecodeError::NotCanonical);
            }
            self.writers.push(w);
        }
        self.writer_used = vec![false; self.writers.len()];
        for _ in 0..self.len()? {
            let l = self.stamp()?;
            if self.lives.last().is_some_and(|last| *last >= l) {
                return Err(DecodeError::NotCanonical);
            }
            self.lives.push(l);
        }
        self.life_used = vec![false; self.lives.len()];
        Ok(())
    }
    /// The end of the input; every table entry must have been used.
    fn finish(&self) -> Result<(), DecodeError> {
        if !self.rest.is_empty() {
            return Err(DecodeError::Trailing(self.rest.len()));
        }
        if self.writer_used.iter().chain(&self.life_used).any(|u| !u) {
            return Err(DecodeError::NotCanonical);
        }
        Ok(())
    }
    fn wid(&mut self) -> Result<WriterId, DecodeError> {
        let i = self.len()?;
        let w = *self.writers.get(i).ok_or(DecodeError::BadIndex)?;
        // Once the tables are read `writer_used` is as long as `writers`; while the life
        // table itself is read it is, so the mark cannot miss.
        self.writer_used[i] = true;
        Ok(w)
    }
    fn life(&mut self) -> Result<MStamp, DecodeError> {
        let i = self.len()?;
        let l = *self.lives.get(i).ok_or(DecodeError::BadIndex)?;
        self.life_used[i] = true;
        Ok(l)
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
            0 => Ok(Value::Unset),
            15 => Ok(Value::Null),
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
    fn obs_key(&mut self) -> Result<ObsKey, DecodeError> {
        let l = self.life()?;
        let w = self.wid()?;
        Ok((l, w, self.opt_stamp()?))
    }
    fn obs(&mut self) -> Result<BTreeMap<ObsKey, i128>, DecodeError> {
        let mut m = BTreeMap::new();
        for _ in 0..self.len()? {
            let k = self.obs_key()?;
            put(&mut m, k, self.total()?)?;
        }
        Ok(m)
    }
    fn obs_stamped(&mut self) -> Result<BTreeMap<ObsKey, (MStamp, i128)>, DecodeError> {
        let mut m = BTreeMap::new();
        for _ in 0..self.len()? {
            let k = self.obs_key()?;
            let s = self.stamp()?;
            put(&mut m, k, (s, self.total()?))?;
        }
        Ok(m)
    }
    fn loss_key(&mut self) -> Result<LossKey, DecodeError> {
        let kind = match self.u8()? {
            0 => LossKind::Clash,
            1 => LossKind::DeleteWon,
            2 => LossKind::InsertLost,
            3 => LossKind::RetiredWriter,
            4 => LossKind::TypeClash,
            5 => LossKind::ArrayClash,
            t => return Err(DecodeError::BadTag(t)),
        };
        let path = self.path()?;
        Ok(LossKey { kind, path, stamp: self.stamp()? })
    }
    fn loss_value(&mut self) -> Result<LossValue, DecodeError> {
        match self.u8()? {
            0 => Ok(LossValue::Value(self.value()?)),
            1 => Ok(LossValue::Total(self.total()?)),
            2 => {
                let mut f = Vec::new();
                for _ in 0..self.len()? {
                    f.push(self.path()?);
                }
                Ok(LossValue::Fields(f))
            }
            t => Err(DecodeError::BadTag(t)),
        }
    }
}
