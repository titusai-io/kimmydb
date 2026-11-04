//! The compact canonical form, `QMS` version 2.
//!
//! [`MergeState::canonical_bytes`] and [`MergeState::encode`] (version 1, `codec.rs`) write a full
//! 44-byte stamp for every life and every cell, `u64` lengths, and a generation in each cell:
//! 5 to 9 times the size the design assumes. This is the compact form that
//! replaces them. NOTHING STORES OR HASHES IT YET: the version 1 form, its golden vectors and
//! [`MergeState::state_hash`] are unchanged, and the switch (one format for the hash and for
//! storage, the goldens re-pinned) waits on the goldens being pinned once, with the counter cell and
//! the order state.
//!
//! The state hash, the stored form and the wire form will all be [`MergeState::compact_bytes`]
//! (the body) and, with a header, [`MergeState::encode_compact`]: the 3 bytes `QMS`, ONE format
//! version byte (2) and the body. Every map is written in key order. Mutant memory (`Gen::vv`,
//! `MergeState::gens`) is in none of them.
//!
//! # Primitives
//!
//! * a count or length is a LEB128 varint of a `u64`, written in its shortest form;
//! * a counter total is a PAYLOAD: the zigzag of the `i128` as a LEB128 varint of a `u128`, shortest
//!   form, for an `Int64` or an `Int32` amount, the 16 bytes for a `Decimal128`. Its TYPE is
//!   spelled beside it: in the flag byte of a slot (section 7), and as a leading TYPE BYTE (0 `Int64`,
//!   1 `Int32`, 2 `Decimal128`, any other byte refused) on every total that has no flag byte, a
//!   generation's `seen` entry and the total of a loss record. A typed total is the type byte and
//!   its payload;
//! * a string is a length and its UTF-8 bytes; a [`Path`] is its segment count (at least 1) and
//!   its segments as strings;
//! * an `Int32`, `Int64` and `Date` and the parts of a `Timestamp` are fixed width big-endian, a
//!   `Double` is its IEEE bit pattern (`to_bits`, big-endian);
//! * a value is a tag byte and its payload: 0 `Unset`, 1 `Int64`, 2 `Double`, 3 `String`,
//!   4 `Array` (a count and the values), 5 the explicit map marker (a register value, never in
//!   an array), **6 reserved for the implicit map marker**, 7 `Int32`, 8 `Decimal128`, 9 `Bool`, 10 `Date`, 11 `ObjectId`, 12 `Binary`,
//!   13 `Timestamp`, 14 `Regex`, 15 `Null`, 16 `Document` (a count, then each field as a name and
//!   a value; legal ONLY inside an array, at any depth, and refused as a register or a lost
//!   value: a document outside an array is a map. The version 1 form has no tag for it);
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
//! these ten:
//!
//! 1. the **writer table**: a count, then every [`WriterId`] the state mentions, in strictly
//!    increasing order;
//! 2. the **life table**: a count, then every stamp the state uses as a life (a key of `lives`,
//!    a member of `dead`, the life of a register or a counter slot, the life in a `seen`
//!    key), as stamps, in strictly increasing order. A life is written as a varint
//!    index into this table everywhere else, which is what makes a cell cheap: its life is one
//!    byte, not 44;
//! 3. `lives`: a count; each is a life index, the count and the full stamps of the deletes its
//!    creator had seen, the field order as a count of paths, and a kind byte (0 insert,
//!    1 upsert);
//! 4. `dead`: a count and life indexes;
//! 5. `deletes`: a count; each a full stamp and a version vector;
//! 6. `regs` (the cells): a count; each a life index, a path, a flags byte, the register
//!    stamp unless it is the life's, the value, and the generation: its stamp
//!    unless it is the register's, and its `seen` totals (each a key and a typed total). There is
//!    no `obs` map: what a base write had counted of a slot is the slot's absorbed pair (section 7). Flags: bits 0 and 1 are the
//!    generation (0 none, 1 started by this register's own stamp, 2 started by another stamp),
//!    bit 2 is `soi`, bit 3 says the register stamp is the life's, and the other bits are 0;
//! 7. `slots`: a count; each a life index, a path, a writer index, a generation byte (0 none,
//!    1 and then its stamp), the stamp, a FLAG BYTE, the total's payload and, when bit 0 of the flag
//!    byte is set, the ABSORBED pair: a stamp and the absorbed total's payload. The flag byte: bit 0
//!    an absorbed pair is present; bits 1 and 2 the type of the total (0 `Int64`, 1 `Int32`,
//!    2 `Decimal128`); bits 3 and 4 the type of the absorbed total (0 when there is no pair).
//!    Type 3, a type bit with no pair, and any other bit set are refused (`BadTag`);
//! 8. the **version vector**: a count; each a writer index and an HLC;
//! 9. `losses`: a count; each a kind byte, an "about" byte (0 the whole document, 1 a field and
//!    then its path), a stamp and the lost value. The whole document is its own tag, not the
//!    path `_`, so that it cannot be mistaken for a field named `_` (the version 1 form cannot
//!    tell them apart; see `codec.rs`).
//!
//! 10. the **order state** (`ords`, the `FieldOrd` of option A, `ord.rs`): a count, 0 when the
//!     state has none (and only then: a state has one spelling), then each entry keyed `(life index, path)` like a
//!     register (the path as a segment count and the segments, the count 0 being the root of a
//!     replace, `OrdAt::Root`; strictly increasing in the key order of the map). An entry is
//!     ONE floor byte, then its parts:
//!     * the floor byte: 0 no floor; 1 an `$unset` floor `u`; 2 a replace floor `r`; 3 a `u`
//!       floor with the update bit ("the unsetting write also wrote other fields", 0 bytes of
//!       its own). Any other byte is refused. An `r` floor carries no bit and no version vector:
//!       its spelling would be the byte 2 and a bit, which the byte table has no place for, so
//!       "the bit on an `r` floor" is refused as an unknown floor byte;
//!     * for a floor, its stamp; for a `u` floor (1 or 3), then `prev`, the highest HLC the
//!       unsetter had seen, as 10 bytes;
//!     * the per-writer firsts: a count, then each as a writer index (strictly increasing), a
//!       stamp and the position as a varint. The firsts count is a varint after the floor
//!       parts; the floor byte's spare bits are not used for it (one byte for 0 to 127, two for
//!       128 and up);
//!     * a flags byte (bit 0: there is a `last`, bit 1: there is a `lvl`; no other bit), then
//!       the `last` as a stamp and a varint position, then the `lvl` as a stamp.
//!
//! A `seen` key is a life index, a writer index and a generation as in `slots`; its entry is a typed
//! total.
//!
//! # Section 10 always carries its count
//!
//! Section 10 always carries its count, and a count of 0 means no order state; every section has a
//! count, so the body is self-delimiting and any cut of it is refused (`Truncated`), where a section
//! present only when non-empty would have let a cut at the end of the losses pass as the same state
//! without its order state. The version byte is 2 for every state.
//!
//! # Carriers
//!
//! The format detects a cut or an appended byte (`Truncated`, `Trailing`), not a flipped bit inside a
//! section, so what a carrier must add is the integrity check, and the delimiting it already has. Today:
//! a stored record's body "needs no length prefix because it runs to the end of the value"
//! (`docs/storage.md`, the record layout), which the store delimits; a flipped bit that still decodes is
//! NOT detected by a serving node (`docs/operations.md`, "A flipped bit in a page that still decodes":
//! redb verifies its page checksums only in repair, rebuild and `check-store`); the cluster frame is
//! length-prefixed and checked against `MAX_FRAME` before anything is allocated and authenticated with
//! `cluster_secret` (`docs/threat-model.md`, "A frame sized to exhaust memory"; `docs/roadmap.md`, the
//! replication transport). So the carrier delimits and the format is complete under a cut; an integrity
//! check on the compact body (its `compact_hash` next to it) is for Step 2 to add where a bit flip must
//! not pass. No carrier of the compact form exists yet.
//!
//! # Extension points
//!
//! * **Order state** (the `FieldOrd` of option A: floors, per-writer firsts, last, level floor):
//!   section 10, above. Its stamps use the same stamp and writer index primitives. The hash of a
//!   state that carries one is NOT pinned by any golden: they wait for the counter cell.
//! * **Counter cell** (frozen): a slot is its stamp and total and, optionally, the highest
//!   `(stamp, total)` of that very slot that any base write on the path had held (the ABSORBED pair;
//!   section 7). A base write folds into the pairs every slot of the path its writer holds in an
//!   alive life, of any generation. The read side counts only what a slot gained since its pair, and
//!   the field is present when the slot moved since the pair's stamp (B1). The pair is keyed by the
//!   slot, joined by the higher absorbed stamp, and dropped with the slot's life. It replaces the
//!   per-register `obs` map. A state that has a pair has no version 1 spelling (`V1CannotHold`).
//!   A decoded pair is refused above its slot (`AbsorbedAboveSlot`), at the slot's own stamp
//!   with another total (`AbsorbedTotalMismatch`), by another writer than its slot's key
//!   (`AbsorbedWriterMismatch`), and of a wider type than its slot (`AbsorbedWiderThanSlot`); a
//!   slot stamped by another writer than its key is refused too (`SlotWriterMismatch`). The provisional `ESCAPE` of the typed counters is
//!   gone: the type has a place of its own in the flag byte and the type byte.
//! * **Map markers and type clashes**: tag 5 (the explicit marker) is in use, with loss kinds 4
//!   and 5 (`typeClash`, `arrayClash`); tag 6 (the implicit marker) is reserved for the write
//!   rule the next model round decides, and nothing writes it.
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
use super::counter::Num;
use super::ord::{Cand, FieldOrd, Floor, OrdAt};
use super::state::{Gen, Life, LifeKind, MAX_TOTAL, MergeState, ObsKey, Reg, Slot, Vv};
use super::types::{Epoch, LossAt, LossKey, LossKind, LossValue, MStamp, Path, Value, WriterId};
use crate::hlc::Hlc;
use crate::ids::NodeId;

/// The format version byte of this form: for every state, with or without an order state.
const VERSION: u8 = 2;

/// The type code of a counter amount, in the type byte of a typed total and in the slot flag
/// byte: 0 `Int64`, 1 `Int32`, 2 `Decimal128`. 3 is not a type.
fn num_type(n: &Num) -> u8 {
    match n {
        Num::Int64(_) => 0,
        Num::Int32(_) => 1,
        Num::Decimal128(_) => 2,
    }
}

/// The flag byte of a counter slot (section 7): bit 0 an absorbed pair follows the total, bits 1
/// and 2 the type of the total, bits 3 and 4 the type of the absorbed total (0 when there is no
/// pair). The other bits are 0.
const SLOT_ABSORBED: u8 = 1;
const SLOT_TOTAL_TYPE_SHIFT: u8 = 1;
const SLOT_ABSORBED_TYPE_SHIFT: u8 = 3;

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
    /// A typed counter total: the type byte (see [`num_type`]), then the payload.
    fn total(&mut self, n: &Num) {
        self.u8(num_type(n));
        self.num_payload(n);
    }
    /// The payload of a counter total: the zigzag of an integer as a varint, the 16 bytes of a
    /// `Decimal128`. The type is spelled by the caller.
    fn num_payload(&mut self, n: &Num) {
        let zigzag = |w: &mut Self, v: i128| w.varint(((v << 1) ^ (v >> 127)) as u128);
        match n {
            Num::Int64(v) | Num::Int32(v) => zigzag(self, *v),
            Num::Decimal128(b) => self.out.extend_from_slice(b),
        }
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
    /// The key of an order entry: a path, or the root as a segment count of 0.
    fn ord_at(&mut self, at: &OrdAt) {
        match at {
            OrdAt::Root => self.len(0),
            OrdAt::Field(p) => self.path(p),
        }
    }
    /// A candidate: a stamp and the position as a varint.
    fn cand(&mut self, (st, pos): &Cand) {
        self.stamp(st);
        self.varint(u128::from(*pos));
    }
    /// An order entry after its key: the floor byte and its parts, the firsts, the flags byte, `last`
    /// and `lvl` (section 10).
    fn field_ord(&mut self, x: &FieldOrd) {
        match &x.floor {
            None => self.u8(0),
            Some(Floor::U { stamp, prev, others }) => {
                self.u8(if *others { 3 } else { 1 });
                self.stamp(stamp);
                self.hlc(*prev);
            }
            Some(Floor::R { stamp }) => {
                self.u8(2);
                self.stamp(stamp);
            }
        }
        self.len(x.firsts.len());
        for (wr, c) in &x.firsts {
            self.wid(wr);
            self.cand(c);
        }
        self.u8(u8::from(x.last.is_some()) | u8::from(x.lvl.is_some()) << 1);
        if let Some(c) = &x.last {
            self.cand(c);
        }
        if let Some(st) = &x.lvl {
            self.stamp(st);
        }
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
            // 4 is the array; 5 the explicit map marker; 6 is kept for the implicit one.
            Value::Map => self.u8(5),
            Value::Array(a) => {
                self.u8(4);
                self.len(a.len());
                for x in a {
                    self.value(x);
                }
            }
            // 16: a document, legal only inside an array: a count, then each field as a name
            // and a value.
            Value::Document(f) => {
                self.u8(16);
                self.len(f.len());
                for (name, x) in f {
                    self.str(name);
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
    fn obs(&mut self, m: &BTreeMap<ObsKey, Num>) {
        self.len(m.len());
        for (k, t) in m {
            self.obs_key(k);
            self.total(t);
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
        match &k.at {
            LossAt::Document => self.u8(0),
            LossAt::Field(p) => {
                self.u8(1);
                self.path(p);
            }
        }
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
                self.total(t);
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
    /// Sections 3 to 10, with the tables of the pass `w` is in.
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
            let mut flags = num_type(&s.total) << SLOT_TOTAL_TYPE_SHIFT;
            if let Some((_, a)) = &s.absorbed {
                flags |= SLOT_ABSORBED | num_type(a) << SLOT_ABSORBED_TYPE_SHIFT;
            }
            w.u8(flags);
            w.num_payload(&s.total);
            if let Some((st, a)) = &s.absorbed {
                w.stamp(st);
                w.num_payload(a);
            }
        }
        w.vv(&self.vv);
        w.len(self.losses.len());
        for (k, v) in &self.losses {
            w.loss_key(k);
            w.loss_value(v);
        }
        // section 10: the order state, its count first (0 when there is none)
        w.len(self.ords.len());
        for ((l, at), x) in &self.ords {
            w.life(l);
            w.ord_at(at);
            w.field_ord(x);
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
    /// registers, slots, version vector, losses, the order state) behind
    /// its writer and life tables. Mutant memory (`Gen::vv`, `gens`) is not part of it.
    pub fn compact_bytes(&self) -> Vec<u8> {
        self.write_compact_body()
    }

    /// Every writer the state mentions: the writer table of the compact form.
    pub fn writers(&self) -> BTreeSet<WriterId> {
        let mut scan = W::scanning();
        self.write_sections(&mut scan);
        scan.seen_writers
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
        if r.take(3)? != MAGIC {
            return Err(DecodeError::BadHeader);
        }
        if r.u8()? != VERSION {
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
            put(&mut s.regs, (l, p), Reg { stamp, value, generation, soi })?;
        }
        for _ in 0..r.len()? {
            let l = r.life()?;
            let p = r.path()?;
            let wr = r.wid()?;
            let g = r.opt_stamp()?;
            let stamp = r.stamp()?;
            let flags = r.u8()?;
            let (ty, aty) =
                ((flags >> SLOT_TOTAL_TYPE_SHIFT) & 3, (flags >> SLOT_ABSORBED_TYPE_SHIFT) & 3);
            // no stray bit, no type 3, and no absorbed type without an absorbed pair
            if flags & 0xe0 != 0 || ty == 3 || aty == 3 || (flags & SLOT_ABSORBED == 0 && aty != 0)
            {
                return Err(DecodeError::BadTag(flags));
            }
            let total = r.num_payload(ty)?;
            let absorbed = if flags & SLOT_ABSORBED != 0 {
                let st = r.stamp()?;
                Some((st, r.num_payload(aty)?))
            } else {
                None
            };
            put(&mut s.slots, (l, p, wr, g), Slot { stamp, total, absorbed })?;
        }
        s.vv = r.vv()?;
        for _ in 0..r.len()? {
            let k = r.loss_key()?;
            let v = r.loss_value()?;
            put(&mut s.losses, k, v)?;
        }
        // section 10: the count (0 when there is no order state), then the entries
        for _ in 0..r.len()? {
            let l = r.life()?;
            let at = r.ord_at()?;
            let x = r.field_ord()?;
            put(&mut s.ords, (l, at), x)?;
        }
        r.finish()?;
        s.check_decoded()?;
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
    /// A zigzag `u128` as an integer total, of magnitude at most [`MAX_TOTAL`] (see there).
    fn zigzag(z: u128) -> Result<i128, DecodeError> {
        let t = ((z >> 1) as i128) ^ -((z & 1) as i128);
        if t.unsigned_abs() > MAX_TOTAL.unsigned_abs() {
            return Err(DecodeError::TotalOutOfRange);
        }
        Ok(t)
    }
    /// A typed counter total, as `W::total` writes it: the type byte, then the payload.
    fn total(&mut self) -> Result<Num, DecodeError> {
        let t = self.u8()?;
        if t > 2 {
            return Err(DecodeError::BadTag(t));
        }
        self.num_payload(t)
    }
    /// The payload of a counter total of the type code `ty` (0, 1 or 2): see `W::num_payload`.
    fn num_payload(&mut self, ty: u8) -> Result<Num, DecodeError> {
        match ty {
            0 => Ok(Num::Int64(Self::zigzag(self.varint(19)?)?)),
            1 => Ok(Num::Int32(Self::zigzag(self.varint(19)?)?)),
            _ => Ok(Num::Decimal128(self.arr()?)),
        }
    }
    fn str(&mut self) -> Result<String, DecodeError> {
        let n = self.len()?;
        String::from_utf8(self.take(n)?.to_vec()).map_err(|_| DecodeError::BadString)
    }
    fn path(&mut self) -> Result<Path, DecodeError> {
        let n = self.len()?;
        if n > MAX_VALUE_DEPTH {
            return Err(DecodeError::PathTooDeep);
        }
        let mut segments = Vec::new();
        for _ in 0..n {
            segments.push(self.str()?);
        }
        Path::new(segments).map_err(|_| DecodeError::BadPath)
    }
    fn hlc(&mut self) -> Result<Hlc, DecodeError> {
        Ok(Hlc::from_bytes(self.arr()?))
    }
    /// The key of an order entry: a path, or the root (a segment count of 0).
    fn ord_at(&mut self) -> Result<OrdAt, DecodeError> {
        // the count is read as a path does; 0 is the root, which a path cannot be
        let n = self.len()?;
        if n == 0 {
            return Ok(OrdAt::Root);
        }
        if n > MAX_VALUE_DEPTH {
            return Err(DecodeError::PathTooDeep);
        }
        let mut segments = Vec::new();
        for _ in 0..n {
            segments.push(self.str()?);
        }
        Path::new(segments).map(OrdAt::Field).map_err(|_| DecodeError::BadPath)
    }
    /// A candidate: a stamp and a position (a `u64` varint, shortest form).
    fn cand(&mut self) -> Result<Cand, DecodeError> {
        let st = self.stamp()?;
        let pos = u64::try_from(self.varint(10)?).map_err(|_| DecodeError::NotCanonical)?;
        Ok((st, pos))
    }
    /// An order entry after its key (see `W::field_ord`).
    fn field_ord(&mut self) -> Result<FieldOrd, DecodeError> {
        let floor = match self.u8()? {
            0 => None,
            k @ (1 | 3) => {
                let stamp = self.stamp()?;
                Some(Floor::U { stamp, prev: self.hlc()?, others: k == 3 })
            }
            2 => Some(Floor::R { stamp: self.stamp()? }),
            b => return Err(DecodeError::OrdBadFloor(b)),
        };
        let mut firsts = BTreeMap::new();
        for _ in 0..self.len()? {
            let wr = self.wid()?;
            put(&mut firsts, wr, self.cand()?)?;
        }
        let flags = self.u8()?;
        if flags > 3 {
            return Err(DecodeError::BadTag(flags));
        }
        let last = if flags & 1 != 0 { Some(self.cand()?) } else { None };
        let lvl = if flags & 2 != 0 { Some(self.stamp()?) } else { None };
        Ok(FieldOrd { floor, firsts, last, lvl })
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
    /// An index into the writer or the life table: a `u64` varint like a count, but NOT a count, so
    /// it is not bounded by what is left of the input (the last writer of a table of 13 is index 12,
    /// and fewer than 12 bytes can follow it).
    fn index(&mut self) -> Result<usize, DecodeError> {
        usize::try_from(self.varint(10)?).map_err(|_| DecodeError::BadIndex)
    }
    fn wid(&mut self) -> Result<WriterId, DecodeError> {
        let i = self.index()?;
        let w = *self.writers.get(i).ok_or(DecodeError::BadIndex)?;
        // Once the tables are read `writer_used` is as long as `writers`; while the life
        // table itself is read it is, so the mark cannot miss.
        self.writer_used[i] = true;
        Ok(w)
    }
    fn life(&mut self) -> Result<MStamp, DecodeError> {
        let i = self.index()?;
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
            // The map marker is a register value, a document an array element: the other way
            // round is refused (the depth is the number of arrays and documents above).
            5 if depth == 0 => Ok(Value::Map),
            16 if depth > 0 => {
                if depth >= MAX_VALUE_DEPTH {
                    return Err(DecodeError::TooDeep);
                }
                let mut f = Vec::new();
                for _ in 0..self.len()? {
                    let name = self.str()?;
                    f.push((name, self.value_at(depth + 1)?));
                }
                Ok(Value::Document(f))
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
    fn obs(&mut self) -> Result<BTreeMap<ObsKey, Num>, DecodeError> {
        let mut m = BTreeMap::new();
        for _ in 0..self.len()? {
            let k = self.obs_key()?;
            put(&mut m, k, self.total()?)?;
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
        let at = match self.u8()? {
            0 => LossAt::Document,
            1 => LossAt::Field(self.path()?),
            t => return Err(DecodeError::BadTag(t)),
        };
        Ok(LossKey { kind, at, stamp: self.stamp()? })
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
