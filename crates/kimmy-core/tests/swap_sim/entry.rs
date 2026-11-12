//! Entries, physicals and jobs (`swapmodel5.py` 209-246). A stamp is the product's `MStamp`
//! (`hlc || node || epoch`, the frozen oplog key order); a model member is a node whose letter orders as the
//! model's origin name does, with epoch 0 (only the restore scenario changes an epoch).

use std::rc::Rc;

use kimmy_core::hlc::Hlc;
use kimmy_core::ids::NodeId;
use kimmy_core::merge_state::{Epoch, MStamp, WriterId};

use kimmy_core::merge_state::MergeState;

use crate::omap::OMap;

pub const L: &str = "L";
pub type Stamp = MStamp;
pub type EntryId = usize;
pub type EntryRef = Rc<Entry>;
/// A document body: a list of marks, or `None` for a delete.
pub type Body = Option<Rc<Vec<String>>>;

pub fn node_of(origin: usize) -> NodeId {
    NodeId::from_bytes([b'A' + origin as u8; 16])
}

/// The writer identity of model member `origin` (node letter, epoch 0).
pub fn writer(origin: usize) -> WriterId {
    WriterId { node: node_of(origin), epoch: Epoch::from_u64(0) }
}

pub fn stamp(hlc: i64, origin: usize) -> Stamp {
    assert!(hlc >= 1, "model HLCs start at 1");
    MStamp { hlc: Hlc::new(hlc as u64, 0), writer: writer(origin) }
}

/// "Minus infinity": the identity physical counts as bound there.
pub fn neg() -> Stamp {
    MStamp {
        hlc: Hlc::ZERO,
        writer: WriterId { node: NodeId::from_bytes([0; 16]), epoch: Epoch::from_u64(0) },
    }
}

pub fn hlc_of(s: &Stamp) -> i64 {
    s.hlc.wall_ms as i64
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Doc,
    Create,
    Bind,
    Abandon,
    Ldrop,
    Lcreate,
    Loss,
    /// `field`: one of Q7's own loss records (`clash`, `deleteWon`, `insertLost`, ...) as an entry (V6M2). Never a document
    /// write, never evaluated for lateness, always appended and served onward (D3).
    LossQ7,
}

impl Entry {
    /// A Q6 kind (Q6 v2.5 FIX-2): a binding, a recreate or a logical drop (the register events), a stage's create, an abandon
    /// or a loss record. A document entry (and Q7's own loss entries) is not one.
    pub fn is_q6_kind(&self) -> bool {
        matches!(
            self.kind,
            Kind::Bind | Kind::Lcreate | Kind::Ldrop | Kind::Create | Kind::Abandon | Kind::Loss
        )
    }
}

impl Entry {
    /// The product's `OpKind` this entry is written as (H-Q6-K1, FIX-2 item 4: the stage's create is ASSUMED to be a
    /// `CreateCollection`; the harness has no product oplog, so this is the mapping the build must confirm by decoding a real
    /// replacing `$out`). Register events and records have no product kind yet (no code path today): `None`.
    pub fn product_op_kind(&self, f: &crate::flags::Flags) -> Option<kimmy_core::OpKind> {
        use kimmy_core::OpKind;
        match self.kind {
            Kind::Create if f.fm_create_not_ddl => Some(OpKind::Insert), // MUTANT
            Kind::Create => Some(OpKind::CreateCollection),
            Kind::Doc => Some(match self.opk {
                Some(Opk::Insert) => OpKind::Insert,
                Some(Opk::Replace) => OpKind::Replace,
                _ => OpKind::Delete,
            }),
            _ => None,
        }
    }
}

impl Kind {
    /// A register event (the swap register's: a binding, a recreate or a logical drop).
    pub fn is_register_event(self) -> bool {
        matches!(self, Kind::Bind | Kind::Lcreate | Kind::Ldrop)
    }

    pub fn name(self) -> &'static str {
        match self {
            Kind::Doc => "doc",
            Kind::Create => "create",
            Kind::Bind => "bind",
            Kind::Abandon => "abandon",
            Kind::Ldrop => "ldrop",
            Kind::Lcreate => "lcreate",
            Kind::Loss => "loss",
            Kind::LossQ7 => "loss_q7",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Opk {
    Insert,
    Replace,
    Delete,
}

/// The key of a loss record: `(key, loser stamp)`, or `(key,)` under `m-loss-key-collapse`, or
/// `(key, stamp, minter)` under `m-record-per-minter`. `head()` is the model's `k[:2]`.
///
/// Under the `field` policy a record is per lost head, so the key also names the `path` (an index into the world's path
/// table; `None` for the `document` policy and for a whole-document lost write such as a delete).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct LKey {
    pub key: u32,
    pub path: Option<u16>,
    pub stamp: Option<Stamp>,
    pub minter: Option<usize>,
}

/// What identifies a lost head: `(_id, path, loser stamp)`.
pub type HeadKey = (u32, Option<u16>, Option<Stamp>);

impl LKey {
    pub fn plain(key: u32, stamp: Stamp) -> Self {
        Self { key, path: None, stamp: Some(stamp), minter: None }
    }
    pub fn at(key: u32, path: Option<u16>, stamp: Stamp) -> Self {
        Self { key, path, stamp: Some(stamp), minter: None }
    }
    pub fn head(&self) -> HeadKey {
        (self.key, self.path, self.stamp)
    }
}

/// What the model keeps in `Entry.extra`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Extra {
    None,
    /// loss: the physical of the lost write; lcreate: the physical the creator last served
    Phys(String),
    /// bind: the writer's staged (count, rows) digest, read only by `m-digest`
    Digest(Vec<(u32, Stamp)>),
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub idx: EntryId,
    pub origin: usize,
    pub hlc: i64,
    pub stamp: Stamp,
    pub kind: Kind,
    pub phys: String,
    pub key: u32,
    pub lkey: Option<LKey>,
    pub body: Body,
    pub opk: Option<Opk>,
    pub staging: bool,
    pub dl: i64,
    pub extra: Extra,
    /// `field` policy: the writer's whole merge state after the write (Q7 D1/D11)
    pub fstate: Option<Rc<MergeState>>,
    /// a `LossQ7` entry: the record and its value
    pub q7: Option<(kimmy_core::merge_state::LossKey, kimmy_core::merge_state::LossValue)>,
    /// a stage's create: the `mergePolicy` it carries (D8)
    pub merge_policy: Option<crate::world::Policy>,
}

impl Entry {
    pub fn new(origin: usize, hlc: i64, kind: Kind) -> Self {
        Self {
            idx: usize::MAX,
            origin,
            hlc,
            stamp: stamp(hlc, origin),
            kind,
            phys: String::new(),
            key: 0,
            lkey: None,
            body: None,
            opk: None,
            staging: false,
            dl: 0,
            extra: Extra::None,
            fstate: None,
            q7: None,
            merge_policy: None,
        }
    }
    pub fn phys(mut self, p: &str) -> Self {
        self.phys = p.to_string();
        self
    }
    pub fn doc(mut self, key: u32, body: Body, opk: Opk) -> Self {
        self.key = key;
        self.body = body;
        self.opk = Some(opk);
        self
    }
    pub fn staging(mut self) -> Self {
        self.staging = true;
        self
    }
    pub fn dl(mut self, dl: i64) -> Self {
        self.dl = dl;
        self
    }
    pub fn extra(mut self, x: Extra) -> Self {
        self.extra = x;
        self
    }
}

impl std::fmt::Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({},{}):{}:{}", self.kind.name(), self.hlc, self.origin, self.phys, self.key)
    }
}

/// One version of a document in a physical.
#[derive(Clone, Debug)]
pub struct Ver {
    /// `document`: the write's stamp; `field`: the latest write merged in (Q7 D11)
    pub stamp: Stamp,
    /// the visible body (`None` for a deleted document)
    pub body: Body,
    /// `field`: the merge state (joined with any unfinished join's page states)
    pub state: Option<Rc<MergeState>>,
    /// `field`: the state built by entries delivered in position only, while an unfinished join's page states are
    /// held (the gap-1 analogue, `held_records`); `None` when there is no such split
    pub applied: Option<Rc<MergeState>>,
}

impl Ver {
    pub fn doc(stamp: Stamp, body: Body) -> Self {
        Self { stamp, body, state: None, applied: None }
    }
}

/// Convergence of a version: the stamp, the visible body and (`field`) the version vector. The records a join detects
/// (`MergeState.losses`) are left out: they converge through the loss entries, not through the join.
impl PartialEq for Ver {
    fn eq(&self, o: &Self) -> bool {
        self.stamp == o.stamp
            && self.body == o.body
            && self.state.as_ref().map(|s| &s.vv) == o.state.as_ref().map(|s| &s.vv)
    }
}

pub type Docs = OMap<u32, Ver>;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PKind {
    Id,
    Stage,
    Rc,
}

#[derive(Clone, Debug)]
pub struct Phys {
    pub kind: PKind,
    pub alive: bool,
    pub docs: Docs,
    pub kill: Option<Stamp>,
    pub kept: bool,
    pub missing: bool,
    pub orphan: bool,
    pub ghost: Option<Docs>,
}

impl Phys {
    pub fn new(kind: PKind) -> Self {
        Self {
            kind,
            alive: true,
            docs: Docs::new(),
            kill: None,
            kept: false,
            missing: false,
            orphan: false,
            ghost: None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JobKind {
    Good,
    Late,
    CrashAbandon,
    Vanish,
}

#[derive(Clone, Debug)]
pub struct Job {
    pub writer: usize,
    pub keys: Vec<u32>,
    pub kind: JobKind,
    pub bind_step: i64,
    pub stage: String,
    pub dl: i64,
    pub digest: Vec<(u32, Stamp)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamps_order_as_the_model_orders_them() {
        // the model's stamp is (hlc, origin name); the frozen order is hlc, node, epoch
        assert!(stamp(3, 0) < stamp(3, 1));
        assert!(stamp(3, 1) < stamp(4, 0));
        assert!(neg() < stamp(1, 0));
        assert_eq!(hlc_of(&stamp(17, 2)), 17);
    }
}
