//! The idempotency model test (B5, design "Tests > Model test"): three engines run
//! random, seeded schedules of keyed writes, retries, crashes, pulls, snapshots,
//! drops and retention, and invariants I1 to I7 are asserted after every step.
//!
//! **Members.** Three engines in one process, opened from template stores whose node
//! identities are fixed (`0x11..`, `0x22..`, `0x33..`), so stamps tie-break the same
//! way on every run; a member reset is a fresh store with a spare fixed identity
//! (`0x44..` on), as a wiped node is given a new one. Every engine commits `multi`
//! writes in chunks of two documents.
//!
//! **Clocks.** Every local step carries a time `t`, milliseconds past a base an hour
//! ahead of the wall clock, and the member's clock is moved to it first
//! (`advance_clock_past`), as `convergence_model_tests` does: skew between members is
//! a choice of `t`, drawn per member for the schedule and changed now and then. A
//! request's clock can also move between two of its commits (a `Clock` step inside
//! it), so its later chunks are stamped later. The life rule's wall clock is global:
//! a retention step moves it, and every member's lookups with it, to its own time,
//! and the schedule's times jump past the window with it.
//!
//! **Data.** `shop.orders` holds five counters `{_id: i, n: 0, ks: []}`, written once
//! before the schedule and replicated everywhere; only keyed `$inc` requests change
//! them (multi, chunked by two, or single), and `orders` is never dropped.
//! `shop.items` takes keyed inserts with and without `_id` (the derived one), bulk
//! inserts of two, replaces and deletes by id, and is dropped and created again.
//!
//! **Key sets.** Every version a keyed request writes carries, in its `ks` field, the
//! requests whose applications it contains: the base's list and the request itself,
//! appended even when present (so a second application is never an unchanged no-op).
//! A merge keeps the winner's body, and so the winner's set. A tombstone's set is
//! kept by the test, by its stamp.
//!
//! **Steps.** Send a request (a first send or a retry) to a member, with a crash at
//! one of its commits (`AfterCommit`: the drain deadline passes, so the request's next
//! transaction is refused; `BeforeCommit`: the next commit's first write fails;
//! `FsyncFails`: the next commit's fsync fails, its outcome unknown), after which the
//! member is reopened, and with steps run between any two of its commits (pulls in any
//! direction, a concurrent attempt of the same request on another member, a page, a
//! restart of another member, its own clock moving); pull a window, or a short one
//! (cut at two entries); take a scoped or converge page of a collection, optionally
//! followed by a restart; reset a member (a fresh store, then a whole-database
//! snapshot); restart or crash a member; drop or create `items`; and a retention pass
//! with a moved clock. "Between two commits" is the engine's own commit point
//! (`barrier_hooks::Point::Released`, after every commit, durable by then), so it is
//! between any two commits whatever the code under test makes them.
//!
//! **Like the server**, no keyed request goes to a member that is catching up after a
//! whole-database snapshot (until its witnessed vector covers every peer's version
//! vector), and none once its key's memory ended: a retention step closes an era, and
//! a request of an earlier era is not sent again (a retry then is a new request,
//! guarantee 7).
//!
//! **Tracks.** `Home`: every `$inc` request and every retry of it goes to one member
//! (the home), each is driven to completion there before the next one starts and
//! before a retention step, and I5 holds exactly. `Anywhere`: retries land on any
//! member, concurrent attempts on two members included, and I6 bounds the counters.
//!
//! **The invariants**, after every step unless said otherwise:
//! - **I1**, never on its own effect: no application of K writes on a base whose set
//!   holds K. Checked where it happens (the `$inc`'s `apply` sees its base; the test
//!   reads the base of an insert, replace or delete just before it) and on every
//!   stored version (no list holds a request twice). Not counted: K's record belongs to
//!   an earlier life of the collection on that member (its greatest `last` at or below
//!   the member's tombstone or floor: the life rule, under which a retry after a drop
//!   applies afresh).
//! - **I2**, the record with the effect: a member that processed an entry of K by
//!   history (its own, or one a pull shipped it and its coverage then passed) holds
//!   K's live record with `first` at or below that entry, unless the entry is of an
//!   earlier life there or K's memory ended; and a member that completed a snapshot
//!   holds every live record the sender held for what the snapshot carried, merged
//!   (`first` at or below, `last` at or above).
//! - **I3**, once per member: on one member, a request writes each document at most
//!   once across all its attempts (counted from the entries each node minted).
//! - **I4**, after quiescence: identical documents, identical live records, and at
//!   most one document per (request, position) of a keyed insert.
//! - **I5** (`Home`), after quiescence: each counter is the number of `$inc` requests
//!   accepted for it.
//! - **I6**: no counter is ever above the number of `$inc` requests sent for it.
//! - **I7**: a keyed insert never lands on a derived `_id` another request's insert
//!   landed on, and no live inserted document at a derived `_id` was written by
//!   another request.
//! - **Life**: no replay answers from a record of an earlier life (`last` at or below
//!   the member's tombstone or floor of the collection), or one past its window.
//!
//! **Finding G1** (reported; a design change is pending): a member that took a
//! collection by a whole-database snapshot relays the later versions it holds as state
//! without the record of the request whose effect they contain (a page's record is
//! rowed at its `last`, and a window carries only the rows at the stamps it ships), and
//! the relay's coverage passes the request's own entries, so the receiver never gets
//! the record. A retry there applies on top of the effect. The model classifies exactly
//! that shape (an application on its own effect by a member that held no live record of
//! the key before it and processed no entry of the request of the current life) as G1
//! and counts it; with `IDEM_STRICT=1` it fails the run as I1 (and I4, I5).
//! `finding_g1_*` is its reproducer.

// Members are indices into three-element arrays (`LETTERS`, the members, a step's
// records) all through: a range reads as "every member", an enumeration would not.
#![allow(clippy::needless_range_loop)]

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, Mutex, OnceLock};

use bson::{Bson, Document, doc};
use kimmy_core::{CollectionId, DocId, Fingerprint, Hlc, NodeId, OpKind, ScopeHash, Stamp};

use crate::engine::barrier_hooks::{self, Point};
use crate::idempotency::{IdempotencyRecord, Keyed, KeyedWrite, RecordsVia};
use crate::{
    Candidates, CollectionMeta, Engine, MarkedRange, ModifySpec, PeerWindow, RetentionPolicy,
    ServeVerdict, SnapshotProgress, StorageError, SyncOutcome, WalkScope, tables,
};

const DB: &str = "shop";
const NAMES: [&str; 2] = ["orders", "items"];
const ORDERS: u8 = 0;
const ITEMS: u8 = 1;
const LETTERS: [char; 3] = ['A', 'B', 'C'];
/// Counters in `orders`.
const COUNTERS: i64 = 5;
/// Documents per chunk of a `multi` write.
const CHUNK: usize = 2;
/// The key memory, `storage.oplog_retention_secs`, for records and entries alike.
const WINDOW_SECS: u64 = 1;
/// Tombstones outlive every run: a resurrection after an age-out is ADR-085's, not
/// this test's.
const TOMBSTONE_SECS: u64 = 24 * 60 * 60;
const BATCH: usize = 1024;
/// A batch cut: the window a short pull takes.
const SHORT: usize = 2;
const QUIESCE_ROUNDS: usize = 8;
/// Fresh identities a schedule's resets can take.
const SPARES: usize = 6;
/// A client `_id` in `items` is `ITEM_BASE + d`.
const ITEM_BASE: i64 = 100;

// -- Schedules ------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Track {
    Home,
    Anywhere,
}

/// A document of `items` a replace or delete addresses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Target {
    /// `ITEM_BASE + d`.
    Client(u8),
    /// The derived `_id` of request `r`'s insert at `pos`.
    Derived(u8, u8),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// An insert into `items`, with the client `_id` `ITEM_BASE + d` or the derived one.
    Insert {
        id: Option<u8>,
    },
    /// Two documents into `items`, both without `_id`.
    Bulk,
    /// `$inc` of every counter, chunked by two.
    IncMulti,
    /// `$inc` of counter `d`, the single-document form.
    IncSingle {
        d: u8,
    },
    /// A replace of an `items` document, upserting.
    Replace {
        at: Target,
    },
    Delete {
        at: Target,
    },
}

impl Kind {
    fn coll(&self) -> u8 {
        match self {
            Kind::IncMulti | Kind::IncSingle { .. } => ORDERS,
            _ => ITEMS,
        }
    }

    fn is_inc(&self) -> bool {
        self.coll() == ORDERS
    }

    /// Whether counter `d` is one it changes.
    fn targets(&self, d: i64) -> bool {
        match self {
            Kind::IncMulti => true,
            Kind::IncSingle { d: x } => i64::from(*x) == d,
            _ => false,
        }
    }
}

/// A request: its key (the scope), what it does, and (implicitly) its fingerprint,
/// one per request (`r + 1`). A key reused for another request has the first one's
/// `key`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Request {
    pub key: u8,
    pub kind: Kind,
}

impl Request {
    fn scope(&self) -> ScopeHash {
        ScopeHash([self.key; 32])
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Crash {
    /// The drain deadline passes after the commit: the request's next transaction is
    /// refused.
    AfterCommit,
    /// The next commit's first write fails: it is not there.
    BeforeCommit,
    /// The next commit's fsync fails: it may be there.
    FsyncFails,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    /// Request `r` to member `m`. `crash`: at commit `j` (0: before the first), and the
    /// member is reopened after the request. `inner`: steps run after the request's
    /// `j`-th commit; those it never reaches run after it returns.
    Send {
        r: u8,
        m: u8,
        crash: Option<(u8, Crash)>,
        inner: Vec<(u8, Vec<Op>)>,
    },
    Pull {
        to: u8,
        from: u8,
    },
    PullShort {
        to: u8,
        from: u8,
    },
    /// A scoped page of collection `c` (a converge page with `converge`), then a
    /// restart of the receiver with `restart`.
    Page {
        to: u8,
        from: u8,
        c: u8,
        converge: bool,
        restart: bool,
    },
    /// A fresh store with a new identity, then a whole-database snapshot from `from`.
    Reset {
        m: u8,
        from: u8,
    },
    Restart {
        m: u8,
    },
    /// Dropped without a close: the next open repairs.
    Crash {
        m: u8,
    },
    /// Drop `items`.
    Drop {
        m: u8,
    },
    /// Create `items`.
    Create {
        m: u8,
    },
    /// A retention pass on `m` at `t`, which the life rule's clock moves to everywhere.
    Retention {
        m: u8,
    },
    /// Move `m`'s clock to `t`.
    Clock {
        m: u8,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Op {
    pub step: Step,
    pub t: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct Schedule {
    pub track: Track,
    /// The `Home` track's member for every `$inc`.
    pub home: u8,
    pub requests: Vec<Request>,
    pub ops: Vec<Op>,
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let l = |m: u8| LETTERS[m as usize];
        let t = self.t;
        match &self.step {
            Step::Send { r, m, crash, inner } => {
                write!(f, "{}.send(r{r})@{t}", l(*m))?;
                if let Some((j, how)) = crash {
                    write!(f, " crash {how:?} at {j}")?;
                }
                for (j, ops) in inner {
                    let shown: Vec<String> = ops.iter().map(|op| op.to_string()).collect();
                    write!(f, " [at {j}: {}]", shown.join(", "))?;
                }
                Ok(())
            }
            Step::Pull { to, from } => write!(f, "{}<-{}", l(*to), l(*from)),
            Step::PullShort { to, from } => write!(f, "{}<-{} short", l(*to), l(*from)),
            Step::Page { to, from, c, converge, restart } => write!(
                f,
                "{}<-{} {}({}){}",
                l(*to),
                l(*from),
                if *converge { "converge" } else { "scoped" },
                NAMES[*c as usize],
                if *restart { " then restart" } else { "" }
            ),
            Step::Reset { m, from } => write!(f, "{}.reset from {}", l(*m), l(*from)),
            Step::Restart { m } => write!(f, "{}.restart", l(*m)),
            Step::Crash { m } => write!(f, "{}.crash", l(*m)),
            Step::Drop { m } => write!(f, "{}.drop(items)@{t}", l(*m)),
            Step::Create { m } => write!(f, "{}.create(items)@{t}", l(*m)),
            Step::Retention { m } => write!(f, "{}.retention@{t}", l(*m)),
            Step::Clock { m } => write!(f, "{}.clock@{t}", l(*m)),
        }
    }
}

fn fingerprint(r: u8) -> Fingerprint {
    Fingerprint([r.wrapping_add(1); 32])
}

fn keyed_write(requests: &[Request], r: u8) -> KeyedWrite {
    KeyedWrite::new(requests[r as usize].scope(), fingerprint(r))
}

fn coll_id(c: u8) -> CollectionId {
    CollectionId::derive(DB, NAMES[c as usize])
}

// -- Templates and members ------------------------------------------------------------------

/// The three member stores and the spares, empty, with fixed identities, opened once
/// and copied per run.
fn templates() -> &'static [PathBuf; 3 + SPARES] {
    static TEMPLATES: OnceLock<(tempfile::TempDir, [PathBuf; 3 + SPARES])> = OnceLock::new();
    &TEMPLATES
        .get_or_init(|| {
            let dir = tempfile::tempdir().unwrap();
            let paths = std::array::from_fn(|i| {
                let path = dir.path().join(format!("member{i}.redb"));
                Engine::open(&path).unwrap().close().expect("a fresh store closes cleanly");
                let db = redb::Database::open(&path).unwrap();
                let txn = db.begin_write().unwrap();
                {
                    let mut meta = txn.open_table(tables::META).unwrap();
                    let id = [0x11u8 * (i as u8 + 1); 16];
                    meta.insert(tables::META_NODE_ID, id.as_slice()).unwrap();
                }
                txn.commit().unwrap();
                drop(db);
                path
            });
            (dir, paths)
        })
        .1
}

struct Member {
    _dir: tempfile::TempDir,
    path: PathBuf,
    engine: Option<Arc<Engine>>,
    /// After a whole-database snapshot, until it holds what its peers hold: no keyed
    /// request is sent to it.
    catching_up: bool,
}

// -- Findings --------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Inv {
    I1,
    I2,
    I3,
    I4,
    I5,
    I6,
    I7,
    Life,
    /// The run itself failed: an unexpected error, or a cluster that never settled.
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Failure {
    pub inv: Inv,
    pub detail: String,
}

/// What one run found.
#[derive(Debug, Default)]
pub(crate) struct Outcome {
    pub failures: Vec<Failure>,
    pub notes: Vec<String>,
    /// Steps run, steps between commits and drives included.
    pub steps: usize,
    /// Violations classified as finding G1.
    pub g1: usize,
    /// Effects of the current life whose record a drop buried by its `last`: finding G2.
    pub g2: usize,
    /// Settled records that differ only as finding G3: an earlier state of the
    /// merged record, with its `first`.
    pub g3: usize,
    /// Applications afresh on an earlier life's effect, which the life rule allows.
    pub life_exempt: usize,
    /// Of those and of I2's exemptions, the ones only a drop made elsewhere explains: the
    /// member had not yet received it (finding D1, see the module docs).
    pub dropped_elsewhere: usize,
    pub replays: usize,
    pub resumes: usize,
    pub crashes: usize,
    /// Steps run between two commits of a request.
    pub between: usize,
    /// Settled documents that differ only by a retired identity's versions (R1).
    pub retired: usize,
}

type Res<T> = std::result::Result<T, String>;

fn err(what: &str, e: impl std::fmt::Display) -> String {
    format!("{what}: {e}")
}

/// `IDEM_TRACE=1`: every window, its entries and its records, in the notes.
fn trace() -> bool {
    std::env::var("IDEM_TRACE").is_ok_and(|v| v == "1")
}

fn strict() -> bool {
    std::env::var("IDEM_STRICT").is_ok_and(|v| v == "1")
}

// -- The `$inc` --------------------------------------------------------------------------------

/// What an `$inc`'s `apply` saw: the documents whose base already held its request.
/// Each with the phase of the request it happened in: how many of its commit points had
/// run their steps.
type OwnEffect = Arc<Mutex<Vec<(DocId, Vec<i64>, usize)>>>;

/// `$inc` of `n` by one on the counters it targets, appending the request to the
/// version's key set. A base whose set already holds the request is logged: an
/// application on its own effect (I1), seen where it happens.
struct Inc {
    r: i64,
    target: Option<i64>,
    on_own_effect: OwnEffect,
    phase: Arc<std::sync::atomic::AtomicUsize>,
}

fn ks_of(doc: &Document) -> Vec<i64> {
    match doc.get("ks") {
        Some(Bson::Array(items)) => items.iter().filter_map(Bson::as_i64).collect(),
        _ => Vec::new(),
    }
}

fn with_ks(mut doc: Document, ks: &[i64]) -> Document {
    doc.insert("ks", ks.iter().map(|k| Bson::Int64(*k)).collect::<Vec<_>>());
    doc
}

fn id_of(doc: &Document) -> DocId {
    DocId::try_from_bson(doc.get("_id").expect("a document with an `_id`")).expect("an id")
}

impl ModifySpec for Inc {
    fn matches(&self, doc: &Document) -> kimmy_core::Result<bool> {
        if !doc.contains_key("n") {
            return Ok(false);
        }
        Ok(match self.target {
            Some(d) => doc.get_i64("_id").is_ok_and(|id| id == d),
            None => true,
        })
    }
    fn compare(&self, _: &Document, _: &Document) -> Ordering {
        Ordering::Equal
    }
    fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
        let mut ks = ks_of(doc);
        if ks.contains(&self.r) {
            let phase = self.phase.load(std::sync::atomic::Ordering::SeqCst);
            self.on_own_effect.lock().unwrap().push((id_of(doc), ks.clone(), phase));
        }
        ks.push(self.r);
        let mut next = doc.clone();
        next.insert("n", doc.get_i64("n").map_err(|e| e.to_string())? + 1);
        Ok(Some(with_ks(next, &ks)))
    }
    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        None
    }
}

// -- The ghost -------------------------------------------------------------------------------

/// What happened, as the test saw it.
#[derive(Default)]
struct Ghost {
    /// Every entry a keyed request minted: its request, collection and document.
    entries: HashMap<Stamp, (u8, u8, DocId)>,
    /// A tombstone's key set, by its stamp.
    tomb_ks: HashMap<Stamp, Vec<i64>>,
    /// (node, entry) for every entry of a keyed request a node took by history, with
    /// the order it was taken in.
    processed: HashMap<(NodeId, Stamp), u64>,
    seq: u64,
    /// Entries of request `r` a node minted, per document (I3).
    writes: HashMap<(NodeId, u8, u8, Vec<u8>), Vec<Stamp>>,
    /// The greatest `last` seen on any record of request `r` (by its fingerprint).
    last: HashMap<u8, Stamp>,
    /// The request whose insert first landed on a derived `_id` (I7).
    owner: HashMap<String, u8>,
    /// (collection, document key, request) of every application on its own effect
    /// the life rule allows or that is finding G1: a stored double it explains.
    explained: HashSet<(u8, Vec<u8>, u8)>,
    /// Every drop of `items` minted, as its tombstone's stamp.
    drops: Vec<(u8, Stamp)>,
    /// (node, entry) I2 let pass because a drop elsewhere ended its life (D1).
    dropped_elsewhere: HashSet<(NodeId, Stamp)>,
    /// (node, entry) of the current life whose record is buried (finding G2).
    g2: HashSet<(NodeId, Stamp)>,
    /// (node, entry) a sender holding the record shipped without it (finding G1b).
    g1b: HashSet<(NodeId, Stamp)>,
    /// Those G1b entries I2 has counted.
    g1b_counted: HashSet<(NodeId, Stamp)>,
}

impl Ghost {
    fn mark_processed(&mut self, node: NodeId, stamp: Stamp) {
        if !self.processed.contains_key(&(node, stamp)) {
            self.seq += 1;
            self.processed.insert((node, stamp), self.seq);
        }
    }
}

// -- A prepared request ----------------------------------------------------------------------

enum Call {
    Insert(Document),
    Bulk(Vec<Document>),
    Inc { target: Option<i64>, stop: Option<usize> },
    Replace(DocId, Document),
    Delete(DocId),
}

struct Prepared {
    r: u8,
    m: usize,
    /// The member's engine, let go before a crash's reopen.
    engine: Option<Arc<Engine>>,
    node: NodeId,
    coll: CollectionMeta,
    keyed: KeyedWrite,
    call: Call,
    /// The clock's last stamp before the call: what it mints is above it.
    before: Hlc,
    /// The key set and stamp of each document an insert, replace or delete writes,
    /// before it.
    base: Vec<(DocId, Vec<i64>, Option<Stamp>)>,
    /// The member's live record of the key before the call.
    pre_record: Option<IdempotencyRecord>,
    /// The member's tombstone or floor of the collection before the call.
    floor: Hlc,
    /// The greatest `last` of the request's records seen before the call.
    last_before: Option<Stamp>,
    on_own_effect: OwnEffect,
    /// The commit points whose steps have run, and the ghost's order after each (the
    /// first before the call): what a member had taken when it wrote.
    phase: Arc<std::sync::atomic::AtomicUsize>,
    phase_seq: Rc<RefCell<Vec<u64>>>,
}

// One per request, moved once.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum Answer {
    /// Whether it changed anything, and whether it resumed an earlier attempt.
    Fresh {
        wrote: bool,
        resumed: bool,
    },
    Replayed(IdempotencyRecord),
    Failed(StorageError),
}

fn map_keyed<T>(k: Keyed<T>, f: impl FnOnce(T) -> (bool, bool)) -> Answer {
    match k {
        Keyed::Fresh(v) => {
            let (wrote, resumed) = f(v);
            Answer::Fresh { wrote, resumed }
        }
        Keyed::Replayed(record) => Answer::Replayed(record),
    }
}

fn call(p: &Prepared) -> Answer {
    let e = p.engine.as_ref().expect("the request's engine");
    let answer = match &p.call {
        Call::Insert(doc) => {
            e.insert_keyed(&p.coll, doc.clone(), &p.keyed).map(|k| map_keyed(k, |_| (true, false)))
        }
        Call::Bulk(docs) => e
            .insert_many_keyed(&p.coll, docs.clone(), &p.keyed)
            .map(|k| map_keyed(k, |_| (true, false)))
            .map_err(|e| e.source),
        Call::Inc { target, stop } => {
            let spec = Inc {
                r: i64::from(p.r),
                target: *target,
                on_own_effect: p.on_own_effect.clone(),
                phase: p.phase.clone(),
            };
            e.modify_where_keyed(&p.coll, &Candidates::Scan, &spec, *stop, &p.keyed)
                .map(|k| map_keyed(k, |o| (o.modified > 0, o.resumed)))
        }
        Call::Replace(id, doc) => e
            .replace_keyed(&p.coll, id, doc.clone(), true, None, &p.keyed)
            .map(|k| map_keyed(k, |o| (o.modified || o.upserted, false))),
        Call::Delete(id) => e
            .delete_keyed(&p.coll, id, None, &p.keyed)
            .map(|k| map_keyed(k, |o| (o.is_some(), false))),
    };
    answer.unwrap_or_else(Answer::Failed)
}

/// A stored row, live or a tombstone: its stamp and, when live, its body.
fn stored(e: &Engine, c: u8, id: &DocId) -> Option<(Stamp, Option<Document>)> {
    use redb::ReadableDatabase;
    let key = crate::docs::doc_key(id).ok()?;
    let txn = e.db().begin_read().unwrap();
    let docs = txn.open_table(tables::DOCS).unwrap();
    let raw = docs.get((coll_id(c).0, key.as_slice())).unwrap()?;
    let record = crate::codec::decode_doc_record(raw.value()).unwrap();
    if record.is_live() {
        Some((record.stamp, record.document().unwrap()))
    } else {
        Some((record.stamp, None))
    }
}

/// What a request may answer without a crash: a key reused for another request, a
/// duplicate `_id`, a collection that is gone. Anything else fails the run.
fn expected_failure(e: &StorageError) -> bool {
    matches!(e, StorageError::IdempotencyKeyReused | StorageError::Core(_))
}

// -- The world -------------------------------------------------------------------------------

struct World {
    sched: Schedule,
    members: Vec<Member>,
    base: u64,
    /// The life rule's wall clock, once a retention step has set it.
    now: Option<u64>,
    era: u32,
    /// The era of each request's first send.
    req_era: Vec<Option<u32>>,
    /// Sent at least once; accepted (it changed something, or replayed); its last
    /// attempt ended it.
    sent: Vec<bool>,
    accepted: Vec<bool>,
    finished: Vec<bool>,
    spare: usize,
    labels: HashMap<NodeId, String>,
    ghost: Ghost,
    out: Outcome,
    /// The member a request with steps between its commits runs on.
    running: Option<usize>,
    /// Identities a reset retired.
    retired: HashSet<NodeId>,
    /// A drive is running: the home track's policy does not nest.
    driving: bool,
    step: String,
}

impl World {
    fn new(sched: Schedule, base: u64) -> Self {
        let n = sched.requests.len();
        let members = templates()[..3]
            .iter()
            .map(|template| {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("kimmy.redb");
                std::fs::copy(template, &path).unwrap();
                let engine = Engine::open(&path).unwrap();
                Member { _dir: dir, path, engine: Some(Arc::new(engine)), catching_up: false }
            })
            .collect();
        let mut world = World {
            sched,
            members,
            base,
            now: None,
            era: 0,
            req_era: vec![None; n],
            sent: vec![false; n],
            accepted: vec![false; n],
            finished: vec![false; n],
            spare: 0,
            labels: HashMap::new(),
            ghost: Ghost::default(),
            out: Outcome::default(),
            running: None,
            retired: HashSet::new(),
            driving: false,
            step: "setup".into(),
        };
        for m in 0..3 {
            let e = world.e(m);
            world.configure(&e);
            world.labels.insert(e.node_id(), LETTERS[m].to_string());
        }
        world
    }

    fn e(&self, m: usize) -> Arc<Engine> {
        Arc::clone(self.members[m].engine.as_ref().expect("the member is open"))
    }

    fn configure(&self, e: &Engine) {
        e.set_multi_chunk_docs(CHUNK);
        e.set_idempotency_window(WINDOW_SECS);
        e.set_idempotency_clock_for_test(self.now);
    }

    fn label(&self, node: NodeId) -> String {
        self.labels.get(&node).cloned().unwrap_or_else(|| "?".into())
    }

    fn hlc(&self, h: Hlc) -> String {
        if h == Hlc::ZERO {
            return "0".into();
        }
        let rel = h.wall_ms as i64 - self.base as i64;
        if h.counter == 0 { format!("t{rel}") } else { format!("t{rel}.{}", h.counter) }
    }

    fn stamp(&self, s: Stamp) -> String {
        format!("{}{}", self.hlc(s.hlc), self.label(s.node))
    }

    fn note(&mut self, s: impl Into<String>) {
        let line = format!("  [{}] {}", self.step, s.into());
        self.out.notes.push(line);
    }

    fn fail(&mut self, inv: Inv, detail: impl Into<String>) {
        let failure = Failure { inv, detail: format!("[{}] {}", self.step, detail.into()) };
        if !self.out.failures.contains(&failure) {
            self.out.notes.push(format!("  !! {inv:?}: {}", failure.detail));
            self.out.failures.push(failure);
        }
    }

    fn clock(&self, m: usize, t: u32) {
        let e = self.e(m);
        e.advance_clock_past(&Stamp::new(Hlc::new(self.base + u64::from(t), 0), e.node_id()));
    }

    /// The member's tombstone or incarnation floor of collection `c`, whichever is
    /// later: a record or an entry at or below it belongs to an earlier life.
    fn floor(&self, m: usize, c: u8) -> Res<Hlc> {
        let e = self.e(m);
        let tomb = e
            .collection_dropped_at(coll_id(c))
            .map_err(|e| err("collection_dropped_at", e))?
            .map_or(Hlc::ZERO, |s| s.hlc);
        let floor = e
            .get_collection(DB, NAMES[c as usize])
            .ok()
            .and_then(|meta| meta.incarnation_floor)
            .unwrap_or(Hlc::ZERO);
        Ok(tomb.max(floor))
    }

    fn past_window(&self, record: &IdempotencyRecord) -> bool {
        self.now.is_some_and(|now| record.last.hlc < crate::gc::cutoff(now, WINDOW_SECS))
    }

    /// The member's record under `key` in `c` when it is alive there: not past its
    /// window, not of an earlier life.
    fn live_record(&self, m: usize, c: u8, key: u8) -> Res<Option<IdempotencyRecord>> {
        let record = self
            .e(m)
            .idempotency_record(coll_id(c), &ScopeHash([key; 32]))
            .map_err(|e| err("idempotency_record", e))?;
        let floor = self.floor(m, c)?;
        Ok(record.filter(|r| !self.past_window(r) && r.last.hlc > floor))
    }

    /// Whether request `r` is of an era a retention step has closed: its key's memory
    /// ended.
    fn memory_ended(&self, r: u8) -> bool {
        self.req_era[r as usize].is_some_and(|era| era < self.era)
    }

    fn finish_purges(&self) -> Res<()> {
        for m in 0..3 {
            self.e(m).finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
        }
        Ok(())
    }

    // -- Members coming and going ----------------------------------------------------------

    fn open(&mut self, m: usize) -> Res<()> {
        let engine = Engine::open(&self.members[m].path).map_err(|e| err("open", e))?;
        engine.reconcile_database_tombstones().map_err(|e| err("reconcile_database", e))?;
        engine.reconcile_late_drops().map_err(|e| err("reconcile_late_drops", e))?;
        engine.finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
        self.configure(&engine);
        self.members[m].engine = Some(Arc::new(engine));
        Ok(())
    }

    fn reopen(&mut self, m: usize, clean: bool) -> Res<()> {
        let arc = self.members[m].engine.take().expect("open");
        let engine = Arc::into_inner(arc).ok_or("the engine is held elsewhere")?;
        if clean {
            engine.close().map_err(|e| format!("close: {e:?}"))?;
        } else {
            drop(engine);
        }
        self.open(m)
    }

    fn reset(&mut self, m: usize, from: usize) -> Res<()> {
        if self.spare >= SPARES {
            self.note(format!("{} has no spare identity left: restarted instead", LETTERS[m]));
            return self.reopen(m, true);
        }
        let arc = self.members[m].engine.take().expect("open");
        self.retired.insert(arc.node_id());
        drop(Arc::into_inner(arc).ok_or("the engine is held elsewhere")?);
        let path = self.members[m].path.clone();
        std::fs::remove_file(&path).map_err(|e| err("remove", e))?;
        std::fs::copy(&templates()[3 + self.spare], &path).map_err(|e| err("copy", e))?;
        self.spare += 1;
        self.open(m)?;
        let node = self.e(m).node_id();
        let label = format!("{}{}", LETTERS[m], "'".repeat(self.spare));
        self.labels.insert(node, label.clone());
        self.note(format!("{} reset as {label}", LETTERS[m]));
        self.transfer(m, from, None, None)
    }

    // -- Replication --------------------------------------------------------------------------

    /// One pull, as the transport makes it (`convergence_model_tests::pull`), with the
    /// window's records: a whole-database snapshot beyond the horizon, a scoped page for
    /// a collection the batch stopped at.
    fn pull(&mut self, to: usize, from: usize, limit: usize) -> Res<()> {
        let mut stopped_at: Option<Stamp> = None;
        for _ in 0..32 {
            let (src, dst) = (self.e(from), self.e(to));
            let theirs = src.version_vector().map_err(|e| err("version_vector", e))?;
            let mine = dst.witnessed_vector().map_err(|e| err("witnessed_vector", e))?;
            let marked: Vec<MarkedRange> = dst
                .held_marks_covered_by(&mine)
                .map_err(|e| err("held_marks_covered_by", e))?
                .into_iter()
                .filter_map(|(span, _)| {
                    let through = span.through.min(theirs.get(span.origin));
                    (through >= span.from).then_some(MarkedRange { through, ..span })
                })
                .collect();
            let start = mine.behind(&theirs).or_else(|| {
                (!marked.is_empty())
                    .then(|| theirs.iter().map(|(_, h)| h).max().unwrap_or(Hlc::ZERO))
            });
            let Some(start) = start else { break };
            match src.serve_verdict(&mine).map_err(|e| err("serve_verdict", e))? {
                ServeVerdict::Serve => {}
                verdict => {
                    self.note(format!(
                        "{}<-{}: {verdict:?}, a whole-database snapshot",
                        LETTERS[to], LETTERS[from]
                    ));
                    self.transfer(to, from, None, None)?;
                    break;
                }
            }
            let window = src
                .entries_for_peer_marked(start, limit, Some(&mine), &marked, WalkScope::Background)
                .map_err(|e| err("entries_for_peer", e))?;
            let mut outcome = SyncOutcome::default();
            dst.apply_peer_window_carrying_into(
                &theirs,
                Some(&mine),
                &window.entries,
                &window.records,
                RecordsVia::Pull,
                PeerWindow {
                    scanned_to: window.scanned_to,
                    exhausted: window.exhausted,
                    passed_through: None,
                    asked_partial: false,
                },
                &mut outcome,
            )
            .map_err(|e| err("apply_peer_window", e))?;
            if trace() {
                let shipped: Vec<String> =
                    window.entries.iter().map(|e| self.stamp(e.stamp)).collect();
                let records: Vec<String> = window
                    .records
                    .iter()
                    .map(|c| {
                        let r = IdempotencyRecord::decode(&c.record).ok().flatten();
                        format!(
                            "key {} at {:?}: {:?}",
                            c.scope.0[0],
                            c.carrying.map(|s| self.stamp(s)),
                            r.map(|r| (self.stamp(r.first), self.stamp(r.last), r.state))
                        )
                    })
                    .collect();
                let s = format!(
                    "{}<-{} window: [{}] records [{}] applied {} superseded {} deferred {}",
                    LETTERS[to],
                    LETTERS[from],
                    shipped.join(" "),
                    records.join("; "),
                    outcome.applied,
                    outcome.superseded,
                    outcome.deferred
                );
                self.note(s);
            }
            // What the window shipped and the coverage then passed was taken by history:
            // applied, superseded, or judged history.
            self.processed_by(from, &dst, &window.entries, &window.records)?;
            if outcome.purge_pending_at.is_some() {
                dst.finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
                continue;
            }
            if let Some(unknown) = outcome.unknown.clone() {
                if stopped_at == Some(unknown.stamp) {
                    self.note(format!(
                        "{}<-{}: stuck at an unknown collection",
                        LETTERS[to], LETTERS[from]
                    ));
                    break;
                }
                stopped_at = Some(unknown.stamp);
                self.transfer(to, from, Some(unknown.id), None)?;
                continue;
            }
            if limit < BATCH || window.exhausted {
                break;
            }
        }
        self.maybe_caught_up(to)
    }

    /// The entries of a window `dst` took by history: those its coverage passed, of a
    /// collection it holds (one it does not hold takes them as nothing, and holds no
    /// effect of them).
    ///
    /// An entry of a keyed request shipped without its request's record by a sender
    /// that holds the record alive is finding G1 at its source (G1b): the window carries
    /// only the rows at the stamps it ships, and a sender can ship an entry of a commit
    /// whose first entry, the record's carrying stamp, it does not hold (that entry lost
    /// there, or a page gave it the later one as state).
    fn processed_by(
        &mut self,
        from: usize,
        dst: &Engine,
        entries: &[kimmy_core::OplogEntry],
        records: &[crate::idempotency::CarriedRecord],
    ) -> Res<()> {
        let after = dst.witnessed_vector().map_err(|e| err("witnessed_vector", e))?;
        let node = dst.node_id();
        let held: Vec<bool> =
            NAMES.iter().map(|name| dst.get_collection(DB, name).is_ok()).collect();
        for entry in entries {
            let Some((r, c, _)) = self.ghost.entries.get(&entry.stamp).cloned() else { continue };
            if !held[c as usize] || entry.stamp.hlc > after.get(entry.stamp.node) {
                continue;
            }
            self.ghost.mark_processed(node, entry.stamp);
            let key = self.sched.requests[r as usize].key;
            let carried = records
                .iter()
                .any(|w| w.collection == coll_id(c) && w.scope == ScopeHash([key; 32]));
            if !carried && self.live_record(from, c, key)?.is_some() {
                self.ghost.g1b.insert((node, entry.stamp));
            }
        }
        Ok(())
    }

    fn maybe_caught_up(&mut self, m: usize) -> Res<()> {
        if !self.members[m].catching_up {
            return Ok(());
        }
        let mine = self.e(m).witnessed_vector().map_err(|e| err("witnessed_vector", e))?;
        for peer in 0..3 {
            if peer == m {
                continue;
            }
            let theirs = self.e(peer).version_vector().map_err(|e| err("version_vector", e))?;
            if mine.behind(&theirs).is_some() {
                return Ok(());
            }
        }
        self.members[m].catching_up = false;
        self.note(format!("{} caught up", LETTERS[m]));
        Ok(())
    }

    /// Every (collection, key) a request of the schedule uses.
    fn keys(&self) -> BTreeSet<(u8, u8)> {
        self.sched.requests.iter().map(|q| (q.kind.coll(), q.key)).collect()
    }

    /// The records `from` holds alive for the collections a snapshot of `scope` carries.
    fn sender_records(
        &self,
        from: usize,
        scope: Option<CollectionId>,
    ) -> Res<Vec<(u8, u8, IdempotencyRecord)>> {
        let mut out = Vec::new();
        for (c, key) in self.keys() {
            if scope.is_some_and(|id| id != coll_id(c)) {
                continue;
            }
            if self.e(from).get_collection(DB, NAMES[c as usize]).is_err() {
                continue;
            }
            if let Some(record) = self.live_record(from, c, key)? {
                out.push((c, key, record));
            }
        }
        Ok(out)
    }

    /// A snapshot from `from`: of the whole database (`scope` none), a scoped page, or a
    /// converge page. Then I2's snapshot half.
    fn transfer(
        &mut self,
        to: usize,
        from: usize,
        scope: Option<CollectionId>,
        converge: Option<kimmy_core::VersionVector>,
    ) -> Res<()> {
        let expected = self.sender_records(from, scope)?;
        let mut progress = match (scope, converge) {
            (None, _) => SnapshotProgress::whole_database(),
            (Some(id), None) => SnapshotProgress::of_collection(id),
            (Some(id), Some(cut)) => SnapshotProgress::of_collection_converging(id, cut),
        };
        let mut waits = 0;
        let mut pages = 0;
        let mut refused = false;
        while !progress.is_complete() {
            pages += 1;
            if pages > 64 {
                return Err("a snapshot that never ends".into());
            }
            let (src, dst) = (self.e(from), self.e(to));
            let page = src
                .snapshot_page(progress.after().cloned(), progress.scope())
                .map_err(|e| err("snapshot_page", e))?;
            let applied = dst
                .apply_snapshot_page(src.node_id(), &mut progress, &page)
                .map_err(|e| err("apply_snapshot_page", e))?;
            if applied.sender_dropped || applied.history {
                // Refused whole (the sender's life is dropped, or one this member
                // buried): the pull ends with nothing taken.
                refused = true;
                break;
            }
            if applied.purge_pending.is_none() {
                continue;
            }
            waits += 1;
            if waits > 4 {
                return Err("a page that waits for a purge that never finishes".into());
            }
            dst.finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
        }
        if scope.is_none() {
            self.members[to].catching_up = true;
        }
        if refused {
            return Ok(());
        }
        // I2, a completed snapshot: the receiver holds every live record the sender held
        // for what it carried, merged.
        for (c, key, theirs) in expected {
            if self.e(to).get_collection(DB, NAMES[c as usize]).is_err()
                || theirs.last.hlc <= self.floor(to, c)?
            {
                continue;
            }
            let mine = self
                .e(to)
                .idempotency_record(coll_id(c), &ScopeHash([key; 32]))
                .map_err(|e| err("idempotency_record", e))?;
            let held =
                mine.as_ref().is_some_and(|m| m.first <= theirs.first && m.last >= theirs.last);
            if !held {
                let s = format!(
                    "{} took {} from {} without key {key}'s record (sender first {} last {}; \
                     receiver {:?})",
                    LETTERS[to],
                    if scope.is_some() { NAMES[c as usize] } else { "the database" },
                    LETTERS[from],
                    self.stamp(theirs.first),
                    self.stamp(theirs.last),
                    mine.map(|m| (self.stamp(m.first), self.stamp(m.last)))
                );
                self.fail(Inv::I2, s);
            }
        }
        Ok(())
    }

    // -- Requests -----------------------------------------------------------------------------

    /// Everything a send needs, read before it: `None` when it is not sent (the member
    /// is catching up, the key's memory ended, the collection is not there).
    fn prepare(&mut self, r: u8, m: usize, t: u32) -> Res<Option<Prepared>> {
        let req = self.sched.requests[r as usize];
        let c = req.kind.coll();
        if self.members[m].catching_up {
            self.note(format!("r{r} not sent: {} is catching up", LETTERS[m]));
            return Ok(None);
        }
        if self.memory_ended(r) {
            self.note(format!("r{r} not sent: its key's memory ended"));
            return Ok(None);
        }
        if self.e(m).get_collection(DB, NAMES[c as usize]).is_err() {
            self.note(format!("r{r} not sent: {} lacks {}", LETTERS[m], NAMES[c as usize]));
            return Ok(None);
        }
        // On the home track one `$inc` is unfinished at a time: every other one ends
        // first, so that no request's resume leaves a counter another changed meanwhile
        // (`skipped`, by design) and I5 can hold exactly.
        if self.sched.track == Track::Home && req.kind.is_inc() && !self.driving {
            self.driving = true;
            for other in self.open_incs() {
                if other != r {
                    self.drive(other)?;
                }
            }
            self.driving = false;
            if self.members[m].catching_up {
                return Ok(None);
            }
        }
        if self.req_era[r as usize].is_none() {
            self.req_era[r as usize] = Some(self.era);
        }
        self.sent[r as usize] = true;
        self.clock(m, t);
        let engine = self.e(m);
        let coll =
            engine.get_collection(DB, NAMES[c as usize]).map_err(|e| err("collection", e))?;
        let keyed = keyed_write(&self.sched.requests, r);
        let pre_record = self.live_record(m, c, req.key)?;
        let floor = self.floor(m, c)?;
        let last_before = self.ghost.last.get(&r).copied();
        let before = engine.clock_last();
        let rid = i64::from(r);
        let base_of = |id: &DocId| -> (Vec<i64>, Option<Stamp>) {
            match stored(&engine, c, id) {
                Some((stamp, Some(doc))) => (ks_of(&doc), Some(stamp)),
                Some((stamp, None)) => {
                    (self.ghost.tomb_ks.get(&stamp).cloned().unwrap_or_default(), Some(stamp))
                }
                None => (Vec::new(), None),
            }
        };
        let target_id = |at: Target| -> DocId {
            match at {
                Target::Client(d) => DocId::Int64(ITEM_BASE + i64::from(d)),
                Target::Derived(owner, pos) => DocId::String(
                    keyed_write(&self.sched.requests, owner).derived_id(coll.id, u64::from(pos)),
                ),
            }
        };
        let appended = |ks: &[i64]| {
            let mut next = ks.to_vec();
            next.push(rid);
            next
        };
        let mut base = Vec::new();
        let call = match req.kind {
            Kind::Insert { id } => {
                let (target, body) = match id {
                    Some(d) => {
                        let id = ITEM_BASE + i64::from(d);
                        let body =
                            doc! { "_id": id, "v": rid, "req": rid, "pos": 0i64, "ins": true };
                        (DocId::Int64(id), body)
                    }
                    None => (
                        DocId::String(keyed.derived_id(coll.id, 0)),
                        doc! { "v": rid, "req": rid, "pos": 0i64, "ins": true },
                    ),
                };
                let (ks, at) = base_of(&target);
                let body = with_ks(body, &appended(&ks));
                base.push((target, ks, at));
                Call::Insert(body)
            }
            Kind::Bulk => {
                let mut docs = Vec::new();
                for pos in 0..2u64 {
                    let target = DocId::String(keyed.derived_id(coll.id, pos));
                    let (ks, at) = base_of(&target);
                    docs.push(with_ks(
                        doc! { "v": rid, "req": rid, "pos": pos as i64, "ins": true },
                        &appended(&ks),
                    ));
                    base.push((target, ks, at));
                }
                Call::Bulk(docs)
            }
            Kind::IncMulti => Call::Inc { target: None, stop: None },
            Kind::IncSingle { d } => Call::Inc { target: Some(i64::from(d)), stop: Some(1) },
            Kind::Replace { at } => {
                let target = target_id(at);
                let (ks, at) = base_of(&target);
                let body = with_ks(doc! { "v": rid, "req": rid, "rep": true }, &appended(&ks));
                base.push((target.clone(), ks, at));
                Call::Replace(target, body)
            }
            Kind::Delete { at } => {
                let target = target_id(at);
                let (ks, at) = match stored(&engine, c, &target) {
                    Some((stamp, Some(doc))) => (ks_of(&doc), Some(stamp)),
                    _ => (Vec::new(), None),
                };
                base.push((target.clone(), ks, at));
                Call::Delete(target)
            }
        };
        let node = engine.node_id();
        Ok(Some(Prepared {
            r,
            m,
            engine: Some(engine),
            node,
            coll,
            keyed,
            call,
            before,
            base,
            pre_record,
            floor,
            last_before,
            on_own_effect: Arc::new(Mutex::new(Vec::new())),
            phase: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            phase_seq: Rc::new(RefCell::new(vec![self.ghost.seq])),
        }))
    }

    /// Classify an application of request `r` on member `m` whose base already holds
    /// it (I1): allowed by the life rule, finding G1, or a violation.
    #[allow(clippy::too_many_arguments)]
    fn on_own_effect(
        &mut self,
        p: &Prepared,
        id: &DocId,
        ks: &[i64],
        base_at: Option<Stamp>,
        phase: usize,
        minted: &HashSet<Stamp>,
    ) {
        // What the member had taken by the phase it wrote in.
        let taken_by = p.phase_seq.borrow().get(phase).copied().unwrap_or(u64::MAX);
        let r = p.r;
        let c = self.sched.requests[r as usize].kind.coll();
        let key = crate::docs::doc_key(id).unwrap_or_default();
        // Where the collection's current life begins, for this member: its own tombstone
        // or floor, or a drop made elsewhere it has not received yet.
        let end = self.ghost_life_end(c).max(p.floor);
        // The request's writes of this document in the current life.
        let current: Vec<Stamp> = self
            .ghost
            .entries
            .iter()
            .filter(|(s, (q, qc, d))| *q == r && *qc == c && d == id && !minted.contains(s))
            .map(|(s, _)| *s)
            // A version holds only applications at or below its own stamp.
            .filter(|s| s.hlc > end && base_at.is_none_or(|at| *s <= at))
            .collect();
        // Whether every record of the request is of an earlier life, or the commit that
        // wrote the document straddles a drop (its first entry buried, so its record).
        let record_dead = p.last_before.is_some_and(|last| last.hlc <= end)
            || current.iter().any(|s| self.straddles(r, *s, end));
        let current = !current.is_empty();
        if record_dead && !current {
            self.out.life_exempt += 1;
            if p.last_before.is_some_and(|last| last.hlc > p.floor) {
                self.out.dropped_elsewhere += 1;
            }
            self.ghost.explained.insert((c, key, r));
            let s = format!(
                "r{r} on {} applied afresh on {id:?}, an earlier life's effect",
                LETTERS[p.m]
            );
            self.note(s);
            return;
        }
        let processed_current = self.ghost.processed.iter().any(|((n, s), seq)| {
            *n == p.node
                && *seq <= taken_by
                && !minted.contains(s)
                && s.hlc > end
                && self.ghost.entries.get(s).is_some_and(|(q, ..)| *q == r)
        });
        let detail = format!(
            "r{r} applied on {} to {id:?} whose key set {ks:?} already holds it (live record \
             before: {:?}; an entry of r{r} processed there before: {processed_current}; its \
             greatest last {:?}, life from {})",
            LETTERS[p.m],
            p.pre_record.as_ref().map(|rec| (
                self.stamp(rec.first),
                self.stamp(rec.last),
                rec.state
            )),
            p.last_before.map(|l| self.stamp(l)),
            self.hlc(end)
        );
        if record_dead {
            // Finding G2: the request's record is judged by its `last`, the first entry of
            // its latest commit, and a drop in that commit's millisecond buries it while a
            // later entry of the same commit stands in the new life.
            self.out.g2 += 1;
            self.ghost.explained.insert((c, key, r));
            self.note(format!("G2: {detail}"));
            if strict() {
                self.fail(Inv::I1, format!("G2 (strict): {detail}"));
            }
            return;
        }
        if p.pre_record.is_none() && !processed_current {
            self.out.g1 += 1;
            self.ghost.explained.insert((c, key, r));
            self.note(format!("G1: {detail}"));
            if strict() {
                self.fail(Inv::I1, format!("G1 (strict): {detail}"));
            }
            return;
        }
        self.fail(Inv::I1, detail);
    }

    /// The entries member `m` minted during the call: its own node's, above the clock
    /// it had before.
    fn minted(&self, p: &Prepared) -> Res<Vec<kimmy_core::OplogEntry>> {
        let e = self.e(p.m);
        Ok(e.read_oplog_from(p.before, 100_000, WalkScope::Request)
            .map_err(|e| err("read_oplog_from", e))?
            .into_iter()
            .filter(|entry| {
                entry.stamp.node == p.node && entry.stamp.hlc > p.before && entry.doc_id.is_some()
            })
            .collect())
    }

    /// After the call: the crash's reopen, what it minted, and the answer.
    fn after_send(&mut self, mut p: Prepared, answer: Answer, crashed: bool) -> Res<()> {
        let r = p.r;
        let m = p.m;
        let req = self.sched.requests[r as usize];
        let c = req.kind.coll();
        if crashed {
            self.out.crashes += 1;
            // Every handle but the member's own is let go first.
            drop(p.engine.take());
            self.reopen(m, false)?;
            p.engine = Some(self.e(m));
        }
        let minted = self.minted(&p)?;
        let minted_set: HashSet<Stamp> = minted.iter().map(|e| e.stamp).collect();
        // Applications on their own effect: seen by the `$inc` where they happened, or
        // an insert, replace or delete that wrote on a base holding it.
        let seen: Vec<(DocId, Vec<i64>, usize)> =
            std::mem::take(&mut *p.on_own_effect.lock().unwrap());
        for (id, ks, phase) in &seen {
            self.on_own_effect(&p, id, ks, None, *phase, &minted_set);
        }
        // An insert, replace or delete whose write landed (answered or not: a failed
        // fsync's may have) on a base already holding the request.
        if !req.kind.is_inc() {
            for (id, ks, at) in p.base.clone() {
                let landed = minted.iter().any(|e| e.doc_id.as_ref() == Some(&id));
                if landed && ks.contains(&i64::from(r)) {
                    self.on_own_effect(&p, &id, &ks, at, 0, &minted_set);
                }
            }
        }
        for entry in &minted {
            let id = entry.doc_id.clone().expect("a document entry");
            let key = crate::docs::doc_key(&id).unwrap_or_default();
            self.ghost.entries.insert(entry.stamp, (r, c, id.clone()));
            self.ghost.mark_processed(p.node, entry.stamp);
            // Writes of one life of the collection: a drop between two ends the
            // first's, and the life rule lets the request apply afresh after it.
            let life = self
                .ghost
                .drops
                .iter()
                .filter(|(dc, at)| *dc == c && at.hlc < entry.stamp.hlc)
                .count();
            let earlier: Vec<Stamp> = self
                .ghost
                .writes
                .get(&(p.node, r, c, key.clone()))
                .map(|w| {
                    w.iter()
                        .copied()
                        .filter(|s| {
                            self.ghost
                                .drops
                                .iter()
                                .filter(|(dc, at)| *dc == c && at.hlc < s.hlc)
                                .count()
                                == life
                        })
                        .collect()
                })
                .unwrap_or_default();
            self.ghost.writes.entry((p.node, r, c, key.clone())).or_default().push(entry.stamp);
            if !earlier.is_empty() && !self.ghost.explained.contains(&(c, key.clone(), r)) {
                let s = format!(
                    "r{r} wrote {id:?} on {} again at {}, after {}",
                    self.label(p.node),
                    self.stamp(entry.stamp),
                    earlier.iter().map(|s| self.stamp(*s)).collect::<Vec<_>>().join(" ")
                );
                self.fail(Inv::I3, s);
            }
            if entry.kind == OpKind::Delete {
                let mut ks = p.base.iter().find(|(d, ..)| *d == id).map(|(_, ks, _)| ks.clone());
                ks.get_or_insert_with(Vec::new).push(i64::from(r));
                self.ghost.tomb_ks.insert(entry.stamp, ks.unwrap_or_default());
            }
            // I7: a derived `_id` is one request's.
            if matches!(req.kind, Kind::Insert { id: None } | Kind::Bulk)
                && entry.kind == OpKind::Insert
                && let DocId::String(s) = &id
            {
                match self.ghost.owner.get(s).copied() {
                    Some(owner) if owner != r => {
                        let d = format!("r{r}'s insert landed on {s}, where r{owner}'s did");
                        self.fail(Inv::I7, d);
                    }
                    Some(_) => {}
                    None => {
                        self.ghost.owner.insert(s.clone(), r);
                    }
                }
            }
        }
        let shown = minted.iter().map(|e| self.stamp(e.stamp)).collect::<Vec<_>>().join(" ");
        match answer {
            Answer::Fresh { wrote, resumed } => {
                if resumed {
                    self.out.resumes += 1;
                }
                self.accepted[r as usize] |= wrote || !minted.is_empty() || resumed;
                self.finished[r as usize] = true;
                let s = format!(
                    "r{r} on {}: fresh{}{}, minted [{shown}]",
                    LETTERS[m],
                    if wrote { "" } else { " (changed nothing)" },
                    if resumed { ", resumed" } else { "" },
                );
                self.note(s);
            }
            Answer::Replayed(record) => {
                self.out.replays += 1;
                self.accepted[r as usize] = true;
                self.finished[r as usize] = true;
                let floor = self.floor(m, c)?;
                if record.last.hlc <= floor {
                    let s = format!(
                        "r{r} on {} replayed a record of an earlier life (last {}, floor {})",
                        LETTERS[m],
                        self.stamp(record.last),
                        self.hlc(floor)
                    );
                    self.fail(Inv::Life, s);
                }
                if self.past_window(&record) {
                    let s = format!("r{r} on {} replayed a record past its window", LETTERS[m]);
                    self.fail(Inv::Life, s);
                }
                let s = format!(
                    "r{r} on {}: replayed (first {} last {} {:?} origin {})",
                    LETTERS[m],
                    self.stamp(record.first),
                    self.stamp(record.last),
                    record.state,
                    self.label(record.origin)
                );
                self.note(s);
            }
            Answer::Failed(e) => {
                self.accepted[r as usize] |= !minted.is_empty();
                self.finished[r as usize] = false;
                self.note(format!("r{r} on {}: {e}, minted [{shown}]", LETTERS[m]));
            }
        }
        Ok(())
    }

    /// A send with nothing between its commits and no crash: a plain step, a drive, or
    /// a concurrent attempt run between another request's commits.
    fn send_plain(&mut self, r: u8, m: usize, t: u32) -> Res<()> {
        let Some(p) = self.prepare(r, m, t)? else { return Ok(()) };
        let answer = call(&p);
        if let Answer::Failed(e) = &answer
            && !expected_failure(e)
        {
            let s = format!("r{r} on {}: {e}", LETTERS[m]);
            self.fail(Inv::Error, s);
        }
        self.after_send(p, answer, false)
    }

    /// Retry request `r` on its home until it is done (the home track's policy).
    fn drive(&mut self, r: u8) -> Res<()> {
        let home = self.sched.home as usize;
        for _ in 0..6 {
            if self.finished[r as usize] || self.members[home].catching_up || self.memory_ended(r) {
                return Ok(());
            }
            self.out.steps += 1;
            let t = self.latest_t(home);
            self.note(format!("drive r{r} on {}", LETTERS[home]));
            self.send_plain(r, home, t)?;
        }
        if !self.finished[r as usize] {
            self.fail(Inv::Error, format!("r{r} never finished on its home"));
        }
        Ok(())
    }

    /// A time for a step the schedule did not place: the member's clock as it stands.
    fn latest_t(&self, m: usize) -> u32 {
        let last = self.e(m).clock_last();
        last.wall_ms.saturating_sub(self.base) as u32
    }

    // -- Steps --------------------------------------------------------------------------------

    /// A step that is not a send with steps between its commits.
    fn apply(&mut self, op: &Op) -> Res<()> {
        self.out.steps += 1;
        match &op.step {
            Step::Send { r, m, .. } => self.send_plain(*r, *m as usize, op.t)?,
            Step::Pull { to, from } => self.pull(*to as usize, *from as usize, BATCH)?,
            Step::PullShort { to, from } => self.pull(*to as usize, *from as usize, SHORT)?,
            Step::Page { to, from, c, converge, restart } => {
                let (to, from) = (*to as usize, *from as usize);
                if self.e(from).get_collection(DB, NAMES[*c as usize]).is_err() {
                    self.note(format!("{} lacks {}: no page", LETTERS[from], NAMES[*c as usize]));
                } else {
                    let cut = converge.then(kimmy_core::VersionVector::new);
                    self.transfer(to, from, Some(coll_id(*c)), cut)?;
                }
                if *restart {
                    self.reopen(to, true)?;
                }
            }
            Step::Reset { m, from } => self.reset(*m as usize, *from as usize)?,
            Step::Restart { m } => self.reopen(*m as usize, true)?,
            Step::Crash { m } => self.reopen(*m as usize, false)?,
            Step::Drop { m } => {
                let m = *m as usize;
                self.clock(m, op.t);
                match self.e(m).drop_collection(DB, NAMES[ITEMS as usize]) {
                    Ok(held) => {
                        let e = self.e(m);
                        let at = e
                            .collection_dropped_at(coll_id(ITEMS))
                            .map_err(|e| err("dropped_at", e))?;
                        if let Some(at) = at {
                            self.ghost.drops.push((ITEMS, at));
                        }
                        self.note(format!("{} dropped items (held {held})", LETTERS[m]))
                    }
                    Err(e) => self.note(format!("{} drop refused: {e}", LETTERS[m])),
                }
            }
            Step::Create { m } => {
                let m = *m as usize;
                self.clock(m, op.t);
                for attempt in 0..2 {
                    match self.e(m).create_collection(DB, NAMES[ITEMS as usize]) {
                        Ok(_) => self.note(format!("{} created items", LETTERS[m])),
                        Err(StorageError::CollectionPurging { .. }) if attempt == 0 => {
                            self.finish_purges()?;
                            continue;
                        }
                        Err(e) => self.note(format!("{} create refused: {e}", LETTERS[m])),
                    }
                    break;
                }
            }
            Step::Retention { m } => {
                let m = *m as usize;
                if self.sched.track == Track::Home {
                    for r in self.open_incs() {
                        self.drive(r)?;
                    }
                }
                let now = self.base + u64::from(op.t);
                self.now = Some(now);
                for j in 0..3 {
                    self.e(j).set_idempotency_clock_for_test(Some(now));
                }
                self.era += 1;
                let out = self
                    .e(m)
                    .collect_garbage_at(now, RetentionPolicy::new(WINDOW_SECS, TOMBSTONE_SECS))
                    .map_err(|e| err("collect_garbage", e))?;
                self.note(format!(
                    "{} retention at {}: {} entries, {} records",
                    LETTERS[m],
                    self.hlc(Hlc::new(now, 0)),
                    out.oplog_removed,
                    out.records_removed
                ));
            }
            Step::Clock { m } => self.clock(*m as usize, op.t),
        }
        Ok(())
    }

    /// `$inc` requests accepted and not finished.
    fn open_incs(&self) -> Vec<u8> {
        (0..self.sched.requests.len() as u8)
            .filter(|r| {
                self.sched.requests[*r as usize].kind.is_inc()
                    && self.accepted[*r as usize]
                    && !self.finished[*r as usize]
            })
            .collect()
    }

    /// A step run between two commits of a request on `running`: never one that would
    /// close that member or send to it.
    fn apply_inner(&mut self, op: &Op) -> Res<()> {
        let running = self.running.expect("inside a request");
        let touches = match &op.step {
            Step::Send { m, .. }
            | Step::Restart { m }
            | Step::Crash { m }
            | Step::Reset { m, .. }
            | Step::Drop { m }
            | Step::Create { m }
            | Step::Retention { m } => *m as usize == running,
            Step::Page { to, restart, .. } => *restart && *to as usize == running,
            _ => false,
        };
        if touches {
            self.note(format!("skipped between commits: {op}"));
            return Ok(());
        }
        self.out.between += 1;
        let saved = self.step.clone();
        self.step = format!("{saved} > {op}");
        let out = self.apply(op);
        self.step = saved;
        out
    }

    // -- After every step ---------------------------------------------------------------------

    fn after_step(&mut self) -> Res<()> {
        self.finish_purges()?;
        self.observe_records()?;
        self.check_stored()?;
        self.check_i2()
    }

    /// The greatest `last` of each request's records, on any member.
    fn observe_records(&mut self) -> Res<()> {
        for r in 0..self.sched.requests.len() as u8 {
            let req = self.sched.requests[r as usize];
            for m in 0..3 {
                let record = self
                    .e(m)
                    .idempotency_record(coll_id(req.kind.coll()), &req.scope())
                    .map_err(|e| err("idempotency_record", e))?;
                if let Some(record) = record
                    && record.fingerprint == fingerprint(r)
                {
                    let last = self.ghost.last.entry(r).or_insert(record.last);
                    *last = (*last).max(record.last);
                }
            }
        }
        Ok(())
    }

    /// Every live document of a member, by collection and `_id`.
    fn documents(&self, m: usize) -> Res<BTreeMap<(u8, String), (Stamp, Document)>> {
        let mut out = BTreeMap::new();
        let e = self.e(m);
        for c in [ORDERS, ITEMS] {
            let Ok(meta) = e.get_collection(DB, NAMES[c as usize]) else { continue };
            e.for_each_record_after(&meta, None, WalkScope::Background, |id, stamp, doc| {
                out.insert((c, format!("{id:?}")), (stamp, doc));
                Ok(true)
            })
            .map_err(|e| err("for_each_record_after", e))?;
        }
        Ok(out)
    }

    /// I1 on every stored version, I6 on every counter, I7 on every inserted document.
    fn check_stored(&mut self) -> Res<()> {
        for m in 0..3 {
            for ((c, shown), (stamp, doc)) in self.documents(m)? {
                let ks = ks_of(&doc);
                let id = id_of(&doc);
                let key = crate::docs::doc_key(&id).unwrap_or_default();
                let mut seen = BTreeSet::new();
                let mut unexplained = false;
                for k in &ks {
                    if !seen.insert(*k)
                        && !self.ghost.explained.contains(&(c, key.clone(), *k as u8))
                    {
                        unexplained = true;
                        let s = format!(
                            "{} holds {shown} at {} with r{k} applied twice: {ks:?}",
                            LETTERS[m],
                            self.stamp(stamp)
                        );
                        self.fail(Inv::I1, s);
                    }
                }
                if c == ORDERS {
                    let d = doc.get_i64("_id").unwrap_or(-1);
                    let n = doc.get_i64("n").unwrap_or(0);
                    let bound = (0..self.sched.requests.len())
                        .filter(|r| self.sent[*r] && self.sched.requests[*r].kind.targets(d))
                        .count() as i64;
                    if n > bound && (unexplained || strict()) {
                        let s = format!(
                            "{} counter {d} is {n}, above the {bound} requests sent for it: {ks:?}",
                            LETTERS[m]
                        );
                        self.fail(Inv::I6, s);
                    }
                }
                if c == ITEMS
                    && doc.get_bool("ins").unwrap_or(false)
                    && let DocId::String(s) = &id
                    && let Some(owner) = self.ghost.owner.get(s).copied()
                {
                    let req = doc.get_i64("req").unwrap_or(-1);
                    if req != i64::from(owner) {
                        let d = format!(
                            "{} holds r{req}'s insert at {s}, r{owner}'s derived `_id`",
                            LETTERS[m]
                        );
                        self.fail(Inv::I7, d);
                    }
                }
            }
        }
        Ok(())
    }

    /// I2: every entry of a keyed request a member processed by history has the
    /// request's record there.
    fn check_i2(&mut self) -> Res<()> {
        let mut records: HashMap<(usize, u8, u8), Option<IdempotencyRecord>> = HashMap::new();
        let mut floors: HashMap<(usize, u8), Hlc> = HashMap::new();
        let mut failures = Vec::new();
        let nodes: Vec<NodeId> = (0..3).map(|m| self.e(m).node_id()).collect();
        let mut processed: Vec<(NodeId, Stamp)> = self.ghost.processed.keys().copied().collect();
        processed.sort();
        for (node, stamp) in processed {
            let Some(m) = nodes.iter().position(|n| *n == node) else { continue };
            let Some((r, c, id)) = self.ghost.entries.get(&stamp).cloned() else { continue };
            if self.memory_ended(r) {
                continue;
            }
            let floor = match floors.get(&(m, c)) {
                Some(f) => *f,
                None => {
                    let f = self.floor(m, c)?;
                    floors.insert((m, c), f);
                    f
                }
            };
            if stamp.hlc <= floor {
                continue;
            }
            if stamp.hlc <= self.ghost_life_end(c) {
                // A life a drop made elsewhere has ended: its sender purged the record.
                if self.ghost.dropped_elsewhere.insert((node, stamp)) {
                    self.out.dropped_elsewhere += 1;
                }
                continue;
            }
            let key = self.sched.requests[r as usize].key;
            let record = match records.get(&(m, c, key)) {
                Some(record) => record.clone(),
                None => {
                    let record = self.live_record(m, c, key)?;
                    records.insert((m, c, key), record.clone());
                    record
                }
            };
            if !record.as_ref().is_some_and(|rec| rec.first <= stamp) {
                if self.ghost.g1b.contains(&(node, stamp)) {
                    if self.ghost.g1b_counted.insert((node, stamp)) {
                        self.out.g1 += 1;
                        let s = format!(
                            "G1b: {} took r{r}'s entry {} without its record, from a sender \
                             that held it",
                            LETTERS[m],
                            self.stamp(stamp)
                        );
                        self.note(s.clone());
                        if strict() {
                            failures.push(format!("(strict) {s}"));
                        }
                    }
                    continue;
                }
                let end = self.ghost_life_end(c).max(floor);
                if self.straddles(r, stamp, end)
                    || (record.is_none() && self.ghost.last.get(&r).is_some_and(|l| l.hlc <= end))
                {
                    // Finding G2 (see `on_own_effect`): the entry stands in the current
                    // life, and its request's record is buried by its `last`.
                    if self.ghost.g2.insert((node, stamp)) {
                        self.out.g2 += 1;
                        let s = format!(
                            "G2: {} holds r{r}'s entry {} of the current life (from {}), its \
                             record buried at its last {:?}",
                            LETTERS[m],
                            self.stamp(stamp),
                            self.hlc(end),
                            self.ghost.last.get(&r).map(|l| self.stamp(*l))
                        );
                        self.note(s.clone());
                        if strict() {
                            failures.push(format!("(strict) {s}"));
                        }
                    }
                    continue;
                }
                failures.push(format!(
                    "{} processed r{r}'s entry {} ({id:?}) without its record (holds {:?})",
                    LETTERS[m],
                    self.stamp(stamp),
                    record.map(|rec| (self.stamp(rec.first), self.stamp(rec.last)))
                ));
            }
        }
        for f in failures {
            self.fail(Inv::I2, f);
        }
        Ok(())
    }

    // -- Quiescence ---------------------------------------------------------------------------

    fn fingerprint(&self) -> Res<String> {
        let mut out = String::new();
        for m in 0..3 {
            let e = self.e(m);
            let _ = write!(
                out,
                "{:?}{:?}{:?}",
                self.documents(m)?,
                e.version_vector().map_err(|e| err("version_vector", e))?,
                e.witnessed_vector().map_err(|e| err("witnessed_vector", e))?
            );
            for (c, key) in self.keys() {
                let record = e
                    .idempotency_record(coll_id(c), &ScopeHash([key; 32]))
                    .map_err(|e| err("idempotency_record", e))?;
                let _ = write!(out, "{:?}", record.map(|r| r.encode()));
            }
        }
        Ok(out)
    }

    fn quiesce(&mut self) -> Res<bool> {
        for _ in 0..QUIESCE_ROUNDS {
            let before = self.fingerprint()?;
            for to in 0..3 {
                for from in 0..3 {
                    if to != from {
                        self.pull(to, from, BATCH)?;
                        self.finish_purges()?;
                    }
                }
            }
            self.repair_round()?;
            self.finish_purges()?;
            if self.fingerprint()? == before {
                for m in 0..3 {
                    self.maybe_caught_up(m)?;
                }
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The divergence check every contact makes, and the repair it plans (`peers.rs`),
    /// as `convergence_model_tests::repair_round` runs it: a collection the peer holds
    /// that this member does not, past its tombstone, is pulled as a scoped page; one
    /// whose live count differs is replayed from the peer's oplog from
    /// `count_repair_replay_from`, or pulled as a scoped page when the peer cannot
    /// serve from there.
    fn repair_round(&mut self) -> Res<()> {
        for me in 0..3 {
            for peer in 0..3 {
                if me == peer {
                    continue;
                }
                let (mine, theirs) = (self.e(me), self.e(peer));
                let local = crate::DivergenceLocalState {
                    collections: mine.all_collection_ids().map_err(|e| err("ids", e))?,
                    probe: None,
                    dropped: mine.collection_tombstones().map_err(|e| err("tombstones", e))?,
                };
                let answer = crate::DivergencePeerAnswer {
                    collections: theirs.all_collection_ids().map_err(|e| err("ids", e))?,
                    probe_count: None,
                    incarnations: theirs
                        .collection_incarnations()
                        .map_err(|e| err("incarnations", e))?,
                };
                let findings = crate::compare_divergence(&local, &answer);
                let common: Vec<CollectionId> =
                    local.collections.intersection(&answer.collections).copied().collect();
                let mut counts = Vec::new();
                for id in common {
                    let a = mine.count_by_id(id).map_err(|e| err("count", e))?;
                    let b = theirs.count_by_id(id).map_err(|e| err("count", e))?;
                    if a != b {
                        counts.push((id, mine.collection_by_id(id).map_err(|e| err("by_id", e))?));
                    }
                }
                for id in findings.existence {
                    self.note(format!(
                        "repair: {} lacks a collection, a scoped page from {}",
                        LETTERS[me], LETTERS[peer]
                    ));
                    self.transfer(me, peer, Some(id), None)?;
                }
                // The cut digest (ADR-230): a collection whose digests differ at the
                // cut is repaired by a converge page (ADR-237).
                let budget = crate::ScanBudget::default();
                let witnessed = theirs.witnessed_vector().map_err(|e| err("witnessed", e))?;
                let plan = mine
                    .digest_plan(&witnessed, None, budget)
                    .map_err(|e| err("digest_plan", e))?;
                let verdicts = match theirs
                    .digest_answer(&plan.ask, budget)
                    .map_err(|e| err("digest_answer", e))?
                {
                    Some(reply) => mine
                        .digest_compare(&plan, &reply, budget)
                        .map_err(|e| err("digest_compare", e))?,
                    None => Vec::new(),
                };
                for verdict in verdicts {
                    if let crate::DigestOutcome::Compared { equal: false, .. } = verdict.outcome {
                        self.note(format!(
                            "repair: {}'s digest differs from {}'s, a converge page",
                            LETTERS[me], LETTERS[peer]
                        ));
                        self.transfer(me, peer, Some(verdict.collection), Some(plan.cut.clone()))?;
                    }
                }
                for (id, meta) in counts {
                    match meta {
                        Some(meta) => {
                            let from = self
                                .e(me)
                                .count_repair_replay_from(&meta)
                                .map_err(|e| err("count_repair_replay_from", e))?;
                            self.note(format!(
                                "repair: {}'s count differs from {}'s, a replay",
                                LETTERS[me], LETTERS[peer]
                            ));
                            self.replay(me, peer, from, id)?;
                        }
                        None => self.transfer(me, peer, Some(id), None)?,
                    }
                }
            }
        }
        Ok(())
    }

    /// A replay (`Repair::Replay`): the peer's oplog from `floor`, below this member's
    /// position, with no `held`, window by window, with the windows' records; a peer
    /// that cannot serve from there sends a scoped page instead.
    fn replay(&mut self, to: usize, from: usize, floor: Hlc, id: CollectionId) -> Res<()> {
        if self.e(from).serve_verdict_from(floor).map_err(|e| err("serve_verdict_from", e))?
            != ServeVerdict::Serve
        {
            return self.transfer(to, from, Some(id), None);
        }
        let mut start = floor;
        for _ in 0..32 {
            let (src, dst) = (self.e(from), self.e(to));
            let theirs = src.version_vector().map_err(|e| err("version_vector", e))?;
            let mine = dst.witnessed_vector().map_err(|e| err("witnessed_vector", e))?;
            let start_at = mine.behind(&theirs).map_or(start, |behind| behind.min(start));
            let window = src
                .entries_for_peer_holding(start_at, BATCH, None, WalkScope::Background)
                .map_err(|e| err("entries_for_peer", e))?;
            let mut outcome = SyncOutcome::default();
            dst.apply_peer_window_carrying_into(
                &theirs,
                Some(&mine),
                &window.entries,
                &window.records,
                RecordsVia::Repair,
                PeerWindow {
                    scanned_to: window.scanned_to,
                    exhausted: window.exhausted,
                    passed_through: None,
                    asked_partial: false,
                },
                &mut outcome,
            )
            .map_err(|e| err("apply_peer_window", e))?;
            self.processed_by(from, &dst, &window.entries, &window.records)?;
            if outcome.purge_pending_at.is_some() {
                dst.finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
                continue;
            }
            if let Some(unknown) = outcome.unknown {
                return self.transfer(to, from, Some(unknown.id), None);
            }
            if window.exhausted {
                return Ok(());
            }
            start = window.scanned_to.successor();
        }
        Err("a replay that never ended".into())
    }

    /// I4 and I5 once the cluster is quiet.
    fn check_settled(&mut self) -> Res<()> {
        // Identical documents.
        let docs: Vec<_> = (0..3).map(|m| self.documents(m)).collect::<Res<_>>()?;
        for m in 1..3 {
            if docs[m] == docs[0] {
                continue;
            }
            let mut diff = String::new();
            let mut retired_only = true;
            let keys: BTreeSet<_> = docs[0].keys().chain(docs[m].keys()).collect();
            for k in keys {
                let (a, b) = (docs[0].get(k), docs[m].get(k));
                if a != b {
                    // A version of a retired identity (a member reset away) that only a
                    // page left as held state elsewhere: no vector covers its origin, so
                    // every pull defers it and the digest never compares it. Not this
                    // test's: convergence across a reset.
                    let retired = |v: Option<&(Stamp, Document)>| {
                        v.is_none_or(|(s, _)| self.retired.contains(&s.node))
                    };
                    retired_only &= (retired(a) || retired(b))
                        && [a, b].iter().flatten().any(|(s, _)| self.retired.contains(&s.node));
                    let _ = write!(
                        diff,
                        " {k:?}: A {:?} / {} {:?};",
                        a.map(|(s, d)| (self.stamp(*s), d.to_string())),
                        LETTERS[m],
                        b.map(|(s, d)| (self.stamp(*s), d.to_string()))
                    );
                }
            }
            let detail = format!("documents differ between A and {}:{diff}", LETTERS[m]);
            if retired_only {
                self.out.retired += 1;
                self.note(format!("R1: {detail}"));
            } else {
                self.fail(Inv::I4, detail);
            }
        }
        // At most one document per (request, position) of a keyed insert.
        for (m, held) in docs.iter().enumerate() {
            let mut seen: HashMap<(i64, i64), String> = HashMap::new();
            for ((c, id), (_, doc)) in held {
                if *c != ITEMS || !doc.get_bool("ins").unwrap_or(false) {
                    continue;
                }
                let at = (doc.get_i64("req").unwrap_or(-1), doc.get_i64("pos").unwrap_or(-1));
                if let Some(other) = seen.insert(at, id.clone()) {
                    let s = format!(
                        "{} holds two documents of r{} position {}: {other} and {id}",
                        LETTERS[m], at.0, at.1
                    );
                    self.fail(Inv::I4, s);
                }
            }
        }
        // Identical live records, after the merge rule.
        for (c, key) in self.keys() {
            let records: Vec<Option<IdempotencyRecord>> =
                (0..3).map(|m| self.live_record(m, c, key)).collect::<Res<_>>()?;
            if records.iter().all(|r| *r == records[0]) {
                continue;
            }
            // Each member's record against the merge of them all. One the merged record
            // dominates (merging it in changes nothing), or none where no document holds
            // a request under the key: finding G3, a record state carried only by entries
            // that lost where the member pulled them from (or a record-only `done`), which
            // never reaches it; harmless while I2 holds, since every entry the member
            // processed is at or above its own `first`. None where a document holds a
            // request under the key, every entry vetted by I2 as it was taken: the effect
            // came without its record, finding G1 (or G2). Anything else fails.
            let merged = records
                .iter()
                .flatten()
                .cloned()
                .reduce(|a, b| a.merge(&b))
                .expect("a record differs, so one is present");
            let mut kinds = Vec::new();
            for m in 0..3 {
                // Whether a live document here holds a request under the key.
                let holds_effect = docs[m].iter().any(|((dc, _), (_, doc))| {
                    *dc == c
                        && ks_of(doc).iter().any(|k| {
                            self.sched.requests.get(*k as usize).is_some_and(|q| q.key == key)
                        })
                });
                kinds.push(match &records[m] {
                    Some(r) if *r == merged => "same",
                    None if !holds_effect => "G3",
                    None => "G1",
                    Some(r) if r.merge(&merged) == merged => "G3",
                    _ => "bad",
                });
            }
            let shown: Vec<String> = records
                .iter()
                .zip(&kinds)
                .map(|(r, kind)| match r {
                    Some(r) => format!(
                        "[{kind}] first {} last {} {:?} origin {} {:?}",
                        self.stamp(r.first),
                        self.stamp(r.last),
                        r.state,
                        self.label(r.origin),
                        r.outcome
                    ),
                    None => format!("[{kind}] none"),
                })
                .collect();
            let detail = format!(
                "key {key} in {}: records differ: {}",
                NAMES[c as usize],
                shown.join(" | ")
            );
            if kinds.contains(&"bad") {
                self.fail(Inv::I4, detail);
                continue;
            }
            if kinds.contains(&"G1") {
                self.out.g1 += 1;
            }
            if kinds.contains(&"G3") {
                self.out.g3 += 1;
            }
            self.note(format!("finding: {detail}"));
            if strict() {
                self.fail(Inv::I4, format!("(strict) {detail}"));
            }
        }
        // I5: on the home track every counter is the number of `$inc` requests accepted
        // for it.
        if self.sched.track == Track::Home {
            for (m, held) in docs.iter().enumerate() {
                for ((c, _), (_, doc)) in held {
                    if *c != ORDERS {
                        continue;
                    }
                    let d = doc.get_i64("_id").unwrap_or(-1);
                    let n = doc.get_i64("n").unwrap_or(0);
                    let want = (0..self.sched.requests.len())
                        .filter(|r| self.accepted[*r] && self.sched.requests[*r].kind.targets(d))
                        .count() as i64;
                    let ks = ks_of(doc);
                    let doubled = ks.len() != ks.iter().collect::<BTreeSet<_>>().len();
                    let explained = doubled
                        && ks.iter().all(|k| {
                            ks.iter().filter(|x| *x == k).count() == 1
                                || self.ghost.explained.contains(&(
                                    ORDERS,
                                    crate::docs::doc_key(&DocId::Int64(d)).unwrap_or_default(),
                                    *k as u8,
                                ))
                        });
                    if n != want && !(explained && !strict()) {
                        let s = format!(
                            "{} counter {d} is {n}, not the {want} requests accepted for it: {ks:?}",
                            LETTERS[m]
                        );
                        self.fail(Inv::I5, s);
                    }
                }
            }
        }
        Ok(())
    }

    /// Whether the commit of request `r` that wrote `entry` straddles `end`, a drop: an
    /// entry of the same node and millisecond at or below it, `entry` above it (finding
    /// G2: the record's `last`, the commit's first entry, is then buried with it).
    fn straddles(&self, r: u8, entry: Stamp, end: Hlc) -> bool {
        entry.hlc > end
            && self.ghost.entries.iter().any(|(s, (q, ..))| {
                *q == r
                    && s.node == entry.node
                    && s.hlc.wall_ms == entry.hlc.wall_ms
                    && s.hlc <= end
            })
    }

    /// The latest drop of `c` minted anywhere: what is at or below it belongs to a life
    /// that has ended, whether or not a member has received the drop yet.
    fn ghost_life_end(&self, c: u8) -> Hlc {
        self.ghost
            .drops
            .iter()
            .filter(|(dc, _)| *dc == c)
            .map(|(_, at)| at.hlc)
            .max()
            .unwrap_or(Hlc::ZERO)
    }

    /// Whether member `m` processed any entry of a request under `key` in `c`.
    fn processed_any(&self, m: usize, c: u8, key: u8) -> bool {
        let node = self.e(m).node_id();
        self.ghost.processed.keys().any(|(n, s)| {
            *n == node
                && !self.ghost.g1b.contains(&(*n, *s))
                && self.ghost.entries.get(s).is_some_and(|(r, rc, _)| {
                    *rc == c && self.sched.requests[*r as usize].key == key
                })
        })
    }
}

// -- Running a schedule ----------------------------------------------------------------------

/// A send, with its crash and the steps between its commits armed at the engine's
/// commit point.
fn send(world: &Rc<RefCell<World>>, op: &Op) -> Res<()> {
    let Step::Send { r, m, crash, inner } = &op.step else { unreachable!("a send") };
    let m = *m as usize;
    world.borrow_mut().out.steps += 1;
    let prepared = world.borrow_mut().prepare(*r, m, op.t)?;
    let Some(p) = prepared else {
        // Not sent: the steps it carried run all the same.
        for (_, ops) in inner {
            for op in ops {
                run_op(world, op)?;
            }
        }
        return Ok(());
    };
    let pending: Rc<RefCell<BTreeMap<u8, Vec<Op>>>> = Rc::new(RefCell::new(BTreeMap::new()));
    for (j, ops) in inner {
        pending.borrow_mut().entry(*j).or_default().extend(ops.iter().cloned());
    }
    let engine = Arc::clone(p.engine.as_ref().expect("the request's engine"));
    // At 0, before the first commit: a failure armed for it. A stop there is a reopen
    // after the request (the drain deadline stops nothing before a first commit, and a
    // stopping engine refuses the steps between commits).
    if let Some((0, how)) = crash
        && *how != Crash::AfterCommit
    {
        arm_crash(&engine, *how);
    }
    world.borrow_mut().running = Some(m);
    let phases = (Arc::clone(&p.phase), Rc::clone(&p.phase_seq));
    arm(Rc::clone(world), Rc::clone(&pending), *crash, Arc::downgrade(&engine), phases, 1);
    drop(engine);
    let answer = call(&p);
    // Disarmed: a hook left armed would run at the next commit on this thread.
    barrier_hooks::at(Point::Released, || {});
    world.borrow_mut().running = None;
    if crash.is_none()
        && let Answer::Failed(e) = &answer
        && !expected_failure(e)
    {
        let s = format!("r{r} on {}: {e}", LETTERS[m]);
        world.borrow_mut().fail(Inv::Error, s);
    }
    world.borrow_mut().after_send(p, answer, crash.is_some())?;
    let rest: Vec<Op> =
        std::mem::take(&mut *pending.borrow_mut()).into_values().flatten().collect();
    for op in rest {
        world.borrow_mut().note(format!("after the request: {op}"));
        run_op(world, &op)?;
    }
    Ok(())
}

fn arm_crash(engine: &Engine, how: Crash) {
    match how {
        Crash::AfterCommit => engine.set_stopping(),
        Crash::BeforeCommit => assert!(engine.arm_test_storage_failure("write")),
        Crash::FsyncFails => assert!(engine.arm_test_storage_failure("sync_data")),
    }
}

/// Arm the `j`-th commit point of the request on `engine`: its steps, then its crash,
/// then the next point.
fn arm(
    world: Rc<RefCell<World>>,
    pending: Rc<RefCell<BTreeMap<u8, Vec<Op>>>>,
    crash: Option<(u8, Crash)>,
    engine: std::sync::Weak<Engine>,
    phases: (Arc<std::sync::atomic::AtomicUsize>, Rc<RefCell<Vec<u64>>>),
    j: u8,
) {
    if j > 8 {
        return;
    }
    barrier_hooks::at(Point::Released, move || {
        let ops = pending.borrow_mut().remove(&j).unwrap_or_default();
        for op in &ops {
            let out = world.borrow_mut().apply_inner(op);
            if let Err(e) = out {
                world.borrow_mut().fail(Inv::Error, format!("between commits: {op}: {e}"));
            }
        }
        phases.1.borrow_mut().push(world.borrow().ghost.seq);
        phases.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some((at, how)) = crash
            && at == j
            && let Some(engine) = engine.upgrade()
        {
            arm_crash(&engine, how);
        }
        arm(Rc::clone(&world), Rc::clone(&pending), crash, engine, phases, j + 1);
    });
}

fn run_op(world: &Rc<RefCell<World>>, op: &Op) -> Res<()> {
    match &op.step {
        Step::Send { crash, inner, .. } if crash.is_some() || !inner.is_empty() => send(world, op),
        _ => world.borrow_mut().apply(op),
    }
}

/// The base the schedule's times are counted from: an hour ahead of the wall clock.
fn base() -> u64 {
    (crate::physical_now_ms() / 1000 + 3_600) * 1000
}

/// `orders` with its counters and an empty `items`, on A, pulled by B and C.
fn setup(w: &mut World) -> Res<()> {
    let a = w.e(0);
    let orders = a.create_collection(DB, NAMES[ORDERS as usize]).map_err(|e| err("create", e))?;
    a.create_collection(DB, NAMES[ITEMS as usize]).map_err(|e| err("create", e))?;
    let counters: Vec<Document> =
        (0..COUNTERS).map(|i| doc! { "_id": i, "n": 0i64, "ks": Vec::<Bson>::new() }).collect();
    a.insert_many(&orders, counters).map_err(|e| err("insert_many", e.source))?;
    w.pull(1, 0, BATCH)?;
    w.pull(2, 0, BATCH)
}

/// The end: quiet, then (home track) every accepted `$inc` driven to its end and quiet
/// again, then every check.
fn settle(w: &mut World) -> Res<()> {
    w.step = "quiescence".into();
    if !w.quiesce()? {
        w.fail(Inv::Error, "still moving after the quiescence rounds");
        return Ok(());
    }
    if w.sched.track == Track::Home {
        for r in w.open_incs() {
            w.drive(r)?;
        }
        if !w.quiesce()? {
            w.fail(Inv::Error, "still moving after the drive");
            return Ok(());
        }
    }
    w.after_step()?;
    w.check_settled()
}

/// Run a schedule and settle it.
pub(crate) fn run(sched: &Schedule) -> Outcome {
    let world = Rc::new(RefCell::new(World::new(sched.clone(), base())));
    let out = setup(&mut world.borrow_mut());
    if let Err(e) = out {
        world.borrow_mut().fail(Inv::Error, format!("setup: {e}"));
    }
    for (i, op) in sched.ops.iter().enumerate() {
        if !world.borrow().out.failures.is_empty() {
            break;
        }
        world.borrow_mut().step = format!("{i} {op}");
        let out = run_op(&world, op).and_then(|()| world.borrow_mut().after_step());
        if let Err(e) = out {
            world.borrow_mut().fail(Inv::Error, e);
        }
    }
    if world.borrow().out.failures.is_empty() {
        let mut w = world.borrow_mut();
        if let Err(e) = settle(&mut w) {
            w.fail(Inv::Error, e);
        }
    }
    barrier_hooks::at(Point::Released, || {});
    let mut w = world.borrow_mut();
    std::mem::take(&mut w.out)
}

pub(crate) fn render(seed: Option<u64>, sched: &Schedule, out: &Outcome) -> String {
    let mut s = String::new();
    let invs: BTreeSet<Inv> = out.failures.iter().map(|f| f.inv).collect();
    let _ = writeln!(
        s,
        "MODEL-KILL: {}",
        invs.iter().map(|i| format!("{i:?}")).collect::<Vec<_>>().join(",")
    );
    if let Some(seed) = seed {
        let _ = writeln!(
            s,
            "seed {seed} track {:?} home {}",
            sched.track, LETTERS[sched.home as usize]
        );
    }
    let _ = writeln!(s, "requests: {:?}", sched.requests);
    let _ = writeln!(s, "schedule:");
    for (i, op) in sched.ops.iter().enumerate() {
        let _ = writeln!(s, "  {i}: {op}");
    }
    let _ = writeln!(s, "failures:");
    for f in &out.failures {
        let _ = writeln!(s, "  {:?}: {}", f.inv, f.detail);
    }
    let _ = writeln!(s, "notes:");
    for n in &out.notes {
        let _ = writeln!(s, "{n}");
    }
    s
}

// -- Generation ------------------------------------------------------------------------------

/// SplitMix64: reproducible from a seed, no dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x5DEE_CE66_D1CE_4E5B)
    }
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn member(&mut self) -> u8 {
        self.below(3) as u8
    }
    fn other(&mut self, m: u8) -> u8 {
        (m + 1 + self.below(2) as u8) % 3
    }
    fn pair(&mut self) -> (u8, u8) {
        let to = self.member();
        (to, self.other(to))
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

struct Gen {
    rng: Rng,
    track: Track,
    home: u8,
    requests: Vec<Request>,
    /// Requests of the current era, which may be sent again.
    open: Vec<u8>,
    skew: [i64; 3],
    g: i64,
    resets: usize,
}

const SKEWS: [i64; 7] = [-20, -10, 0, 0, 0, 10, 20];

impl Gen {
    fn t(&self, m: u8, extra: i64) -> u32 {
        (self.g + extra + self.skew[m as usize]).max(1) as u32
    }

    fn target(&mut self) -> Target {
        let derived: Vec<(u8, u8)> = self
            .requests
            .iter()
            .enumerate()
            .flat_map(|(r, q)| match q.kind {
                Kind::Insert { id: None } => vec![(r as u8, 0)],
                Kind::Bulk => vec![(r as u8, 0), (r as u8, 1)],
                _ => vec![],
            })
            .collect();
        if !derived.is_empty() && self.rng.chance(50) {
            let (r, pos) = derived[self.rng.below(derived.len() as u64) as usize];
            Target::Derived(r, pos)
        } else {
            Target::Client(self.rng.below(3) as u8)
        }
    }

    fn kind(&mut self) -> Kind {
        match self.rng.below(100) {
            0..=29 => Kind::IncMulti,
            30..=41 => Kind::IncSingle { d: self.rng.below(COUNTERS as u64) as u8 },
            42..=55 => Kind::Insert { id: None },
            56..=62 => Kind::Insert { id: Some(self.rng.below(3) as u8) },
            63..=69 => Kind::Bulk,
            70..=84 => Kind::Replace { at: self.target() },
            _ => Kind::Delete { at: self.target() },
        }
    }

    /// A new request: now and then a key reused for another request.
    fn new_request(&mut self) -> u8 {
        let r = self.requests.len() as u8;
        let kind = self.kind();
        let key = if !self.open.is_empty() && self.rng.chance(10) {
            self.requests[self.open[self.rng.below(self.open.len() as u64) as usize] as usize].key
        } else {
            r + 1
        };
        self.requests.push(Request { key, kind });
        self.open.push(r);
        r
    }

    fn member_for(&mut self, r: u8) -> u8 {
        if self.track == Track::Home && self.requests[r as usize].kind.is_inc() {
            self.home
        } else {
            self.rng.member()
        }
    }

    fn send(&mut self, r: u8) -> Op {
        let m = self.member_for(r);
        let crash = self.rng.chance(25).then(|| {
            let how = [Crash::AfterCommit, Crash::BeforeCommit, Crash::FsyncFails]
                [self.rng.below(3) as usize];
            (self.rng.below(4) as u8, how)
        });
        let mut inner = Vec::new();
        if self.rng.chance(40) {
            for _ in 0..1 + self.rng.below(2) {
                let j = 1 + self.rng.below(3) as u8;
                let ops = (0..1 + self.rng.below(2)).map(|_| self.inner_op(r, m)).collect();
                inner.push((j, ops));
            }
        }
        Op { step: Step::Send { r, m, crash, inner }, t: self.t(m, 0) }
    }

    /// A step between two commits of request `r` on `m`.
    fn inner_op(&mut self, r: u8, m: u8) -> Op {
        let extra = 1 + self.rng.below(8) as i64;
        match self.rng.below(100) {
            0..=34 => {
                let (to, from) = self.rng.pair();
                Op { step: Step::Pull { to, from }, t: 0 }
            }
            35..=44 => {
                let (to, from) = self.rng.pair();
                Op { step: Step::PullShort { to, from }, t: 0 }
            }
            45..=69 => {
                // A concurrent attempt on another member: of the same request, or, on the
                // home track (where every `$inc` stays home), of one that is not an `$inc`.
                let other = self.rng.other(m);
                let pick = if self.track == Track::Home && self.requests[r as usize].kind.is_inc() {
                    let not_inc: Vec<u8> = self
                        .open
                        .iter()
                        .copied()
                        .filter(|q| !self.requests[*q as usize].kind.is_inc())
                        .collect();
                    (!not_inc.is_empty())
                        .then(|| not_inc[self.rng.below(not_inc.len() as u64) as usize])
                } else {
                    Some(r)
                };
                match pick {
                    Some(q) => Op {
                        step: Step::Send { r: q, m: other, crash: None, inner: Vec::new() },
                        t: self.t(other, extra),
                    },
                    None => Op { step: Step::Clock { m }, t: self.t(m, extra) },
                }
            }
            70..=84 => {
                let later = extra + self.rng.below(40) as i64;
                Op { step: Step::Clock { m }, t: self.t(m, later) }
            }
            85..=94 => {
                let (to, from) = self.rng.pair();
                let c = if self.rng.chance(50) { ORDERS } else { ITEMS };
                let converge = self.rng.chance(40);
                let restart = to != m && self.rng.chance(50);
                Op { step: Step::Page { to, from, c, converge, restart }, t: 0 }
            }
            _ => Op { step: Step::Restart { m: self.rng.other(m) }, t: 0 },
        }
    }
}

/// A schedule of `len` steps from `seed`.
pub(crate) fn generate(seed: u64, track: Track, len: usize) -> Schedule {
    let mut rng = Rng::new(seed);
    let home = rng.member();
    let skew = std::array::from_fn(|_| SKEWS[rng.below(SKEWS.len() as u64) as usize]);
    let mut plan =
        Gen { rng, track, home, requests: Vec::new(), open: Vec::new(), skew, g: 100, resets: 0 };
    let mut ops = Vec::with_capacity(len);
    for _ in 0..len {
        plan.g += 10;
        if plan.rng.below(10) == 0 {
            let m = plan.rng.below(3) as usize;
            plan.skew[m] = SKEWS[plan.rng.below(SKEWS.len() as u64) as usize];
        }
        let op = match plan.rng.below(100) {
            0..=23 => {
                let r = plan.new_request();
                plan.send(r)
            }
            24..=43 | 94..=99 if !plan.open.is_empty() => {
                let r = plan.open[plan.rng.below(plan.open.len() as u64) as usize];
                plan.send(r)
            }
            24..=43 | 94..=99 => {
                let r = plan.new_request();
                plan.send(r)
            }
            44..=61 => {
                let (to, from) = plan.rng.pair();
                Op { step: Step::Pull { to, from }, t: 0 }
            }
            62..=66 => {
                let (to, from) = plan.rng.pair();
                Op { step: Step::PullShort { to, from }, t: 0 }
            }
            67..=72 => {
                let (to, from) = plan.rng.pair();
                let c = if plan.rng.chance(50) { ORDERS } else { ITEMS };
                let converge = plan.rng.chance(40);
                let restart = plan.rng.chance(60);
                Op { step: Step::Page { to, from, c, converge, restart }, t: 0 }
            }
            73..=75 if plan.resets < SPARES => {
                plan.resets += 1;
                let (mut m, from) = plan.rng.pair();
                if track == Track::Home && m == plan.home {
                    // A reset of the home loses what it alone held, the `$inc`s it
                    // answered among them: data loss, not a retry's concern.
                    m = 3 - m - from;
                }
                Op { step: Step::Reset { m, from }, t: 0 }
            }
            73..=79 => Op { step: Step::Restart { m: plan.rng.member() }, t: 0 },
            80..=82 => Op { step: Step::Crash { m: plan.rng.member() }, t: 0 },
            83..=86 => {
                let m = plan.rng.member();
                Op { step: Step::Drop { m }, t: plan.t(m, 0) }
            }
            87..=91 => {
                let m = plan.rng.member();
                Op { step: Step::Create { m }, t: plan.t(m, 0) }
            }
            _ => {
                // The window passes: an era ends.
                plan.g += WINDOW_SECS as i64 * 1000 + 500;
                plan.open.clear();
                Op { step: Step::Retention { m: plan.rng.member() }, t: plan.g as u32 }
            }
        };
        ops.push(op);
    }
    Schedule { track, home: plan.home, requests: plan.requests, ops }
}

// -- Running many ----------------------------------------------------------------------------

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Totals over a sweep.
#[derive(Debug, Default)]
struct Sweep {
    schedules: usize,
    steps: usize,
    between: usize,
    g1: usize,
    g2: usize,
    g3: usize,
    dropped_elsewhere: usize,
    life_exempt: usize,
    replays: usize,
    resumes: usize,
    crashes: usize,
}

/// Run each seed of `seeds` on both tracks, `threads` at a time; panic with the first
/// failing schedule's report.
fn sweep(seeds: std::ops::Range<u64>, len: usize, threads: usize) -> Sweep {
    let started = std::time::Instant::now();
    let jobs: Vec<(u64, Track)> =
        seeds.clone().flat_map(|seed| [(seed, Track::Home), (seed, Track::Anywhere)]).collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let totals = Mutex::new(Sweep::default());
    let failure: Mutex<Option<String>> = Mutex::new(None);
    std::thread::scope(|scope| {
        for _ in 0..threads.max(1) {
            scope.spawn(|| {
                loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if i >= jobs.len() || failure.lock().unwrap().is_some() {
                        return;
                    }
                    let (seed, track) = jobs[i];
                    let sched = generate(seed, track, len);
                    let out = run(&sched);
                    {
                        let mut t = totals.lock().unwrap();
                        t.schedules += 1;
                        t.steps += out.steps;
                        t.between += out.between;
                        t.g1 += out.g1;
                        t.g2 += out.g2;
                        t.g3 += out.g3;
                        t.dropped_elsewhere += out.dropped_elsewhere;
                        t.life_exempt += out.life_exempt;
                        t.replays += out.replays;
                        t.resumes += out.resumes;
                        t.crashes += out.crashes;
                    }
                    if !out.failures.is_empty() {
                        failure.lock().unwrap().get_or_insert(render(Some(seed), &sched, &out));
                        return;
                    }
                }
            });
        }
    });
    let totals = totals.into_inner().unwrap();
    eprintln!(
        "idempotency model: seeds {seeds:?} x 2 tracks, len {len}: {} schedules, {} steps ({} \
         between commits), {} replays, {} resumes, {} crashes, {} life-rule applications, {} exempt by a drop elsewhere (D1), \
         {} G1, {} G2, {} G3, in {:.1?}",
        totals.schedules,
        totals.steps,
        totals.between,
        totals.replays,
        totals.resumes,
        totals.crashes,
        totals.life_exempt,
        totals.dropped_elsewhere,
        totals.g1,
        totals.g2,
        totals.g3,
        started.elapsed()
    );
    if let Some(report) = failure.into_inner().unwrap() {
        panic!("{report}");
    }
    totals
}

fn threads() -> usize {
    env_u64("IDEM_THREADS", std::thread::available_parallelism().map_or(4, |n| n.get()) as u64)
        as usize
}

/// The schedule length a generated run uses.
const LEN: usize = 30;

/// The CI run: a short seeded sweep of both tracks (`IDEM_CI_SEEDS`, from
/// `IDEM_SEED_START`).
#[test]
fn generated_schedules_short() {
    let start = env_u64("IDEM_SEED_START", 0);
    let count = env_u64("IDEM_CI_SEEDS", 24);
    sweep(start..start + count, LEN, threads().min(4));
}

/// The long run, for a gate of record: `IDEM_SEEDS` seeds (default 1000) from
/// `IDEM_SEED_START` (default 1,000,000, past the CI run's), both tracks,
/// `IDEM_THREADS` at a time.
#[test]
#[ignore = "the model's long run: IDEM_SEEDS seeds of both tracks"]
fn generated_schedules_long() {
    let start = env_u64("IDEM_SEED_START", 1_000_000);
    let count = env_u64("IDEM_SEEDS", 1000);
    sweep(start..start + count, LEN, threads());
}

/// One seed, for a failure's report: `IDEM_SEED`, `IDEM_TRACK` (`home` or anything
/// else for anywhere).
#[test]
#[ignore = "one seed of the model, printed: IDEM_SEED and IDEM_TRACK"]
fn one_seed() {
    let seed = env_u64("IDEM_SEED", 0);
    let track = match std::env::var("IDEM_TRACK").as_deref() {
        Ok("home") => Track::Home,
        _ => Track::Anywhere,
    };
    let sched = generate(seed, track, LEN);
    let out = run(&sched);
    eprintln!("{}", render(Some(seed), &sched, &out));
    assert!(out.failures.is_empty());
}

// -- Finding G1 ------------------------------------------------------------------------------

/// `$inc` of `n` by one, with no key set: the reproducer's own.
struct PlainInc;

impl ModifySpec for PlainInc {
    fn matches(&self, doc: &Document) -> kimmy_core::Result<bool> {
        Ok(doc.contains_key("n"))
    }
    fn compare(&self, _: &Document, _: &Document) -> Ordering {
        Ordering::Equal
    }
    fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
        let mut next = doc.clone();
        next.insert("n", Bson::Int64(doc.get_i64("n").map_err(|e| e.to_string())? + 1));
        Ok(Some(next))
    }
    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        None
    }
}

/// A pull of everything `from` has for `to`, with its records, as the transport's pull
/// makes it: from the requester's witnessed vector.
fn pull_all(to: &Engine, from: &Engine) {
    for _ in 0..8 {
        let theirs = from.version_vector().unwrap();
        let mine = to.witnessed_vector().unwrap();
        let Some(start) = mine.behind(&theirs) else { return };
        let window = from
            .entries_for_peer_holding(start, BATCH, Some(&mine), WalkScope::Background)
            .unwrap();
        let mut outcome = SyncOutcome::default();
        to.apply_peer_window_carrying_into(
            &theirs,
            Some(&mine),
            &window.entries,
            &window.records,
            RecordsVia::Pull,
            PeerWindow {
                scanned_to: window.scanned_to,
                exhausted: window.exhausted,
                passed_through: None,
                asked_partial: false,
            },
            &mut outcome,
        )
        .unwrap();
        if window.exhausted {
            return;
        }
    }
}

/// Finding G1, minimal: Y applies a keyed `$inc` and then writes the counter again; X
/// takes the database by snapshot (the record with it, rowed at its `last`); M pulls
/// from X and is sent the later version without the record, and its coverage then
/// passes the `$inc`'s own entry, so Y never serves it the record either, Y serving
/// throughout. A retry on M applies the `$inc` a second time (n 11 -> 12). Asserts
/// guarantee 2 (a member holding any effect of the write holds its record), so it fails
/// until the design closes the gap.
#[test]
#[ignore = "finding G1: a snapshot-held version is relayed without its record (design change pending)"]
fn finding_g1_a_relay_of_a_snapshot_held_version_drops_the_record() {
    let dir = tempfile::tempdir().unwrap();
    let y = Engine::open(&dir.path().join("y.redb")).unwrap();
    let x = Engine::open(&dir.path().join("x.redb")).unwrap();
    let m = Engine::open(&dir.path().join("m.redb")).unwrap();
    let cy = y.create_collection("shop", "orders").unwrap();
    y.insert(&cy, doc! { "_id": 1i64, "n": 0i64 }).unwrap();
    pull_all(&m, &y);
    let cm = m.get_collection("shop", "orders").unwrap();
    let k = KeyedWrite::new(ScopeHash([1; 32]), Fingerprint([1; 32]));
    let first = y.modify_where_keyed(&cy, &Candidates::Scan, &PlainInc, Some(1), &k).unwrap();
    assert!(matches!(first, Keyed::Fresh(_)));
    // Another write on top: the version holds the `$inc`'s effect.
    y.replace(&cy, &DocId::Int64(1), doc! { "n": 11i64 }, false).unwrap();
    let mut progress = SnapshotProgress::whole_database();
    while !progress.is_complete() {
        let page = y.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        x.apply_snapshot_page(y.node_id(), &mut progress, &page).unwrap();
    }
    assert!(x.idempotency_record(cy.id, &k.scope).unwrap().is_some(), "premise: X holds it");
    pull_all(&m, &x);
    pull_all(&m, &y);
    assert_eq!(m.get(&cm, &DocId::Int64(1)).unwrap().unwrap().get_i64("n").unwrap(), 11);
    let held = m.idempotency_record(cm.id, &k.scope).unwrap();
    let retry = m.modify_where_keyed(&cm, &Candidates::Scan, &PlainInc, Some(1), &k).unwrap();
    let n = m.get(&cm, &DocId::Int64(1)).unwrap().unwrap().get_i64("n").unwrap();
    assert!(
        held.is_some() && matches!(retry, Keyed::Replayed(_)) && n == 11,
        "M holds the `$inc`'s effect without its record ({held:?}), and a retry there applied \
         it again: {retry:?}, n = {n}"
    );
}
