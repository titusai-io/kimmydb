//! A convergence model test: concurrent creates of one collection name are not
//! settled (each member keeps its own `created`, ADR-235 "Left open"), and every
//! member must still end with the same **observable state**, whatever the delivery
//! order and route.
//!
//! **Members.** Three engines in one process, opened from template stores whose node
//! identities are fixed (`0x11..`, `0x22..`, `0x33..`), so A sorts below B below C and a
//! stamp tie breaks the same way on every run.
//!
//! **Clocks.** Every local step carries a time `t` (milliseconds past a base an hour
//! ahead of the wall clock), and the member's clock is moved to it before the step
//! (`advance_clock_past`). The wall clock never overtakes the base during a run, so
//! the stamps a schedule mints are a function of the schedule alone: skew between
//! members is a choice of `t`, and two members minting the same `t` without having
//! seen each other tie on the `Hlc` and are ordered by node.
//!
//! **Steps.** Create a collection; write (an upsert) or delete a document; create or
//! drop a plain index (two definitions share the name `k_1`, so a rival is possible);
//! drop the collection; drop the database; pull entries from a peer (the transport's
//! request: the witnessed vector as `held`, the marked spans, the sender's verdict,
//! with the protocol's own fallbacks, a whole-database page beyond the horizon and a
//! scoped page for an unknown collection); pull a short window (a batch cut after two
//! entries); take a scoped snapshot; take a whole-database snapshot (coverage granted
//! on completion); restart (close, open, the two start reconciles `node::run` makes);
//! collect garbage honouring the operator rule (only what every member has witnessed);
//! and age out everything a member holds (the rule broken: ADR-235's premise).
//!
//! **Quiescence.** After a schedule, every ordered pair syncs until nothing moves (the
//! observable state, `created` and the vectors all stand still for a whole round),
//! **once by entries and once by whole-database pages**, each from the same pre-state:
//! the schedule is run twice from the templates and the two pre-states are compared
//! first (a difference is reported as nondeterminism, not hidden).
//!
//! **The assertion.** Equal on all three members: the databases, the collections, each
//! collection's documents (`_id`, stamp, body), its indexes (name and the definition
//! `IndexMeta::differences` compares), its incarnation floor, and the collection and
//! database tombstones (full stamps; below the highest cutoff garbage collection used
//! anywhere they are not compared, since each member collects on its own). `created`
//! (and the re-dating marker, and an index's creation stamp) is **recorded and not
//! asserted**: that is the decision under test. And **no lost write**: computed from a
//! ghost history the test keeps (the stamps minted, the collection and database
//! drops), not from the engine, a document whose latest event is a write stamped
//! after every drop that applies to it (its collection's, and every database drop,
//! whether or not the writer had heard of it: ADR-236) must be on every member with
//! that stamp.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::OnceLock;

use bson::doc;
use kimmy_core::{CollectionId, DocId, Hlc, IndexField, Stamp};

use crate::{
    Engine, MarkedRange, PeerWindow, RetentionPolicy, ServeVerdict, SnapshotProgress, StorageError,
    SyncOutcome, WalkScope, tables,
};

const DB: &str = "shop";
const NAMES: [&str; 2] = ["orders", "items"];
const LETTERS: [char; 3] = ['A', 'B', 'C'];
const BATCH: usize = 1024;
/// A batch cut: the window a short pull takes.
const SHORT: usize = 2;
/// Rounds of quiescence before a cluster that still moves is reported.
const QUIESCE_ROUNDS: usize = 8;

pub(crate) const A: u8 = 0;
pub(crate) const B: u8 = 1;
pub(crate) const C: u8 = 2;
pub(crate) const ORDERS: u8 = 0;
pub(crate) const ITEMS: u8 = 1;

// -- Schedules ----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Step {
    Create {
        m: u8,
        c: u8,
    },
    Write {
        m: u8,
        c: u8,
        d: u8,
    },
    Delete {
        m: u8,
        c: u8,
        d: u8,
    },
    Index {
        m: u8,
        c: u8,
        ix: u8,
    },
    DropIndex {
        m: u8,
        c: u8,
        ix: u8,
    },
    Drop {
        m: u8,
        c: u8,
    },
    DropDb {
        m: u8,
    },
    Pull {
        to: u8,
        from: u8,
    },
    PullShort {
        to: u8,
        from: u8,
    },
    Scoped {
        to: u8,
        from: u8,
        c: u8,
    },
    Whole {
        to: u8,
        from: u8,
    },
    Restart {
        m: u8,
    },
    Gc {
        m: u8,
    },
    AgeOut {
        m: u8,
    },
    /// From here until quiescence the drop purger runs only where a `Purge` step
    /// says: a creation over a dropped life's rows waits (ADR-189), as it does while a
    /// live purger has not yet reached it.
    PurgerOff,
    Purge {
        m: u8,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Op {
    pub step: Step,
    /// The member's wall clock for a local step, ms past the base. Ignored by a
    /// delivery.
    pub t: u32,
}

/// The index slots: `k_1` has two definitions, so two members can create rivals.
fn index_slot(ix: u8) -> (&'static str, Vec<IndexField>) {
    match ix {
        0 => ("k_1", vec![IndexField::ascending("k")]),
        1 => ("k_1", vec![IndexField::descending("k")]),
        _ => ("j_1", vec![IndexField::ascending("j")]),
    }
}

impl Step {
    fn is_local(&self) -> bool {
        matches!(
            self,
            Step::Create { .. }
                | Step::Write { .. }
                | Step::Delete { .. }
                | Step::Index { .. }
                | Step::DropIndex { .. }
                | Step::Drop { .. }
                | Step::DropDb { .. }
        )
    }
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let l = |m: u8| LETTERS[m as usize];
        let n = |c: u8| NAMES[c as usize];
        let t = self.t;
        match self.step {
            Step::Create { m, c } => write!(f, "{}.create({})@{t}", l(m), n(c)),
            Step::Write { m, c, d } => write!(f, "{}.write({}/{d})@{t}", l(m), n(c)),
            Step::Delete { m, c, d } => write!(f, "{}.delete({}/{d})@{t}", l(m), n(c)),
            Step::Index { m, c, ix } => {
                let (name, fields) = index_slot(ix);
                let dir = if fields[0].descending { "desc" } else { "asc" };
                write!(f, "{}.index({},{name}:{dir})@{t}", l(m), n(c))
            }
            Step::DropIndex { m, c, ix } => {
                write!(f, "{}.dropindex({},{})@{t}", l(m), n(c), index_slot(ix).0)
            }
            Step::Drop { m, c } => write!(f, "{}.drop({})@{t}", l(m), n(c)),
            Step::DropDb { m } => write!(f, "{}.dropdb@{t}", l(m)),
            Step::Pull { to, from } => write!(f, "{}<-{} entries", l(to), l(from)),
            Step::PullShort { to, from } => {
                write!(f, "{}<-{} entries(cut at {SHORT})", l(to), l(from))
            }
            Step::Scoped { to, from, c } => write!(f, "{}<-{} scoped({})", l(to), l(from), n(c)),
            Step::Whole { to, from } => write!(f, "{}<-{} whole", l(to), l(from)),
            Step::Restart { m } => write!(f, "{}.restart", l(m)),
            Step::Gc { m } => write!(f, "{}.gc", l(m)),
            Step::AgeOut { m } => write!(f, "{}.ageout", l(m)),
            Step::PurgerOff => write!(f, "purger off"),
            Step::Purge { m } => write!(f, "{}.purge", l(m)),
        }
    }
}

pub(crate) fn show(ops: &[Op]) -> String {
    ops.iter().map(|op| op.to_string()).collect::<Vec<_>>().join("; ")
}

/// The schedule as Rust, to paste into a named row.
pub(crate) fn as_rust(ops: &[Op]) -> String {
    let mut out = String::from("vec![");
    for op in ops {
        let _ = write!(out, "op(Step::{:?}, {}), ", op.step, op.t);
    }
    out.push(']');
    out
}

/// Read back what [`show`] prints.
pub(crate) fn parse(text: &str) -> Vec<Op> {
    let member = |c: char| LETTERS.iter().position(|l| *l == c).expect("a member") as u8;
    let coll = |n: &str| NAMES.iter().position(|x| *x == n).expect("a collection") as u8;
    text.split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            if s == "purger off" {
                return Op { step: Step::PurgerOff, t: 0 };
            }
            let (body, t) = match s.rsplit_once('@') {
                Some((body, t)) => (body, t.parse::<u32>().expect("a time")),
                None => (s, 0),
            };
            let first = body.chars().next().expect("a step");
            if let Some(rest) = body.strip_prefix(&format!("{first}<-")) {
                let from = member(rest.chars().next().expect("a peer"));
                let to = member(first);
                let how = rest[1..].trim();
                let step = if how == "entries" {
                    Step::Pull { to, from }
                } else if how.starts_with("entries(") {
                    Step::PullShort { to, from }
                } else if how == "whole" {
                    Step::Whole { to, from }
                } else if let Some(c) =
                    how.strip_prefix("scoped(").and_then(|r| r.strip_suffix(')'))
                {
                    Step::Scoped { to, from, c: coll(c) }
                } else {
                    panic!("a delivery: {s}")
                };
                return Op { step, t };
            }
            let m = member(first);
            let verb = &body[2..];
            let (name, arg) = match verb.split_once('(') {
                Some((name, arg)) => (name, arg.strip_suffix(')').expect("a closing paren")),
                None => (verb, ""),
            };
            let step = match name {
                "create" => Step::Create { m, c: coll(arg) },
                "drop" => Step::Drop { m, c: coll(arg) },
                "dropdb" => Step::DropDb { m },
                "restart" => Step::Restart { m },
                "gc" => Step::Gc { m },
                "ageout" => Step::AgeOut { m },
                "purge" => Step::Purge { m },
                "write" | "delete" => {
                    let (c, d) = arg.split_once('/').expect("collection/doc");
                    let (c, d) = (coll(c), d.parse().expect("a doc"));
                    if name == "write" { Step::Write { m, c, d } } else { Step::Delete { m, c, d } }
                }
                "index" => {
                    let (c, ix) = arg.split_once(',').expect("collection,index");
                    let ix = match ix {
                        "k_1:asc" => 0,
                        "k_1:desc" => 1,
                        _ => 2,
                    };
                    Step::Index { m, c: coll(c), ix }
                }
                "dropindex" => {
                    let (c, ix) = arg.split_once(',').expect("collection,index");
                    Step::DropIndex { m, c: coll(c), ix: if ix == "k_1" { 0 } else { 2 } }
                }
                _ => panic!("a step: {s}"),
            };
            Op { step, t }
        })
        .collect()
}

pub(crate) fn op(step: Step, t: u32) -> Op {
    Op { step, t }
}

// -- Templates and members ------------------------------------------------------------------

/// Three empty stores with fixed identities, opened once and copied per run.
fn templates() -> &'static [PathBuf; 3] {
    static TEMPLATES: OnceLock<(tempfile::TempDir, [PathBuf; 3])> = OnceLock::new();
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
    engine: Option<Engine>,
}

// -- The ghost -----------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct GhostWrite {
    c: usize,
    d: i64,
    stamp: Stamp,
    by: usize,
    step: usize,
}

/// What happened, as the test saw it: the stamps the members minted for what they
/// were asked to do. Nothing here is read back from what a member kept.
#[derive(Clone, Debug, Default)]
struct Ghost {
    writes: Vec<GhostWrite>,
    deletes: Vec<(usize, i64, Stamp)>,
    /// Every collection drop minted, a database drop's own drops of its collections
    /// included.
    drops: Vec<(usize, Stamp)>,
    db_drops: Vec<Stamp>,
}

impl Ghost {
    fn last_event_is_a_delete(&self, c: usize, d: i64) -> bool {
        let write = self.writes.iter().filter(|w| w.c == c && w.d == d).map(|w| w.stamp).max();
        let delete = self.deletes.iter().filter(|x| x.0 == c && x.1 == d).map(|x| x.2).max();
        match (write, delete) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some(w), Some(x)) => x > w,
        }
    }
}

// -- Observable state --------------------------------------------------------------------

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Coll {
    documents: BTreeMap<i64, (Stamp, String)>,
    indexes: BTreeMap<String, String>,
    floor: Option<Hlc>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Observed {
    databases: BTreeSet<String>,
    collections: BTreeMap<&'static str, Coll>,
    /// Collection tombstones by name, the database's as `#database`.
    tombstones: BTreeMap<String, Stamp>,
}

/// Recorded, not asserted: what concurrent creates are allowed to leave different.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Recorded {
    created: BTreeMap<&'static str, Hlc>,
    redated_from: BTreeMap<&'static str, Hlc>,
    index_created: BTreeMap<String, Option<Stamp>>,
}

fn name_of(id: CollectionId) -> Option<String> {
    if id == kimmy_core::database_drop_id(DB) {
        return Some("#database".into());
    }
    NAMES.iter().find(|n| CollectionId::derive(DB, n) == id).map(|n| n.to_string())
}

fn index_definition(meta: &kimmy_core::IndexMeta) -> String {
    format!(
        "{:?} unique={} {:?} ttl={:?} partial={:?}",
        meta.fields, meta.unique, meta.enforcement, meta.expire_after_secs, meta.partial_filter
    )
}

// -- Findings ------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Category {
    Error,
    Nondeterministic,
    NotQuiescent,
    Databases,
    Collections,
    Docs,
    /// Documents differ after a member aged out what the operator rule says it must
    /// keep (ADR-085's documented resurrection); reported, not a finding by itself.
    DocsAfterAgeOut,
    Indexes,
    Floor,
    CollTombstone,
    DbTombstone,
    /// Two tombstones of one drop at the same `Hlc` and different nodes.
    TombstoneNode,
    /// Each route converged, to different states: the same schedule ends one way by
    /// entries (with the repair loop) and another by pages.
    RouteDependent,
    /// A collection tombstone one member holds and another does not (or holds
    /// older), where both hold the collection live with a floor at or above it: the
    /// floor already refuses everything the tombstone would, so nothing either
    /// member decides differs. Reported (the tombstone table is observable, and it
    /// is what the existence half reads), kept out of the signature a sweep groups
    /// by, so it does not hide the differences that decide something.
    TombstoneSubsumed,
    LostWrite,
    /// A page left its receiver's clock behind a stamp it restored that the sender's
    /// coverage names (ADR-233 item 1: a collection's `created`, a carried collection or
    /// index drop, a scoped page's `dropped`). The receiver's next local stamp could then
    /// sort below state it holds; the consequences reach other members only through a
    /// later step that mints, so this is checked where it arises.
    ClockBehind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Route {
    /// Pulls only, with the protocol's own fallbacks.
    Entries,
    /// The same cluster after `Entries`, with the divergence check's repairs
    /// added (the existence half's scoped page, the count half's replay from
    /// `min(floor, created, tombstone)`): the entries route as the loop runs it.
    Repaired,
    /// Whole-database pages only.
    Pages,
}

#[derive(Clone, Debug)]
pub(crate) struct Failure {
    pub category: Category,
    pub detail: String,
}

#[derive(Debug)]
pub(crate) struct RouteOutcome {
    pub route: Route,
    pub failures: Vec<Failure>,
    pub report: String,
    /// Pages the entries route needed (the protocol's own fallbacks).
    pub fallbacks: usize,
    /// Whether `created` differs between members at the end (recorded only).
    pub created_differs: bool,
    /// Member A's databases, collections, documents, indexes and floors, when the
    /// route converged: what the routes are compared by.
    pub settled: Option<String>,
}

impl RouteOutcome {
    pub(crate) fn signature(&self) -> BTreeSet<Category> {
        self.failures
            .iter()
            .map(|f| f.category)
            .filter(|c| *c != Category::DocsAfterAgeOut && *c != Category::TombstoneSubsumed)
            .collect()
    }
}

// -- The cluster ---------------------------------------------------------------------------

struct Cluster {
    members: Vec<Member>,
    base: u64,
    ghost: Ghost,
    notes: Vec<String>,
    fallbacks: usize,
    /// The highest cutoff any member collected garbage at.
    gc_cutoff: Option<Hlc>,
    aged_out: bool,
    step: usize,
    /// Set by `PurgerOff`: purges run only at `Purge` steps until quiescence.
    defer_purges: bool,
    /// Every page that left its receiver's clock behind a stamp it vouched for.
    clock_behind: Vec<String>,
}

type Res<T> = std::result::Result<T, String>;

fn err(what: &str, e: impl std::fmt::Display) -> String {
    format!("{what}: {e}")
}

impl Cluster {
    fn new(base: u64) -> Self {
        let members = templates()
            .iter()
            .map(|template| {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("kimmy.redb");
                std::fs::copy(template, &path).unwrap();
                let engine = Engine::open(&path).unwrap();
                Member { _dir: dir, path, engine: Some(engine) }
            })
            .collect();
        Cluster {
            members,
            base,
            ghost: Ghost::default(),
            notes: Vec::new(),
            fallbacks: 0,
            gc_cutoff: None,
            aged_out: false,
            step: 0,
            defer_purges: false,
            clock_behind: Vec::new(),
        }
    }

    fn e(&self, m: usize) -> &Engine {
        self.members[m].engine.as_ref().expect("the member is open")
    }

    fn letter_of(&self, node: kimmy_core::NodeId) -> char {
        (0..3).find(|m| self.e(*m).node_id() == node).map_or('?', |m| LETTERS[m])
    }

    fn hlc(&self, h: Hlc) -> String {
        if h == Hlc::ZERO {
            return "0".into();
        }
        let rel = h.wall_ms as i64 - self.base as i64;
        if h.counter == 0 { format!("t{rel}") } else { format!("t{rel}.{}", h.counter) }
    }

    fn stamp(&self, s: Stamp) -> String {
        format!("{}{}", self.hlc(s.hlc), self.letter_of(s.node))
    }

    fn note(&mut self, s: String) {
        self.notes.push(format!("  [{}] {s}", self.step));
    }

    /// Move member `m`'s clock to `t` before a local step.
    fn clock(&self, m: usize, t: u32) {
        let e = self.e(m);
        e.advance_clock_past(&Stamp::new(Hlc::new(self.base + u64::from(t), 0), e.node_id()));
    }

    fn tombstones(&self, m: usize) -> Res<BTreeMap<CollectionId, Stamp>> {
        Ok(self
            .e(m)
            .collections_dropped()
            .map_err(|e| err("collections_dropped", e))?
            .into_iter()
            .collect())
    }

    fn finish_purges(&self) -> Res<()> {
        for m in 0..3 {
            self.e(m).finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
        }
        Ok(())
    }

    fn run(&mut self, ops: &[Op]) -> Res<()> {
        for (i, op) in ops.iter().enumerate() {
            self.step = i;
            self.apply(*op).map_err(|e| format!("step {i} ({op}): {e}"))?;
            if !self.defer_purges {
                self.finish_purges()?;
            }
        }
        Ok(())
    }

    fn apply(&mut self, op: Op) -> Res<()> {
        let before = if op.step.is_local() {
            Some(self.tombstones(Self::member_of(op.step))?)
        } else {
            None
        };
        match op.step {
            Step::Create { m, c } => {
                let m = m as usize;
                self.clock(m, op.t);
                let mut attempt = 0;
                loop {
                    match self.e(m).create_collection(DB, NAMES[c as usize]) {
                        Ok(meta) => {
                            let s = format!("{} created {}", LETTERS[m], self.hlc(meta.created));
                            self.note(s);
                        }
                        Err(StorageError::Core(kimmy_core::Error::CollectionExists { .. })) => {
                            self.note(format!("{} holds it already", LETTERS[m]));
                        }
                        Err(StorageError::CollectionPurging { .. }) if attempt == 0 => {
                            attempt += 1;
                            self.finish_purges()?;
                            continue;
                        }
                        Err(e) => return Err(err("create_collection", e)),
                    }
                    break;
                }
            }
            Step::Write { m, c, d } => {
                let m = m as usize;
                let Ok(coll) = self.e(m).get_collection(DB, NAMES[c as usize]) else {
                    self.note(format!("{} lacks {}: no write", LETTERS[m], NAMES[c as usize]));
                    return Ok(());
                };
                self.clock(m, op.t);
                let body = doc! {
                    "_id": i64::from(d), "k": i64::from(d), "j": i64::from(d),
                    "v": self.step as i64, "by": LETTERS[m].to_string(),
                };
                let out = self
                    .e(m)
                    .replace(&coll, &DocId::Int64(i64::from(d)), body, true)
                    .map_err(|e| err("replace", e))?;
                let stamp = out.stamp.ok_or("a write that wrote nothing")?;
                self.ghost.writes.push(GhostWrite {
                    c: c as usize,
                    d: i64::from(d),
                    stamp,
                    by: m,
                    step: self.step,
                });
                let s = format!("wrote {}/{d} at {}", NAMES[c as usize], self.stamp(stamp));
                self.note(s);
            }
            Step::Delete { m, c, d } => {
                let m = m as usize;
                let Ok(coll) = self.e(m).get_collection(DB, NAMES[c as usize]) else {
                    return Ok(());
                };
                self.clock(m, op.t);
                if let Some(stamp) = self
                    .e(m)
                    .delete_if(&coll, &DocId::Int64(i64::from(d)), None)
                    .map_err(|e| err("delete", e))?
                {
                    self.ghost.deletes.push((c as usize, i64::from(d), stamp));
                    let s = format!("deleted {}/{d} at {}", NAMES[c as usize], self.stamp(stamp));
                    self.note(s);
                }
            }
            Step::Index { m, c, ix } => {
                let m = m as usize;
                if self.e(m).get_collection(DB, NAMES[c as usize]).is_err() {
                    return Ok(());
                }
                self.clock(m, op.t);
                let (name, fields) = index_slot(ix);
                match self.e(m).create_index(
                    DB,
                    NAMES[c as usize],
                    fields,
                    false,
                    Some(name.to_string()),
                ) {
                    Ok(meta) => {
                        let s = format!(
                            "{} index {name} at {:?}",
                            LETTERS[m],
                            meta.created.map(|s| self.stamp(s))
                        );
                        self.note(s);
                    }
                    // A rival definition of a name held here: the client's error.
                    Err(e) => self.note(format!("{} index {name} refused: {e}", LETTERS[m])),
                }
            }
            Step::DropIndex { m, c, ix } => {
                let m = m as usize;
                if self.e(m).get_collection(DB, NAMES[c as usize]).is_err() {
                    return Ok(());
                }
                self.clock(m, op.t);
                let name = index_slot(ix).0;
                match self.e(m).drop_index(DB, NAMES[c as usize], name) {
                    Ok(held) => {
                        self.note(format!("{} dropped index {name} (held {held})", LETTERS[m]))
                    }
                    Err(e) => self.note(format!("{} drop index {name} refused: {e}", LETTERS[m])),
                }
            }
            Step::Drop { m, c } => {
                let m = m as usize;
                self.clock(m, op.t);
                let held =
                    self.e(m).drop_collection(DB, NAMES[c as usize]).map_err(|e| err("drop", e))?;
                self.note(format!("{} dropped {} (held {held})", LETTERS[m], NAMES[c as usize]));
            }
            Step::DropDb { m } => {
                let m = m as usize;
                self.clock(m, op.t);
                let held = self.e(m).drop_database(DB).map_err(|e| err("drop_database", e))?;
                self.note(format!("{} dropped the database (held {held})", LETTERS[m]));
            }
            Step::Pull { to, from } => self.pull(to as usize, from as usize, BATCH)?,
            Step::PullShort { to, from } => self.pull(to as usize, from as usize, SHORT)?,
            Step::Scoped { to, from, c } => self.transfer(
                to as usize,
                from as usize,
                Some(CollectionId::derive(DB, NAMES[c as usize])),
            )?,
            Step::Whole { to, from } => self.transfer(to as usize, from as usize, None)?,
            Step::Restart { m } => self.restart(m as usize)?,
            Step::Gc { m } => self.gc(m as usize, false)?,
            Step::AgeOut { m } => self.gc(m as usize, true)?,
            Step::PurgerOff => self.defer_purges = true,
            Step::Purge { m } => {
                self.e(m as usize).finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
            }
        }
        // The drops a local step minted, read as the tombstones it wrote.
        if let Some(before) = before {
            let m = Self::member_of(op.step);
            for (id, stamp) in self.tombstones(m)? {
                if before.get(&id).is_some_and(|held| *held >= stamp) {
                    continue;
                }
                if id == kimmy_core::database_drop_id(DB) {
                    self.ghost.db_drops.push(stamp);
                } else if let Some(c) = NAMES.iter().position(|n| CollectionId::derive(DB, n) == id)
                {
                    self.ghost.drops.push((c, stamp));
                }
            }
        }
        Ok(())
    }

    fn member_of(step: Step) -> usize {
        match step {
            Step::Create { m, .. }
            | Step::Write { m, .. }
            | Step::Delete { m, .. }
            | Step::Index { m, .. }
            | Step::DropIndex { m, .. }
            | Step::Drop { m, .. }
            | Step::DropDb { m }
            | Step::Restart { m }
            | Step::Gc { m }
            | Step::AgeOut { m }
            | Step::Purge { m } => m as usize,
            Step::PurgerOff => 0,
            Step::Pull { to, .. }
            | Step::PullShort { to, .. }
            | Step::Scoped { to, .. }
            | Step::Whole { to, .. } => to as usize,
        }
    }

    /// One pull, as the transport makes it: the witnessed vector as `held`, the
    /// marked spans, the sender's verdict, and the protocol's fallbacks (a
    /// whole-database page beyond the horizon, a scoped page for a collection the
    /// batch stopped at). `limit` below [`BATCH`] takes one short window only.
    fn pull(&mut self, to: usize, from: usize, limit: usize) -> Res<()> {
        let mut stopped_at: Option<Stamp> = None;
        for _ in 0..32 {
            let src = self.e(from);
            let dst = self.e(to);
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
            let Some(start) = start else { return Ok(()) };
            match src.serve_verdict(&mine).map_err(|e| err("serve_verdict", e))? {
                ServeVerdict::Serve => {}
                verdict => {
                    self.fallbacks += 1;
                    self.note(format!(
                        "{}<-{}: {verdict:?}, a whole-database page instead",
                        LETTERS[to], LETTERS[from]
                    ));
                    return self.transfer(to, from, None);
                }
            }
            let window = src
                .entries_for_peer_marked(start, limit, Some(&mine), &marked, WalkScope::Background)
                .map_err(|e| err("entries_for_peer", e))?;
            let mut outcome = SyncOutcome::default();
            let applied = dst.apply_peer_window_into(
                &theirs,
                Some(&mine),
                &window.entries,
                PeerWindow {
                    scanned_to: window.scanned_to,
                    exhausted: window.exhausted,
                    passed_through: None,
                    asked_partial: false,
                },
                &mut outcome,
            );
            applied.map_err(|e| err("apply_peer_window", e))?;
            if outcome.purge_pending_at.is_some() {
                if self.defer_purges {
                    // The window ends before the creation; a later round asks again.
                    self.note(format!(
                        "{}<-{}: a creation waits for the purger; the window ends before it",
                        LETTERS[to], LETTERS[from]
                    ));
                    return Ok(());
                }
                dst.finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
                continue;
            }
            if let Some(unknown) = outcome.unknown.clone() {
                if stopped_at == Some(unknown.stamp) {
                    // The scoped page did not give it the collection: the entries
                    // route stops here for good.
                    self.note(format!(
                        "{}<-{}: stuck at an entry for {} it cannot get",
                        LETTERS[to],
                        LETTERS[from],
                        name_of(unknown.id).unwrap_or_default()
                    ));
                    return Ok(());
                }
                stopped_at = Some(unknown.stamp);
                self.fallbacks += 1;
                self.note(format!(
                    "{}<-{}: stopped at {} for {}, a scoped page of it",
                    LETTERS[to],
                    LETTERS[from],
                    self.stamp(unknown.stamp),
                    name_of(unknown.id).unwrap_or_default()
                ));
                self.transfer(to, from, Some(unknown.id))?;
                continue;
            }
            if limit < BATCH || window.exhausted {
                return Ok(());
            }
        }
        Err("a pull that never ended".into())
    }

    fn transfer(&mut self, to: usize, from: usize, scope: Option<CollectionId>) -> Res<()> {
        let mut progress =
            scope.map_or_else(SnapshotProgress::whole_database, SnapshotProgress::of_collection);
        let mut waits = 0;
        while !progress.is_complete() {
            let src = self.e(from);
            let dst = self.e(to);
            let page = src
                .snapshot_page(progress.after().cloned(), progress.scope())
                .map_err(|e| err("snapshot_page", e))?;
            let applied = dst
                .apply_snapshot_page(src.node_id(), &mut progress, &page)
                .map_err(|e| err("apply_snapshot_page", e))?;
            if applied.purge_pending.is_none() {
                self.check_the_clock_followed(to, from, scope, &page);
                continue;
            }
            if self.defer_purges {
                self.note(format!(
                    "{}<-{}: a page waits for the purger; asked again in a later round",
                    LETTERS[to], LETTERS[from]
                ));
                return Ok(());
            }
            waits += 1;
            if waits > 4 {
                return Err("a page that waits for a purge that never finishes".into());
            }
            self.e(to).finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
        }
        Ok(())
    }

    /// ADR-233 item 1, checked where it arises: once a page is applied, the receiver's
    /// clock is at or past every stamp the page restores that the sender's own coverage
    /// names. A stamp the coverage does not name is stored and not witnessed by design
    /// (ADR-163), and is not checked.
    fn check_the_clock_followed(
        &mut self,
        to: usize,
        from: usize,
        scope: Option<CollectionId>,
        page: &crate::SnapshotPage,
    ) {
        let named = |hlc: Hlc| page.versions.iter().any(|(_, covered)| hlc <= covered);
        let covered = |stamp: &Stamp| stamp.hlc <= page.versions.get(stamp.node);
        let mut vouched: Vec<(&str, Hlc)> = Vec::new();
        for state in &page.collections {
            if let Some(created) = state.created.filter(|c| named(*c)) {
                vouched.push(("a creation", created));
            }
        }
        if scope.is_none() {
            for (_, dropped) in page.dropped_collections.iter().filter(|(_, s)| covered(s)) {
                vouched.push(("a carried drop", dropped.hlc));
            }
        }
        if let Some(dropped) = page.dropped.filter(|s| scope.is_some() && covered(s)) {
            vouched.push(("a scoped page's drop", dropped.hlc));
        }
        for (collection, _, dropped) in &page.dropped_indexes {
            if scope.is_none_or(|scope| scope == *collection) && covered(dropped) {
                vouched.push(("a carried index drop", dropped.hlc));
            }
        }
        let last = self.e(to).clock_last();
        for (what, stamp) in vouched {
            if last < stamp {
                let s = format!(
                    "step {}: {}<-{} left the clock at {} behind {what} at {}",
                    self.step,
                    LETTERS[to],
                    LETTERS[from],
                    self.hlc(last),
                    self.hlc(stamp)
                );
                if !self.clock_behind.contains(&s) {
                    self.clock_behind.push(s);
                }
            }
        }
    }

    fn restart(&mut self, m: usize) -> Res<()> {
        let engine = self.members[m].engine.take().expect("open");
        engine.close().map_err(|e| format!("close: {e:?}"))?;
        let engine = Engine::open(&self.members[m].path).map_err(|e| err("open", e))?;
        engine.reconcile_database_tombstones().map_err(|e| err("reconcile_database", e))?;
        engine.reconcile_late_drops().map_err(|e| err("reconcile_late_drops", e))?;
        engine.finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
        self.members[m].engine = Some(engine);
        Ok(())
    }

    /// Collect what is older than a cutoff on member `m`: with `aged_out`, everything it
    /// holds (the operator rule broken: a peer partitioned past retention); otherwise
    /// only what every member has witnessed (the rule kept).
    fn gc(&mut self, m: usize, aged_out: bool) -> Res<()> {
        let mut origins = BTreeSet::new();
        let mut witnessed = Vec::new();
        for j in 0..3 {
            let w = self.e(j).witnessed_vector().map_err(|e| err("witnessed_vector", e))?;
            let v = self.e(j).version_vector().map_err(|e| err("version_vector", e))?;
            for (node, _) in w.iter().chain(v.iter()) {
                origins.insert(node);
            }
            witnessed.push(w);
        }
        let cutoff = if aged_out {
            let w = &witnessed[m];
            w.iter().map(|(_, h)| h).max().unwrap_or(Hlc::ZERO).wall_ms + 1
        } else {
            origins
                .iter()
                .flat_map(|node| witnessed.iter().map(move |w| w.get(*node)))
                .min()
                .unwrap_or(Hlc::ZERO)
                .wall_ms
        };
        if cutoff == 0 {
            return Ok(());
        }
        let out = self
            .e(m)
            .collect_garbage_at(cutoff + 1_000, RetentionPolicy::new(1, 1))
            .map_err(|e| err("collect_garbage", e))?;
        let at = Hlc::new(cutoff, 0);
        if self.gc_cutoff.is_none_or(|held| held < at) {
            self.gc_cutoff = Some(at);
        }
        self.aged_out |= aged_out;
        let s = format!(
            "{} collected below {} ({} entries, {} tombstones)",
            LETTERS[m],
            self.hlc(at),
            out.oplog_removed,
            out.tombstones_removed
        );
        self.note(s);
        Ok(())
    }

    // -- Quiescence and comparison ----------------------------------------------------

    /// Everything that can still move: the observable state, what is recorded, and both
    /// vectors of every member.
    fn fingerprint(&self) -> Res<String> {
        let mut out = String::new();
        for m in 0..3 {
            let (observed, recorded) = self.observe(m)?;
            let e = self.e(m);
            let _ = write!(
                out,
                "{observed:?}{recorded:?}{:?}{:?}",
                e.version_vector().map_err(|e| err("version_vector", e))?,
                e.witnessed_vector().map_err(|e| err("witnessed_vector", e))?
            );
        }
        Ok(out)
    }

    fn quiesce(&mut self, route: Route) -> Res<bool> {
        self.step = usize::MAX;
        // The purger catches up.
        self.defer_purges = false;
        self.finish_purges()?;
        for _ in 0..QUIESCE_ROUNDS {
            let before = self.fingerprint()?;
            for to in 0..3 {
                for from in 0..3 {
                    if to == from {
                        continue;
                    }
                    match route {
                        Route::Entries | Route::Repaired => self.pull(to, from, BATCH)?,
                        Route::Pages => self.transfer(to, from, None)?,
                    }
                    self.finish_purges()?;
                }
            }
            if route == Route::Repaired {
                self.repair_round()?;
                self.finish_purges()?;
            }
            if self.fingerprint()? == before {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The divergence check every contact makes, and the repair it plans
    /// (`peers.rs`): a collection the peer holds that this member does not, past
    /// this member's tombstone, is pulled as a scoped page; a collection whose live
    /// count differs is replayed from the peer's oplog from `min(floor, created,
    /// tombstone)` (`Engine::count_repair_replay_from`), or
    /// pulled as a scoped page when the peer cannot serve from there. One check is
    /// enough here: the cluster is level, so the second check would confirm it.
    fn repair_round(&mut self) -> Res<()> {
        for (me, my_letter) in LETTERS.iter().enumerate() {
            for (peer, peer_letter) in LETTERS.iter().enumerate() {
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
                        let meta = mine.collection_by_id(id).map_err(|e| err("by_id", e))?;
                        counts.push((id, meta));
                    }
                }
                for id in findings.existence {
                    self.note(format!(
                        "repair: {} lacks {}, a scoped page from {}",
                        my_letter,
                        name_of(id).unwrap_or_default(),
                        peer_letter
                    ));
                    self.transfer(me, peer, Some(id))?;
                }
                for (id, meta) in counts {
                    match meta {
                        Some(meta) => {
                            // Where `peers.rs` plans it from.
                            let from = self
                                .e(me)
                                .count_repair_replay_from(&meta)
                                .map_err(|e| err("count_repair_replay_from", e))?;
                            self.note(format!(
                                "repair: {}'s count of {} differs from {}'s, a replay from {}",
                                my_letter,
                                name_of(id).unwrap_or_default(),
                                peer_letter,
                                self.hlc(from)
                            ));
                            self.replay(me, peer, from, id)?;
                        }
                        None => self.transfer(me, peer, Some(id))?,
                    }
                }
            }
        }
        Ok(())
    }

    /// A replay (`Repair::Replay`): the peer's oplog from `floor`, below this member's
    /// position, with no `held`, window by window; a peer that cannot serve from
    /// there sends a scoped page instead.
    fn replay(&mut self, to: usize, from: usize, floor: Hlc, id: CollectionId) -> Res<()> {
        let src = self.e(from);
        if src.serve_verdict_from(floor).map_err(|e| err("serve_verdict_from", e))?
            != ServeVerdict::Serve
        {
            self.note(format!("replay: {} cannot serve from there, a scoped page", LETTERS[from]));
            return self.transfer(to, from, Some(id));
        }
        let mut start = floor;
        for _ in 0..32 {
            let src = self.e(from);
            let dst = self.e(to);
            let theirs = src.version_vector().map_err(|e| err("version_vector", e))?;
            let mine = dst.witnessed_vector().map_err(|e| err("witnessed_vector", e))?;
            let start_at = mine.behind(&theirs).map_or(start, |behind| behind.min(start));
            let window = src
                .entries_for_peer_holding(start_at, BATCH, None, WalkScope::Background)
                .map_err(|e| err("entries_for_peer", e))?;
            let mut outcome = SyncOutcome::default();
            dst.apply_peer_window_into(
                &theirs,
                Some(&mine),
                &window.entries,
                PeerWindow {
                    scanned_to: window.scanned_to,
                    exhausted: window.exhausted,
                    passed_through: None,
                    asked_partial: false,
                },
                &mut outcome,
            )
            .map_err(|e| err("apply_peer_window", e))?;
            if outcome.purge_pending_at.is_some() {
                dst.finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
                continue;
            }
            if let Some(unknown) = outcome.unknown {
                self.note(format!(
                    "replay: stopped at {}, a scoped page",
                    name_of(unknown.id).unwrap_or_default()
                ));
                return self.transfer(to, from, Some(unknown.id));
            }
            if window.exhausted {
                return Ok(());
            }
            start = window.scanned_to.successor();
        }
        Err("a replay that never ended".into())
    }

    fn observe(&self, m: usize) -> Res<(Observed, Recorded)> {
        let e = self.e(m);
        let mut observed = Observed::default();
        let mut recorded = Recorded::default();
        for db in e.list_databases().map_err(|e| err("list_databases", e))? {
            observed.databases.insert(db.name);
        }
        for meta in e.all_collections().map_err(|e| err("all_collections", e))? {
            let Some(name) = NAMES.iter().find(|n| meta.db == DB && meta.name == **n) else {
                continue;
            };
            let mut coll = Coll { floor: meta.incarnation_floor, ..Coll::default() };
            for index in &meta.indexes {
                coll.indexes.insert(index.name.clone(), index_definition(index));
                recorded.index_created.insert(format!("{name}.{}", index.name), index.created);
            }
            e.for_each_record_after(&meta, None, WalkScope::Background, |id, stamp, doc| {
                let DocId::Int64(n) = id else { panic!("an id the model never writes") };
                coll.documents.insert(n, (stamp, doc.to_string()));
                Ok(true)
            })
            .map_err(|e| err("for_each_record_after", e))?;
            recorded.created.insert(name, meta.created);
            if meta.is_redated()
                && let Some(marker) = meta.redated
            {
                recorded.redated_from.insert(name, marker.from);
            }
            observed.collections.insert(name, coll);
        }
        // The named collections' and the database's; a vector shadow's tombstone is
        // vector machinery the model does not exercise.
        for (id, stamp) in self.tombstones(m)? {
            if let Some(key) = name_of(id) {
                observed.tombstones.insert(key, stamp);
            }
        }
        Ok((observed, recorded))
    }

    fn describe(&self, observed: &Observed, recorded: &Recorded) -> String {
        let mut out = String::new();
        let _ = write!(out, "dbs {:?}", observed.databases);
        for (name, coll) in &observed.collections {
            let docs: Vec<String> = coll
                .documents
                .iter()
                .map(|(d, (s, _))| format!("{d}@{}", self.stamp(*s)))
                .collect();
            let indexes: Vec<String> = coll
                .indexes
                .iter()
                .map(|(n, def)| {
                    let dir = if def.contains("descending: true") { "desc" } else { "asc" };
                    format!("{n}:{dir}")
                })
                .collect();
            let _ = write!(
                out,
                " | {name}: created {} redated_from {} floor {} docs [{}] indexes [{}]",
                recorded.created.get(name).map_or("-".into(), |h| self.hlc(*h)),
                recorded.redated_from.get(name).map_or("-".into(), |h| self.hlc(*h)),
                coll.floor.map_or("-".into(), |h| self.hlc(h)),
                docs.join(" "),
                indexes.join(" ")
            );
        }
        let tombstones: Vec<String> =
            observed.tombstones.iter().map(|(k, s)| format!("{k}@{}", self.stamp(*s))).collect();
        let _ = write!(out, " | tombstones [{}]", tombstones.join(" "));
        out
    }

    /// Compare the three members, and check the ghost's writes.
    /// What two routes that both converged must agree on: member A's databases and
    /// collections, with their documents (unless an age-out let a delete be lost, the
    /// documented ADR-085 class), indexes and floors.
    fn settled(&self) -> Res<String> {
        let (observed, _) = self.observe(0)?;
        let mut out = format!("{:?}", observed.databases);
        for (name, coll) in &observed.collections {
            let docs = if self.aged_out {
                String::from("(not compared after an age-out)")
            } else {
                coll.documents
                    .iter()
                    .map(|(d, (s, _))| format!("{d}@{}", self.stamp(*s)))
                    .collect::<Vec<_>>()
                    .join(" ")
            };
            let _ = write!(
                out,
                " | {name}: floor {} docs [{docs}] indexes {:?}",
                coll.floor.map_or("-".into(), |h| self.hlc(h)),
                coll.indexes.keys().collect::<Vec<_>>()
            );
        }
        Ok(out)
    }

    fn compare(&self) -> Res<(Vec<Failure>, String, bool)> {
        let mut states = Vec::new();
        let mut report = String::new();
        for (m, letter) in LETTERS.iter().enumerate() {
            let (observed, recorded) = self.observe(m)?;
            let _ = writeln!(report, "  {letter}: {}", self.describe(&observed, &recorded));
            states.push((observed, recorded));
        }
        let created_differs = states[0].1.created != states[1].1.created
            || states[0].1.created != states[2].1.created;
        let mut failures = Vec::new();
        let mut fail = |category: Category, detail: String| {
            if !failures.iter().any(|f: &Failure| f.category == category && f.detail == detail) {
                failures.push(Failure { category, detail });
            }
        };
        let below_cutoff = |s: &Stamp| self.gc_cutoff.is_some_and(|cut| s.hlc <= cut);
        for m in 1..3 {
            let (a, b) = (&states[0].0, &states[m].0);
            let pair = format!("A/{}", LETTERS[m]);
            if a.databases != b.databases {
                fail(
                    Category::Databases,
                    format!("{pair}: {:?} vs {:?}", a.databases, b.databases),
                );
            }
            let names_a: BTreeSet<_> = a.collections.keys().collect();
            let names_b: BTreeSet<_> = b.collections.keys().collect();
            if names_a != names_b {
                fail(Category::Collections, format!("{pair}: {names_a:?} vs {names_b:?}"));
            }
            for (name, ca) in &a.collections {
                let Some(cb) = b.collections.get(name) else { continue };
                if ca.documents != cb.documents {
                    let show = |c: &Coll| -> Vec<String> {
                        c.documents
                            .iter()
                            .map(|(d, (s, _))| format!("{d}@{}", self.stamp(*s)))
                            .collect()
                    };
                    // After an age-out, a document whose last event is a delete may come
                    // back where the delete was collected (ADR-085): that class only.
                    let c = NAMES.iter().position(|n| n == name).expect("named");
                    let differing: BTreeSet<i64> = ca
                        .documents
                        .iter()
                        .chain(cb.documents.iter())
                        .filter(|(d, v)| {
                            ca.documents.get(d) != Some(v) || cb.documents.get(d) != Some(v)
                        })
                        .map(|(d, _)| *d)
                        .collect();
                    let resurrection_only = self.aged_out
                        && differing.iter().all(|d| self.ghost.last_event_is_a_delete(c, *d));
                    let category =
                        if resurrection_only { Category::DocsAfterAgeOut } else { Category::Docs };
                    fail(category, format!("{pair} {name}: {:?} vs {:?}", show(ca), show(cb)));
                }
                if ca.indexes != cb.indexes {
                    fail(
                        Category::Indexes,
                        format!(
                            "{pair} {name}: {:?} vs {:?}",
                            ca.indexes.keys().collect::<Vec<_>>(),
                            cb.indexes.keys().collect::<Vec<_>>()
                        ),
                    );
                }
                if ca.floor != cb.floor {
                    let h = |f: Option<Hlc>| f.map_or("-".into(), |h| self.hlc(h));
                    fail(
                        Category::Floor,
                        format!("{pair} {name}: {} vs {}", h(ca.floor), h(cb.floor)),
                    );
                }
            }
            let keys: BTreeSet<&String> = a.tombstones.keys().chain(b.tombstones.keys()).collect();
            for key in keys {
                let ta = a.tombstones.get(key).filter(|s| !below_cutoff(s));
                let tb = b.tombstones.get(key).filter(|s| !below_cutoff(s));
                if ta == tb {
                    continue;
                }
                let show = |t: Option<&Stamp>| t.map_or("-".into(), |s| self.stamp(*s));
                let detail = format!("{pair} {key}: {} vs {}", show(ta), show(tb));
                let newest = ta.max(tb).expect("one side holds it");
                // Subsumed: live on both with a floor at or above it, or absent on both
                // under a database tombstone at or above it (which refuses every
                // creation and document of the collection the tombstone would).
                let subsumed = |o: &Observed| match o.collections.get(key.as_str()) {
                    Some(c) => c.floor.is_some_and(|floor| floor >= newest.hlc),
                    None => o.tombstones.get("#database").is_some_and(|db| db.hlc >= newest.hlc),
                };
                let category = match (ta, tb) {
                    (Some(x), Some(y)) if x.hlc == y.hlc => Category::TombstoneNode,
                    _ if key == "#database" => Category::DbTombstone,
                    _ if subsumed(a) && subsumed(b) => Category::TombstoneSubsumed,
                    _ => Category::CollTombstone,
                };
                fail(category, detail);
            }
        }
        for behind in &self.clock_behind {
            fail(Category::ClockBehind, behind.clone());
        }
        // No lost write, from the ghost.
        let mut latest: BTreeMap<(usize, i64), (Stamp, Option<usize>)> = BTreeMap::new();
        for (i, w) in self.ghost.writes.iter().enumerate() {
            let slot = latest.entry((w.c, w.d)).or_insert((w.stamp, Some(i)));
            if w.stamp > slot.0 {
                *slot = (w.stamp, Some(i));
            }
        }
        for (c, d, stamp) in &self.ghost.deletes {
            let slot = latest.entry((*c, *d)).or_insert((*stamp, None));
            if *stamp > slot.0 {
                *slot = (*stamp, None);
            }
        }
        for ((c, d), (_, winner)) in latest {
            let Some(i) = winner else { continue };
            let w = &self.ghost.writes[i];
            let after_every_drop = self
                .ghost
                .drops
                .iter()
                .filter(|(dc, _)| *dc == c)
                .all(|(_, s)| w.stamp.hlc > s.hlc);
            // A database's drop keeps what is stamped after it as a collection's does
            // (ADR-236), whether or not the writer had heard of it.
            let after_every_db_drop = self.ghost.db_drops.iter().all(|s| w.stamp.hlc > s.hlc);
            if !(after_every_drop && after_every_db_drop) {
                continue;
            }
            for (m, (observed, _)) in states.iter().enumerate() {
                let held =
                    observed.collections.get(NAMES[c]).and_then(|coll| coll.documents.get(&d));
                if held.map(|(s, _)| *s) != Some(w.stamp) {
                    fail(
                        Category::LostWrite,
                        format!(
                            "{} lacks {}/{d} written by {} at {} (step {}); holds {:?}",
                            LETTERS[m],
                            NAMES[c],
                            LETTERS[w.by],
                            self.stamp(w.stamp),
                            w.step,
                            held.map(|(s, _)| self.stamp(*s))
                        ),
                    );
                }
            }
        }
        Ok((failures, report, created_differs))
    }
}

// -- Running a schedule ----------------------------------------------------------------------

/// The base the schedule's times are counted from: an hour ahead of the wall clock,
/// so no stamp a run mints comes from the wall clock.
fn base() -> u64 {
    (crate::physical_now_ms() / 1000 + 3_600) * 1000
}

/// Quiesce `cluster` by `route` and compare.
fn settle(cluster: &mut Cluster, route: Route) -> RouteOutcome {
    let mut failures = Vec::new();
    let mut report = String::new();
    let mut created_differs = false;
    let mut settled = None;
    let notes_from = cluster.notes.len();
    match cluster.quiesce(route) {
        Err(e) => {
            failures.push(Failure { category: Category::Error, detail: format!("quiescence: {e}") })
        }
        Ok(stable) => {
            if !stable {
                failures.push(Failure {
                    category: Category::NotQuiescent,
                    detail: format!("still moving after {QUIESCE_ROUNDS} rounds"),
                });
            }
            match cluster.compare() {
                Ok((found, r, differs)) => {
                    let converged = found.iter().all(|f| {
                        matches!(
                            f.category,
                            Category::TombstoneSubsumed | Category::DocsAfterAgeOut
                        )
                    });
                    failures.extend(found);
                    report = r;
                    created_differs = differs;
                    if stable && converged {
                        settled = cluster.settled().ok();
                    }
                }
                Err(e) => failures.push(Failure { category: Category::Error, detail: e }),
            }
        }
    }
    let mut full = String::new();
    let _ = writeln!(full, "  notes:");
    let shown =
        if route == Route::Repaired { &cluster.notes[notes_from..] } else { &cluster.notes[..] };
    for n in shown {
        let _ = writeln!(full, "  {n}");
    }
    full.push_str(&report);
    RouteOutcome {
        route,
        failures,
        report: full,
        fallbacks: cluster.fallbacks,
        created_differs,
        settled,
    }
}

/// Run `ops` on a fresh cluster and settle it by `route` (`Entries` also settles the
/// same cluster again with the repairs, as `Repaired`).
fn one_route(ops: &[Op], route: Route, base: u64) -> (Vec<RouteOutcome>, String) {
    let mut cluster = Cluster::new(base);
    if let Err(e) = cluster.run(ops) {
        let failed = |route| RouteOutcome {
            route,
            failures: vec![Failure { category: Category::Error, detail: e.clone() }],
            report: cluster.notes.join("\n"),
            fallbacks: 0,
            created_differs: false,
            settled: None,
        };
        let routes = if route == Route::Entries {
            vec![Route::Entries, Route::Repaired]
        } else {
            vec![route]
        };
        return (routes.into_iter().map(failed).collect(), String::new());
    }
    let pre = cluster.fingerprint().unwrap_or_else(|e| e);
    let mut outcomes = vec![settle(&mut cluster, route)];
    if route == Route::Entries {
        outcomes.push(settle(&mut cluster, Route::Repaired));
    }
    (outcomes, pre)
}

/// Run `ops` twice from the same templates, quiescing once by entries and once by
/// pages, and check that the two pre-states were the same.
pub(crate) fn check(ops: &[Op]) -> Vec<RouteOutcome> {
    check_routes(ops, &[Route::Entries, Route::Pages])
}

pub(crate) fn check_routes(ops: &[Op], routes: &[Route]) -> Vec<RouteOutcome> {
    let base = base();
    let mut outcomes = Vec::new();
    let mut first_pre: Option<String> = None;
    for route in routes {
        let (mut found, pre) = one_route(ops, *route, base);
        if let Some(first) = &first_pre {
            if *first != pre && !pre.is_empty() {
                found[0].failures.push(Failure {
                    category: Category::Nondeterministic,
                    detail: "the two runs of the schedule left different pre-states".into(),
                });
            }
        } else {
            first_pre = Some(pre);
        }
        outcomes.extend(found);
    }
    // Both routes converged: to the same state?
    let settled = |route| {
        outcomes.iter().find(|o: &&RouteOutcome| o.route == route).and_then(|o| o.settled.clone())
    };
    if let (Some(by_entries), Some(by_pages)) = (settled(Route::Repaired), settled(Route::Pages))
        && by_entries != by_pages
    {
        let pages = outcomes.iter_mut().find(|o| o.route == Route::Pages).expect("ran");
        pages.failures.push(Failure {
            category: Category::RouteDependent,
            detail: format!("by entries: {by_entries}\n    by pages:   {by_pages}"),
        });
    }
    outcomes
}

pub(crate) fn render(ops: &[Op], outcomes: &[RouteOutcome]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "schedule: {}", show(ops));
    let _ = writeln!(out, "as rust:  {}", as_rust(ops));
    for o in outcomes {
        let _ = writeln!(
            out,
            "route {:?}: {} failure(s), fallbacks {}, created differs: {}",
            o.route,
            o.failures.len(),
            o.fallbacks,
            o.created_differs
        );
        for f in &o.failures {
            let _ = writeln!(out, "  {:?}: {}", f.category, f.detail);
        }
        out.push_str(&o.report);
    }
    out
}

fn run_route(ops: &[Op], route: Route) -> Vec<RouteOutcome> {
    match route {
        Route::Repaired | Route::Entries => check_routes(ops, &[Route::Entries]),
        // A page route's finding may be the comparison with the entries route.
        Route::Pages => check_routes(ops, &[Route::Entries, Route::Pages]),
    }
}

fn route_signature(ops: &[Op], route: Route) -> BTreeSet<Category> {
    run_route(ops, route)
        .into_iter()
        .find(|o| o.route == route)
        .map(|o| o.signature())
        .unwrap_or_default()
}

/// Remove steps while the failure keeps its signature on `route`.
pub(crate) fn minimise(ops: &[Op], route: Route, want: &BTreeSet<Category>) -> Vec<Op> {
    shrink(ops, |candidate| {
        let got = route_signature(candidate, route);
        !got.is_empty() && want.iter().all(|c| got.contains(c))
    })
}

/// [`minimise`] for a category the signature leaves out.
fn minimise_on(ops: &[Op], route: Route, _: &BTreeSet<Category>, category: Category) -> Vec<Op> {
    shrink(ops, |candidate| {
        run_route(candidate, route)
            .iter()
            .any(|o| o.route == route && o.failures.iter().any(|f| f.category == category))
    })
}

fn shrink(ops: &[Op], still_fails: impl Fn(&[Op]) -> bool) -> Vec<Op> {
    let mut current = ops.to_vec();
    loop {
        let mut shrunk = false;
        let mut i = current.len();
        while i > 0 {
            i -= 1;
            let mut candidate = current.clone();
            candidate.remove(i);
            if still_fails(&candidate) {
                current = candidate;
                shrunk = true;
            }
        }
        if !shrunk {
            return current;
        }
    }
}

// -- Generation ------------------------------------------------------------------------------

/// SplitMix64: reproducible from a seed, no dependency.
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        Rng(seed)
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

    fn pair(&mut self) -> (u8, u8) {
        let to = self.member();
        let from = (to + 1 + self.below(2) as u8) % 3;
        (to, from)
    }
}

/// Which steps a track draws from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Track {
    /// Every step but aging out.
    Full,
    /// Every step, aging out included.
    AgedOut,
}

/// A schedule of `len` steps from `seed`. Each member's clock runs at a skew drawn
/// for the schedule (a multiple of 10 ms, as is the step, so ties are common), and a
/// member's skew can jump mid-schedule.
pub(crate) fn generate(seed: u64, len: usize, track: Track) -> Vec<Op> {
    let mut rng = Rng::new(seed);
    let skews = [-20i64, -10, 0, 0, 0, 10, 20];
    let mut skew: [i64; 3] = std::array::from_fn(|_| skews[rng.below(skews.len() as u64) as usize]);
    let mut ops = Vec::with_capacity(len + 1);
    // A quarter of the schedules run with the drop purger behind: a creation over a
    // dropped life's rows waits (ADR-189) until a `Purge` step or quiescence.
    let purger_off = rng.below(4) == 0;
    if purger_off {
        ops.push(Op { step: Step::PurgerOff, t: 0 });
    }
    let mut g: i64 = 100;
    let coll = |rng: &mut Rng| if rng.below(5) == 0 { ITEMS } else { ORDERS };
    for i in 0..len {
        g += 10;
        // A schedule opens with a creation, so its steps have something to act on.
        if i == 0 {
            let m = rng.member();
            ops.push(Op {
                step: Step::Create { m, c: ORDERS },
                t: (g + skew[m as usize]).max(1) as u32,
            });
            continue;
        }
        if rng.below(12) == 0 {
            let m = rng.below(3) as usize;
            skew[m] = skews[rng.below(skews.len() as u64) as usize];
        }
        let roll = rng.below(100);
        let step = match roll {
            0..=13 => Step::Create { m: rng.member(), c: coll(&mut rng) },
            14..=31 => {
                Step::Write { m: rng.member(), c: coll(&mut rng), d: 1 + rng.below(3) as u8 }
            }
            32..=36 => {
                Step::Delete { m: rng.member(), c: coll(&mut rng), d: 1 + rng.below(3) as u8 }
            }
            37..=41 => Step::Index { m: rng.member(), c: coll(&mut rng), ix: rng.below(3) as u8 },
            42..=43 => {
                Step::DropIndex { m: rng.member(), c: coll(&mut rng), ix: rng.below(3) as u8 }
            }
            44..=53 => Step::Drop { m: rng.member(), c: coll(&mut rng) },
            54..=57 => Step::DropDb { m: rng.member() },
            58..=71 => {
                let (to, from) = rng.pair();
                Step::Pull { to, from }
            }
            72..=75 => {
                let (to, from) = rng.pair();
                Step::PullShort { to, from }
            }
            76..=81 => {
                let (to, from) = rng.pair();
                Step::Scoped { to, from, c: coll(&mut rng) }
            }
            82..=89 => {
                let (to, from) = rng.pair();
                Step::Whole { to, from }
            }
            90..=93 if purger_off && rng.below(2) == 0 => Step::Purge { m: rng.member() },
            90..=93 => Step::Restart { m: rng.member() },
            _ => {
                if track == Track::AgedOut && rng.below(2) == 0 {
                    Step::AgeOut { m: rng.member() }
                } else {
                    Step::Gc { m: rng.member() }
                }
            }
        };
        let t = match step {
            Step::Create { m, .. }
            | Step::Write { m, .. }
            | Step::Delete { m, .. }
            | Step::Index { m, .. }
            | Step::DropIndex { m, .. }
            | Step::Drop { m, .. }
            | Step::DropDb { m } => (g + skew[m as usize]).max(1) as u32,
            _ => 0,
        };
        ops.push(Op { step, t });
    }
    ops
}

// -- Tests ---------------------------------------------------------------------------------

#[test]
fn smoke_templates_have_fixed_identities() {
    let cluster = Cluster::new(base());
    let ids: Vec<_> = (0..3).map(|m| cluster.e(m).node_id()).collect();
    assert!(ids[0] < ids[1] && ids[1] < ids[2], "{ids:?}");
    for m in 0..3 {
        assert!(cluster.e(m).version_vector().unwrap().iter().next().is_none());
    }
}

// -- Named rows ------------------------------------------------------------------------------

use Step::*;

fn create(m: u8, c: u8, t: u32) -> Op {
    op(Create { m, c }, t)
}
fn write(m: u8, c: u8, d: u8, t: u32) -> Op {
    op(Write { m, c, d }, t)
}
fn drop_coll(m: u8, c: u8, t: u32) -> Op {
    op(Drop { m, c }, t)
}
fn drop_db(m: u8, t: u32) -> Op {
    op(DropDb { m }, t)
}
fn pull(to: u8, from: u8) -> Op {
    op(Pull { to, from }, 0)
}
fn pull_short(to: u8, from: u8) -> Op {
    op(PullShort { to, from }, 0)
}
fn whole(to: u8, from: u8) -> Op {
    op(Whole { to, from }, 0)
}
fn scoped(to: u8, from: u8, c: u8) -> Op {
    op(Scoped { to, from, c }, 0)
}
fn age_out(m: u8) -> Op {
    op(AgeOut { m }, 0)
}

/// A delivery of every member's news to every other, in the order `order` names
/// the pairs, by `how`.
fn deliver(how: fn(u8, u8) -> Op, order: &[(u8, u8)]) -> Vec<Op> {
    order.iter().map(|(to, from)| how(*to, *from)).collect()
}

/// What every member must hold at the end of a named row, beside convergence: the
/// documents of each named collection, or `None` for a collection that is absent.
type Expect = &'static [(u8, Option<&'static [i64]>)];

/// Run a named row on both quiescence routes, after each delivery variant, and fail
/// with every schedule that did not converge (or did not end as `expect` says).
fn row(name: &str, pre: &[Op], variants: &[(&str, Vec<Op>)], expect: Expect) {
    row_on(name, pre, variants, expect, &[Route::Entries, Route::Pages]);
}

/// [`row`] on the routes named. A row whose premise is a tombstone collected before
/// every member applied it (ADR-235's own) runs on pages only: past retention the entries
/// route answers `BeyondHorizon` by design (ADR-085), and a peer takes a whole page.
fn row_on(name: &str, pre: &[Op], variants: &[(&str, Vec<Op>)], expect: Expect, routes: &[Route]) {
    if census_takes(name, pre, variants, expect) {
        return;
    }
    let mut failed = Vec::new();
    for (variant, delivery) in variants {
        let mut ops = pre.to_vec();
        ops.extend(delivery.iter().copied());
        let mut outcomes = check_routes(&ops, routes);
        for outcome in &mut outcomes {
            expected_end(&ops, outcome, expect);
        }
        if failing(&outcomes) {
            failed.push(format!("--- {name} / {variant}\n{}", render(&ops, &outcomes)));
        } else if outcomes.iter().any(|o| !o.signature().is_empty()) {
            println!(
                "--- {name} / {variant}: pure entries needed the repair loop\n{}",
                render(&ops, &outcomes)
            );
        }
    }
    assert!(failed.is_empty(), "{}", failed.join("\n"));
}

/// A schedule fails when the entries route as the loop runs it (with its repairs) or
/// the pages route fails; pure entries that the repair loop then converges are not a
/// failure of the route, only a note.
fn failing(outcomes: &[RouteOutcome]) -> bool {
    let strict =
        |o: &RouteOutcome| o.failures.iter().any(|f| f.category != Category::DocsAfterAgeOut);
    outcomes.iter().any(|o| o.route != Route::Entries && strict(o))
        || outcomes
            .iter()
            .any(|o| o.route == Route::Entries && o.signature().contains(&Category::Error))
}

/// Check `expect` against the route's report: every member's line names the documents
/// it holds per collection.
fn expected_end(ops: &[Op], outcome: &mut RouteOutcome, expect: Expect) {
    if expect.is_empty() {
        return;
    }
    // Re-run cheaply is not possible here; the report carries each member's state.
    for line in outcome
        .report
        .lines()
        .filter(|l| l.starts_with("  A:") || l.starts_with("  B:") || l.starts_with("  C:"))
    {
        for (c, docs) in expect {
            let name = NAMES[*c as usize];
            let section = line.split(" | ").find(|part| part.starts_with(&format!("{name}:")));
            match (docs, section) {
                (None, None) => {}
                (None, Some(_)) => outcome.failures.push(Failure {
                    category: Category::Collections,
                    detail: format!("expected {name} absent: {}", &line[..4]),
                }),
                (Some(_), None) => outcome.failures.push(Failure {
                    category: Category::Collections,
                    detail: format!("expected {name} present: {}", &line[..4]),
                }),
                (Some(want), Some(section)) => {
                    let held: Vec<i64> = section
                        .split("docs [")
                        .nth(1)
                        .and_then(|rest| rest.split(']').next())
                        .unwrap_or("")
                        .split_whitespace()
                        .filter_map(|d| d.split('@').next()?.parse().ok())
                        .collect();
                    if held != *want {
                        outcome.failures.push(Failure {
                            category: Category::Docs,
                            detail: format!(
                                "expected {name} to hold {want:?}{}: holds {held:?}",
                                &line[1..4]
                            ),
                        });
                    }
                }
            }
        }
    }
    let _ = ops;
}

/// Every pair, in two opposite orders.
const FORWARD: &[(u8, u8)] = &[(B, A), (A, B), (C, A), (C, B), (A, C), (B, C)];
const BACKWARD: &[(u8, u8)] = &[(A, B), (B, A), (C, B), (C, A), (B, C), (A, C)];

fn both_orders_both_routes() -> Vec<(&'static str, Vec<Op>)> {
    vec![
        ("forward by entries", deliver(pull, FORWARD)),
        ("backward by entries", deliver(pull, BACKWARD)),
        ("forward by whole pages", deliver(whole, FORWARD)),
        ("backward by whole pages", deliver(whole, BACKWARD)),
        ("forward by scoped pages", FORWARD.iter().map(|(t, f)| scoped(*t, *f, ORDERS)).collect()),
        (
            "backward by scoped pages",
            BACKWARD.iter().map(|(t, f)| scoped(*t, *f, ORDERS)).collect(),
        ),
    ]
}

/// The decision's own row: A creates at t136, writes, drops at t150; B, which saw
/// none of it, creates at t162 and writes.
#[test]
fn row_a_creates_drops_b_creates_after_by_a_collection_drop() {
    let pre = [
        create(A, ORDERS, 136),
        write(A, ORDERS, 1, 140),
        drop_coll(A, ORDERS, 150),
        create(B, ORDERS, 162),
        write(B, ORDERS, 2, 165),
    ];
    row("collection drop", &pre, &both_orders_both_routes(), &[(ORDERS, Some(&[2]))]);
}

#[test]
fn row_a_creates_drops_b_creates_after_by_a_database_drop() {
    let pre = [
        create(A, ORDERS, 136),
        write(A, ORDERS, 1, 140),
        drop_db(A, 150),
        create(B, ORDERS, 162),
        write(B, ORDERS, 2, 165),
    ];
    row("database drop", &pre, &both_orders_both_routes(), &[(ORDERS, Some(&[2]))]);
}

#[test]
fn row_three_creators_of_one_name() {
    let pre = [
        create(A, ORDERS, 136),
        create(B, ORDERS, 138),
        create(C, ORDERS, 140),
        write(A, ORDERS, 1, 142),
        write(B, ORDERS, 2, 144),
        write(C, ORDERS, 3, 146),
    ];
    row("three creators", &pre, &both_orders_both_routes(), &[(ORDERS, Some(&[1, 2, 3]))]);
}

#[test]
fn row_three_creators_of_one_name_in_one_millisecond() {
    let pre = [
        create(A, ORDERS, 136),
        create(B, ORDERS, 136),
        create(C, ORDERS, 136),
        write(A, ORDERS, 1, 137),
        write(B, ORDERS, 2, 137),
        write(C, ORDERS, 3, 137),
    ];
    row("three creators, one ms", &pre, &both_orders_both_routes(), &[(ORDERS, Some(&[1, 2, 3]))]);
}

#[test]
fn row_three_creators_with_a_drop_between() {
    let pre = [
        create(A, ORDERS, 136),
        write(A, ORDERS, 1, 140),
        drop_coll(A, ORDERS, 150),
        create(B, ORDERS, 162),
        write(B, ORDERS, 2, 165),
        create(C, ORDERS, 170),
        write(C, ORDERS, 3, 172),
    ];
    row(
        "three creators, drop between",
        &pre,
        &both_orders_both_routes(),
        &[(ORDERS, Some(&[2, 3]))],
    );
}

/// ADR-231's re-dated life beside a concurrent create (review v3 M2): C holds A's life
/// and writes after A's drop without having seen it; B creates the name after the drop.
/// C learns the create first in one variant and the drop first in the other.
#[test]
fn row_a_re_dated_life_and_a_concurrent_create() {
    let pre = [
        create(A, ORDERS, 100),
        write(A, ORDERS, 1, 105),
        pull(C, A),
        drop_coll(A, ORDERS, 150),
        write(C, ORDERS, 5, 155),
        create(B, ORDERS, 162),
        write(B, ORDERS, 2, 165),
    ];
    let variants = vec![
        (
            "C learns the create first",
            vec![pull(C, B), pull(C, A), pull(B, C), pull(A, C), pull(B, A), pull(A, B)],
        ),
        (
            "C learns the drop first",
            vec![pull(C, A), pull(C, B), pull(B, A), pull(B, C), pull(A, B), pull(A, C)],
        ),
        (
            "C learns the create first, by pages",
            vec![whole(C, B), whole(C, A), whole(B, C), whole(A, C), whole(B, A), whole(A, B)],
        ),
        (
            "C learns the drop first, by pages",
            vec![whole(C, A), whole(C, B), whole(B, A), whole(B, C), whole(A, B), whole(A, C)],
        ),
        (
            "C by scoped pages",
            vec![
                scoped(C, B, ORDERS),
                scoped(C, A, ORDERS),
                scoped(B, C, ORDERS),
                scoped(A, C, ORDERS),
            ],
        ),
    ];
    row("re-dated + concurrent create", &pre, &variants, &[(ORDERS, Some(&[2, 5]))]);
}

/// PR #193 review A, minor 1: a re-dated sender whose tombstone aged out. B keeps a
/// write made after A's drop, so its life is re-dated; A and B both age out (the
/// operator rule broken, ADR-235's own premise); C still holds the life before, with a
/// straggler of its own stamped before the drop, and takes a scoped page from B.
#[test]
fn row_a_re_dated_sender_whose_tombstone_aged_out() {
    let pre = [
        create(A, ORDERS, 100),
        write(A, ORDERS, 1, 105),
        pull(B, A),
        pull(C, A),
        write(C, ORDERS, 4, 148),
        drop_coll(A, ORDERS, 150),
        write(B, ORDERS, 3, 155),
        pull(B, A),
        create(A, ITEMS, 160),
        write(A, ITEMS, 1, 161),
        age_out(A),
        age_out(B),
    ];
    let variants = vec![
        ("C takes a scoped page from B", vec![scoped(C, B, ORDERS)]),
        ("C takes a whole page from B", vec![whole(C, B)]),
        ("C pulls from B", vec![pull(C, B)]),
    ];
    row_on("re-dated sender, aged out", &pre, &variants, &[(ORDERS, Some(&[3]))], &[Route::Pages]);
}

/// Two second lives after one drop: every member saw the drop, then B and C each make
/// a life of the name.
#[test]
fn row_two_second_lives_after_one_drop() {
    let pre = [
        create(A, ORDERS, 100),
        write(A, ORDERS, 1, 105),
        pull(B, A),
        pull(C, A),
        drop_coll(A, ORDERS, 150),
        pull(B, A),
        pull(C, A),
        create(B, ORDERS, 160),
        write(B, ORDERS, 2, 161),
        create(C, ORDERS, 165),
        write(C, ORDERS, 3, 166),
    ];
    row("two second lives", &pre, &both_orders_both_routes(), &[(ORDERS, Some(&[2, 3]))]);
}

/// Two second lives after one drop that only one of them saw (the other made its life
/// with no floor).
#[test]
fn row_two_second_lives_one_without_the_drop() {
    let pre = [
        create(A, ORDERS, 100),
        write(A, ORDERS, 1, 105),
        pull(B, A),
        drop_coll(A, ORDERS, 150),
        pull(B, A),
        create(B, ORDERS, 160),
        write(B, ORDERS, 2, 161),
        create(C, ORDERS, 165),
        write(C, ORDERS, 3, 166),
    ];
    row(
        "two second lives, one blind",
        &pre,
        &both_orders_both_routes(),
        &[(ORDERS, Some(&[2, 3]))],
    );
}

/// Review v3 M1 without the siblings: B holds its own life (162) and took a cut window
/// of A's that ends after A's create and write (so not the drop); C applied A's whole
/// window (the tombstone, no collection) and takes B's whole-database page.
#[test]
fn row_review_m1_a_cut_window_then_a_whole_page_to_a_member_holding_the_drop() {
    let pre = [
        create(A, ORDERS, 136),
        write(A, ORDERS, 1, 140),
        drop_coll(A, ORDERS, 150),
        create(B, ORDERS, 162),
        write(B, ORDERS, 2, 165),
        pull_short(B, A),
        pull(C, A),
        whole(C, B),
    ];
    let variants = vec![
        ("then entries", deliver(pull, FORWARD)),
        ("then pages", deliver(whole, FORWARD)),
        ("nothing more", vec![]),
    ];
    row("review M1", &pre, &variants, &[(ORDERS, Some(&[2]))]);
}

/// Review v3 m3 without the siblings: a creation a page restores and its receiver's
/// clock does not follow (the sender's own coverage does not name it), then a local
/// drop on the receiver.
#[test]
fn row_review_m3_a_local_drop_below_an_unwitnessed_creation() {
    let pre = [
        create(A, ORDERS, 300),
        write(A, ORDERS, 1, 301),
        scoped(B, A, ORDERS),
        scoped(C, B, ORDERS),
        drop_coll(C, ORDERS, 110),
    ];
    let variants =
        vec![("then entries", deliver(pull, FORWARD)), ("then pages", deliver(whole, FORWARD))];
    row("review m3", &pre, &variants, &[(ORDERS, None)]);
}

/// The ADR-235 case itself, through the model: the receiver holds life 1, the sender
/// dropped it and made life 2, and the sender's tombstone aged out (its life 2 held no
/// record yet, so retention could take it).
#[test]
fn row_the_adr_235_case_with_the_tombstone_aged_out() {
    let pre = parse(
        "A.create(orders)@100; A.write(orders/1)@105; B<-A entries; A.drop(orders)@150; \
         A.create(orders)@160; A.ageout; A.write(orders/2)@170",
    );
    let variants = vec![
        ("B takes a whole page", parse("B<-A whole")),
        ("B takes a scoped page", parse("B<-A scoped(orders)")),
        ("B pulls", parse("B<-A entries")),
    ];
    row_on("ADR-235", &pre, &variants, &[(ORDERS, Some(&[2]))], &[Route::Pages]);
}

/// PR #193 review B, pre-existing signature 1: a write made after a drop by a member that
/// had not heard of it reaches an entries-only third member in the same window as the
/// drop and before the recreation, finds the collection absent under a tombstone, and is
/// set aside as history (`judge_document`); the recreation then lands without it. Pure
/// entries leave it so; the count half's replay from `min(floor, created)` (ADR-231
/// item 7) brings it back, and pages never lose it.
#[test]
fn row_pr193b_a_write_after_a_drop_set_aside_before_the_recreation() {
    let pre = parse(
        "A.create(orders)@100; A.write(orders/1)@105; B<-A entries; C<-A entries; \
         A.drop(orders)@110; B.write(orders/5)@115; A.create(orders)@120; \
         A.write(orders/7)@125; A<-B entries; C<-A entries",
    );
    row("pr193b signature 1", &pre, &[("as found", vec![])], &[(ORDERS, Some(&[5, 7]))]);
}

/// PR #193 review B, pre-existing signature 2: a member pulls a drop and the
/// recreation in one window, then a bystander's entries, before its purger has run. The
/// window ends before the creation (ADR-189), nothing past it is witnessed, and the next
/// round brings the creation: the collection is late, not lost.
#[test]
fn row_pr193b_a_recreation_that_waits_for_the_purger() {
    let variants = vec![
        (
            "R by entries",
            parse(
                "A.create(orders)@100; A.write(orders/1)@105; B<-A entries; B.write(orders/2)@110; \
                 A.drop(orders)@115; A.create(orders)@120; A.write(orders/3)@125; purger off; \
                 C<-A entries; C<-B entries; C.purge",
            ),
        ),
        (
            "R by a whole page",
            parse(
                "A.create(orders)@100; A.write(orders/1)@105; B<-A whole; B.write(orders/2)@110; \
                 A.drop(orders)@115; A.create(orders)@120; A.write(orders/3)@125; purger off; \
                 C<-A entries; C<-B entries; C.purge",
            ),
        ),
    ];
    row("pr193b signature 2", &[], &variants, &[(ORDERS, Some(&[3]))]);
}

/// ADR-233's witness of a page's `created`: a member whose clock trails a creation it
/// restores by a page that vouches for it. Without the witness its clock stays at its own
/// time (here 110, the creation at 300) and its next write into the life is stamped
/// below the life (the report's vacuity gap: every other schedule moved the clock past
/// the creation by something else first, a document or a local step).
#[test]
fn row_a_page_moves_the_clock_past_the_creation_it_vouches_for() {
    let pre = parse("C.write(items/1)@110; A.create(orders)@300");
    let variants = vec![
        ("by a scoped page", parse("C<-A scoped(orders); C.write(orders/1)@120")),
        ("by a whole page", parse("C<-A whole; C.write(orders/1)@120")),
    ];
    row("clock past a vouched creation", &pre, &variants, &[(ORDERS, Some(&[1]))]);
}

/// ADR-233's witness of a carried drop: a whole page carrying a tombstone its sender's
/// coverage names, to a member whose clock trails it. Without the witness the member's
/// next local creation of the name is minted below the floor it takes from that
/// tombstone, and its creation and writes are history everywhere else.
#[test]
fn row_a_page_moves_the_clock_past_the_drop_it_vouches_for() {
    let pre = parse("C.write(items/1)@110; A.create(orders)@300; A.drop(orders)@310");
    let variants = vec![
        ("by a whole page", parse("C<-A whole; C.create(orders)@200; C.write(orders/1)@205")),
        ("by a scoped page", parse("C<-A scoped(orders); C.create(orders)@200")),
    ];
    row("clock past a vouched drop", &pre, &variants, &[]);
}

// -- The census a vacuity check compares -------------------------------------------------

/// A named schedule, with what its row expects at the end.
type Named = (String, Vec<Op>, Expect);

thread_local! {
    static CENSUS: std::cell::RefCell<Option<Vec<Named>>> =
        const { std::cell::RefCell::new(None) };
}

/// In a census, a row records its schedules instead of running them.
fn census_takes(name: &str, pre: &[Op], variants: &[(&str, Vec<Op>)], expect: Expect) -> bool {
    CENSUS.with(|c| match c.borrow_mut().as_mut() {
        Some(list) => {
            for (variant, delivery) in variants {
                let mut ops = pre.to_vec();
                ops.extend(delivery.iter().copied());
                list.push((format!("{name} / {variant}"), ops, expect));
            }
            true
        }
        None => false,
    })
}

/// Every named row's schedules, every finding's, generated seeds of both tracks and the
/// exhaustive depth: each schedule's strict signature on each route, one line each, into
/// `CONV_CENSUS`. A vacuity check runs it on the code as it is and on the code broken,
/// and diffs the two.
#[test]
#[ignore = "the vacuity census; see the convergence model test report"]
fn vacuity_census() {
    CENSUS.with(|c| *c.borrow_mut() = Some(Vec::new()));
    let named: [fn(); 28] = [
        finding_f11_a_local_creation_below_the_floor_it_takes_is_history_everywhere,
        row_a_page_moves_the_clock_past_the_creation_it_vouches_for,
        row_a_page_moves_the_clock_past_the_drop_it_vouches_for,
        finding_f10_a_local_drop_below_an_unwitnessed_creation_is_undone,
        row_pr193b_a_write_after_a_drop_set_aside_before_the_recreation,
        row_pr193b_a_recreation_that_waits_for_the_purger,
        row_a_creates_drops_b_creates_after_by_a_collection_drop,
        row_a_creates_drops_b_creates_after_by_a_database_drop,
        row_three_creators_of_one_name,
        row_three_creators_of_one_name_in_one_millisecond,
        row_three_creators_with_a_drop_between,
        row_a_re_dated_life_and_a_concurrent_create,
        row_a_re_dated_sender_whose_tombstone_aged_out,
        row_two_second_lives_after_one_drop,
        row_two_second_lives_one_without_the_drop,
        row_review_m1_a_cut_window_then_a_whole_page_to_a_member_holding_the_drop,
        row_review_m3_a_local_drop_below_an_unwitnessed_creation,
        row_the_adr_235_case_with_the_tombstone_aged_out,
        finding_f1_an_ignored_drop_from_a_page_records_no_tombstone,
        finding_f3_a_drop_and_a_creation_in_one_hlc,
        finding_f4_an_ignored_drop_is_not_relayed_and_the_relay_advertises_past_it,
        finding_f5_a_survivors_tombstone_is_recorded_on_one_path_only,
        finding_f6_a_definition_of_a_life_the_database_drop_buried_survives_in_a_newer_life,
        finding_f7_an_index_of_a_newer_life_that_arrives_before_the_life_is_lost,
        finding_f8_a_life_installed_from_a_page_takes_no_floor,
        row_a_relay_without_the_floor_after_the_drop_aged_out,
        finding_f9_a_burial_from_a_page_names_the_sender_as_the_drops_maker,
        the_enumeration_counts,
    ];
    for f in named {
        f();
    }
    let mut all: Vec<Named> = CENSUS.with(|c| c.borrow_mut().take()).expect("collected");
    let seeds = env_u64("CONV_SEEDS", 3000);
    for seed in 0..seeds {
        let len = 8 + (seed % 5) as usize;
        all.push((format!("seed {seed} Full"), generate(seed, len, Track::Full), &[]));
        if seed % 2 == 0 {
            all.push((format!("seed {seed} AgedOut"), generate(seed, len, Track::AgedOut), &[]));
        }
    }
    let alphabet: Vec<Step> = vec![
        Create { m: A, c: ORDERS },
        Create { m: B, c: ORDERS },
        Write { m: A, c: ORDERS, d: 1 },
        Write { m: B, c: ORDERS, d: 2 },
        Drop { m: A, c: ORDERS },
        DropDb { m: B },
        Pull { to: B, from: A },
        Pull { to: A, from: B },
        PullShort { to: B, from: A },
        Whole { to: C, from: B },
        Scoped { to: C, from: A, c: ORDERS },
        Restart { m: B },
    ];
    for (i, ops) in enumerate(&alphabet, env_u64("CONV_DEPTH", 4) as usize).into_iter().enumerate()
    {
        all.push((format!("exhaustive #{i}"), ops, &[]));
    }
    let threads =
        env_u64("CONV_THREADS", std::thread::available_parallelism().map_or(4, |n| n.get() as u64))
            as usize;
    let next = std::sync::atomic::AtomicUsize::new(0);
    let lines: Vec<std::sync::Mutex<Vec<String>>> =
        all.iter().map(|_| std::sync::Mutex::new(Vec::new())).collect();
    std::thread::scope(|scope| {
        for _ in 0..threads.max(1) {
            scope.spawn(|| {
                loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some((id, ops, expect)) = all.get(i) else { break };
                    let mut outcomes = check(ops);
                    for outcome in &mut outcomes {
                        expected_end(ops, outcome, expect);
                        let strict: BTreeSet<Category> = outcome
                            .failures
                            .iter()
                            .map(|f| f.category)
                            .filter(|c| *c != Category::DocsAfterAgeOut)
                            .collect();
                        lines[i]
                            .lock()
                            .unwrap()
                            .push(format!("{id}\t{:?}\t{:?}", outcome.route, strict));
                    }
                }
            });
        }
    });
    let mut out: Vec<String> = lines.into_iter().flat_map(|l| l.into_inner().unwrap()).collect();
    out.sort();
    let failing = out.iter().filter(|l| !l.ends_with("{}")).count();
    let text = out.join("\n");
    println!("census: {} schedules, {} route runs, {failing} failing", all.len(), out.len());
    if let Ok(path) = std::env::var("CONV_CENSUS") {
        std::fs::write(path, text + "\n").unwrap();
    }
}

// -- Findings, one minimal schedule each (see the report) ---------------------------------

/// A finding's schedule, on every route: it must converge (and end as `expect` says).
fn finding(name: &str, schedule: &str, expect: Expect) {
    row(name, &parse(schedule), &[("as found", vec![])], expect);
}

/// F1. A page's drop that the receiver ignores (its life is newer) records no tombstone,
/// where the same drop by entries does.
#[test]
fn finding_f1_an_ignored_drop_from_a_page_records_no_tombstone() {
    finding(
        "F1",
        "B.create(orders)@100; B.drop(orders)@130; C.create(orders)@200; C<-B whole",
        &[(ORDERS, Some(&[]))],
    );
}

/// F3. A drop and another member's creation of the name in one `Hlc`: the creation's
/// stamp sorts above the drop's, so a member that has the drop first created the life;
/// the member that made the creation took the drop as aimed at its life (`Hlc`, not
/// `Stamp`) and buried it, and a write into the life then never reached that member.
/// Ruled (ADR-236): a tie in one `Hlc` is the drop's on every route, so the creation is
/// history everywhere, nobody holds the life, and the write finds no collection.
#[test]
fn finding_f3_a_drop_and_a_creation_in_one_hlc() {
    finding(
        "F3",
        "B.create(orders)@110; C.create(orders)@190; B.drop(orders)@190; A<-B entries; \
         A<-C entries; A.write(orders/1)@300",
        &[(ORDERS, None)],
    );
}

/// F4. A drop a member ignores (its life is newer) is not appended to its oplog, and the
/// member then relays the dropper's later entries: a peer that pulls the relay takes its
/// vector and is never served the drop by anyone. Here the peer made its own life after
/// the drop, so the count repair's replay from `min(floor, created)` starts above the drop
/// too, and the document of the life the drop ended stays on it for good.
#[test]
fn finding_f4_an_ignored_drop_is_not_relayed_and_the_relay_advertises_past_it() {
    finding(
        "F4",
        "C.create(orders)@110; C.write(orders/1)@140; C.drop(orders)@150; \
         C.create(orders)@170; B.create(orders)@190",
        &[(ORDERS, Some(&[]))],
    );
}

/// F5. A database's drop gives the collection it spares a tombstone at its stamp; a
/// member that creates the same life after holding the drop floors it and records none.
#[test]
fn finding_f5_a_survivors_tombstone_is_recorded_on_one_path_only() {
    finding(
        "F5",
        "C.create(orders)@90; C.dropdb@150; A.create(items)@220",
        &[(ITEMS, Some(&[])), (ORDERS, None)],
    );
}

/// F6. A definition made in a life its database's drop covers (created before the drop,
/// the index after it) is buried with that life where the life stood, and kept by a
/// member that already held a newer life of the name, which takes it as written after the
/// drop. A document made the same way converges only because the count repair replays it
/// back; nothing replays a definition.
#[test]
fn finding_f6_a_definition_of_a_life_the_database_drop_buried_survives_in_a_newer_life() {
    let variants = vec![
        // An index alone: nothing makes the count repair replay it.
        (
            "an index",
            parse(
                "A.create(orders)@110; C<-A scoped(orders); A.dropdb@140; \
                 C.index(orders,k_1:asc)@170; A.create(orders)@175",
            ),
        ),
        // Two members write one document, the later stamp into the life the database's
        // drop covers: its writer buries it with that life and then takes the earlier
        // write; the others keep the later one. The counts agree, so nothing repairs it.
        (
            "two writes of one document",
            parse(
                "A.create(orders)@110; A.write(orders/3)@120; A.dropdb@130; B.create(orders)@160; \
                 C<-A entries(cut at 2); B.write(orders/3)@210; C.write(orders/3)@210",
            ),
        ),
        // With a document: the entries route replays both back, the pages route buries
        // both.
        (
            "an index and a document",
            parse(
                "B.create(orders)@110; A<-B entries; A.dropdb@160; B.index(orders,k_1:asc)@190; \
                 B.write(orders/1)@191; C.create(orders)@220",
            ),
        ),
    ];
    row("F6", &[], &variants, &[]);
}

/// F7. A `CreateIndex` for a collection the receiver dropped, stamped after the drop (so
/// of a newer life whose creation the receiver has not had yet), is skipped as gone and
/// witnessed: the index never reaches that member.
#[test]
fn finding_f7_an_index_of_a_newer_life_that_arrives_before_the_life_is_lost() {
    finding(
        "F7",
        "A.create(orders)@110; A.drop(orders)@130; C<-A entries; A.create(orders)@160; \
         B<-A scoped(orders); B.index(orders,j_1:asc)@200; C<-B entries",
        &[(ORDERS, Some(&[]))],
    );
}

/// F8 (#193 MAJOR-1, being fixed). A member that installs a life from a page takes no
/// incarnation floor from it:
/// ADR-235 puts the sender's floor on the page and only a standing life's burial reads
/// it. With F1, a whole page from a member whose life ignored the drop carries neither
/// the tombstone nor a floor the receiver keeps, and its coverage hides the drop's entry.
#[test]
fn finding_f8_a_life_installed_from_a_page_takes_no_floor() {
    finding(
        "F8",
        "B.create(orders)@100; B.drop(orders)@110; A.create(orders)@190; A<-B whole",
        &[(ORDERS, Some(&[]))],
    );
}

/// The aged-out shape of F8: the drop's maker aged out its tombstone, a relay took the
/// new life by a page (no floor), and a member holding the life before takes the relay's
/// page. Past retention by design (ADR-085), so pages only: there, the maker's own page
/// still carries the floor and ends the life before everywhere.
#[test]
fn row_a_relay_without_the_floor_after_the_drop_aged_out() {
    let pre = parse(
        "A.create(orders)@100; A.write(orders/1)@105; B<-A entries; B.write(orders/3)@140; \
         A.drop(orders)@150; A.create(orders)@160; A.ageout; C<-A whole; B<-C whole; \
         C.write(orders/2)@170",
    );
    row_on(
        "aged-out relay",
        &pre,
        &[("as found", vec![])],
        &[(ORDERS, Some(&[2]))],
        &[Route::Pages],
    );
}

/// F9. The tombstone ADR-235 records for the drop a page's floor stands for is stamped
/// with the page's sender, not the drop's maker; when the real drop never arrives (its
/// coverage came with a page), the two members name one drop by two stamps (PR #193
/// review A, minor 2).
#[test]
fn finding_f9_a_burial_from_a_page_names_the_sender_as_the_drops_maker() {
    finding(
        "F9",
        "C.create(orders)@110; A<-C entries; C.drop(orders)@140; B.create(orders)@150; B<-C whole",
        &[(ORDERS, Some(&[]))],
    );
}

/// F10. A local drop is minted at the member's clock, not above the life it buries: a
/// creation a page stored and did not witness (the sender's own coverage does not name it,
/// ADR-233's deliberate trade) leaves the clock below it, the drop is stamped before the
/// creation, every other member takes it as aimed at a life already over, and the
/// acknowledged drop is undone everywhere, the dropping member included.
#[test]
fn finding_f10_a_local_drop_below_an_unwitnessed_creation_is_undone() {
    finding(
        "F10",
        "A.create(orders)@300; B<-A scoped(orders); C<-B scoped(orders); C.drop(orders)@110",
        &[(ORDERS, None)],
    );
}

/// F11 (found while closing the vacuity gap of ADR-233's witnesses). A scoped page from a
/// relay carries the drop of the collection it no longer holds, a stamp the relay's own
/// coverage does not name: recorded, not witnessed. A local creation of the name then took
/// its floor from that tombstone and was minted at the member's clock, below it: the
/// creation and every write into the life were history on every other member.
#[test]
fn finding_f11_a_local_creation_below_the_floor_it_takes_is_history_everywhere() {
    finding(
        "F11",
        "A.create(orders)@300; A.drop(orders)@310; B<-A scoped(orders); C<-B scoped(orders); \
         C.create(orders)@200; C.write(orders/1)@205",
        &[(ORDERS, Some(&[1]))],
    );
}

// -- Generated schedules -------------------------------------------------------------------

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Whether a schedule collects garbage.
fn collects(ops: &[Op]) -> bool {
    ops.iter().any(|o| matches!(o.step, Step::Gc { .. } | Step::AgeOut { .. }))
}

/// Whether the schedule without its collection steps still fails on `route` with at
/// least `signature`: if not, the divergence is the collection's, a tombstone collected
/// before every member applied it (ADR-085's resurrection case, by design).
fn reproduces_without_collection(ops: &[Op], route: Route, signature: &[Category]) -> bool {
    let kept: Vec<Op> = ops
        .iter()
        .filter(|o| !matches!(o.step, Step::Gc { .. } | Step::AgeOut { .. }))
        .copied()
        .collect();
    let got = route_signature(&kept, route);
    signature.iter().all(|c| got.contains(c))
}

/// Failing schedules by route and signature.
type Groups = BTreeMap<(Route, Vec<Category>), Vec<(String, Vec<Op>)>>;

/// Run `schedules`, group the failures by route and signature, minimise the first of
/// each group, and fail with the summary.
fn sweep(label: &str, schedules: impl Iterator<Item = (String, Vec<Op>)>) {
    let started = std::time::Instant::now();
    let mut groups: Groups = BTreeMap::new();
    let mut needed_repair: BTreeMap<Vec<Category>, usize> = BTreeMap::new();
    let mut ran = 0usize;
    let mut created_differs = 0usize;
    let mut fallbacks = 0usize;
    let mut aged = 0usize;
    let mut subsumed: BTreeMap<Route, usize> = BTreeMap::new();
    let mut by_design: BTreeMap<(Route, Vec<Category>), usize> = BTreeMap::new();
    let mut subsumed_example: Option<(Route, String, Vec<Op>)> = None;
    let schedules: Vec<(String, Vec<Op>)> = schedules.collect();
    let threads =
        env_u64("CONV_THREADS", std::thread::available_parallelism().map_or(4, |n| n.get() as u64))
            as usize;
    let next = std::sync::atomic::AtomicUsize::new(0);
    let results: Vec<std::sync::Mutex<Option<Vec<RouteOutcome>>>> =
        schedules.iter().map(|_| std::sync::Mutex::new(None)).collect();
    std::thread::scope(|scope| {
        for _ in 0..threads.max(1) {
            scope.spawn(|| {
                loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some((_, ops)) = schedules.get(i) else { break };
                    *results[i].lock().unwrap() = Some(check(ops));
                }
            });
        }
    });
    for ((id, ops), result) in schedules.iter().zip(results) {
        let (id, ops) = (id.clone(), ops.clone());
        ran += 1;
        aged += usize::from(ops.iter().any(|o| matches!(o.step, Step::AgeOut { .. })));
        let outcomes = result.into_inner().unwrap().expect("ran");
        let repaired_ok =
            outcomes.iter().any(|o| o.route == Route::Repaired && o.signature().is_empty());
        for outcome in &outcomes {
            created_differs +=
                usize::from(outcome.route == Route::Pages && outcome.created_differs);
            fallbacks += usize::from(outcome.route == Route::Entries && outcome.fallbacks > 0);
            if outcome.route != Route::Entries
                && outcome.failures.iter().any(|f| f.category == Category::TombstoneSubsumed)
            {
                *subsumed.entry(outcome.route).or_default() += 1;
                if subsumed_example.is_none() {
                    subsumed_example = Some((outcome.route, id.clone(), ops.clone()));
                }
            }
            let signature: Vec<Category> = outcome.signature().into_iter().collect();
            if signature.is_empty() {
                continue;
            }
            if outcome.route == Route::Entries
                && repaired_ok
                && !signature.contains(&Category::Error)
            {
                *needed_repair.entry(signature).or_default() += 1;
                continue;
            }
            if collects(&ops) && !reproduces_without_collection(&ops, outcome.route, &signature) {
                *by_design.entry((outcome.route, signature)).or_default() += 1;
                continue;
            }
            groups.entry((outcome.route, signature)).or_default().push((id.clone(), ops.clone()));
        }
    }
    let elapsed = started.elapsed();
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{label}: {ran} schedules ({aged} with an age-out), each settled by entries, by entries \
         with the repair loop, and by pages, in {elapsed:?}; {} failing groups; `created` \
         differed at the end (pages route) in {created_differs}; the pure entries route needed a \
         page in {fallbacks}; pure entries left divergence the repair loop then converged in: \
         {needed_repair:?}; a subsumed tombstone difference in {subsumed:?}; a divergence \
         that needs a collection step (gone without it: past retention, by design under \
         ADR-085) in {by_design:?}",
        groups.len()
    );
    if let Some((route, id, ops)) = &subsumed_example {
        let want: BTreeSet<Category> = BTreeSet::new();
        let small = minimise_on(ops, *route, &want, Category::TombstoneSubsumed);
        let _ = writeln!(
            out,
            "== subsumed tombstone example, {id}, minimised to {} steps",
            small.len()
        );
        out.push_str(&render(&small, &run_route(&small, *route)));
    }
    let minimise_cap = env_u64("CONV_MINIMISE", 3) as usize;
    for ((route, signature), members) in &groups {
        let _ = writeln!(
            out,
            "== {route:?} {signature:?}: {} schedule(s), first {}",
            members.len(),
            members[0].0
        );
        let mut seen = BTreeSet::new();
        for (id, ops) in members.iter().take(minimise_cap) {
            let want: BTreeSet<Category> = signature.iter().copied().collect();
            let small = minimise(ops, *route, &want);
            if !seen.insert(show(&small)) {
                let _ = writeln!(out, "-- {id}: minimises to the same schedule as above");
                continue;
            }
            let outcomes = run_route(&small, *route);
            let _ = writeln!(out, "-- {id}, minimised from {} to {} steps", ops.len(), small.len());
            out.push_str(&render(&small, &outcomes));
        }
    }
    println!("{out}");
    if let Ok(path) = std::env::var("CONV_REPORT") {
        std::fs::write(path, &out).unwrap();
    }
    assert!(groups.is_empty(), "{out}");
}

/// The seeded generator: `CONV_SEEDS` schedules (default 200) from `CONV_SEED_START`,
/// each 8 to 12 steps.
#[test]
#[ignore = "the model's long run; see the convergence model test report"]
fn generated_schedules() {
    let start = env_u64("CONV_SEED_START", 0);
    let count = env_u64("CONV_SEEDS", 200);
    let track = if std::env::var("CONV_TRACK").is_ok_and(|t| t == "aged") {
        Track::AgedOut
    } else {
        Track::Full
    };
    sweep(
        &format!("generated {track:?}"),
        (start..start + count).map(move |seed| {
            let len = 8 + (seed % 5) as usize;
            (format!("seed {seed} len {len} {track:?}"), generate(seed, len, track))
        }),
    );
}

/// One seed, printed in full: `CONV_SEED=n`.
#[test]
#[ignore = "reproduces one generated schedule"]
fn one_seed() {
    let seed = env_u64("CONV_SEED", 0);
    let len = 8 + (seed % 5) as usize;
    let track = if std::env::var("CONV_TRACK").is_ok_and(|t| t == "aged") {
        Track::AgedOut
    } else {
        Track::Full
    };
    let ops = generate(seed, len, track);
    println!("{}", render(&ops, &check(&ops)));
}

/// One schedule, printed in full: `CONV_SCHEDULE="A.create(orders)@100; B<-A entries"`.
#[test]
#[ignore = "reproduces one schedule"]
fn one_schedule() {
    let ops = parse(&std::env::var("CONV_SCHEDULE").expect("CONV_SCHEDULE"));
    assert_eq!(parse(&show(&ops)), ops, "the printed form reads back");
    println!("{}", render(&ops, &check(&ops)));
}

/// The reduced alphabet, every schedule up to `CONV_DEPTH` steps (default 4), with
/// the steps that cannot do anything pruned (a write or a drop on a member that cannot
/// hold the collection yet, a delivery from a member that knows nothing the receiver
/// does not): such a schedule is the shorter one without the step, enumerated too.
#[test]
#[ignore = "the model's exhaustive run; see the convergence model test report"]
fn exhaustive_reduced_alphabet() {
    let depth = env_u64("CONV_DEPTH", 4) as usize;
    let alphabet: Vec<Step> = vec![
        Create { m: A, c: ORDERS },
        Create { m: B, c: ORDERS },
        Write { m: A, c: ORDERS, d: 1 },
        Write { m: B, c: ORDERS, d: 2 },
        Drop { m: A, c: ORDERS },
        DropDb { m: B },
        Pull { to: B, from: A },
        Pull { to: A, from: B },
        PullShort { to: B, from: A },
        Whole { to: C, from: B },
        Scoped { to: C, from: A, c: ORDERS },
        Restart { m: B },
    ];
    let schedules = enumerate(&alphabet, depth);
    sweep(
        &format!("exhaustive depth {depth}"),
        schedules.into_iter().enumerate().map(|(i, ops)| (format!("#{i}"), ops)),
    );
}

/// Every schedule of 1..=`depth` steps over `alphabet` in which every step can do
/// something, timed `100 + 10 * position` (the reduced alphabet has no skew).
pub(crate) fn enumerate(alphabet: &[Step], depth: usize) -> Vec<Vec<Op>> {
    #[derive(Clone)]
    struct Knows {
        /// Local steps each member may know of, as a bit set over positions.
        knows: [u64; 3],
        may_hold: [bool; 3],
    }
    fn effective(step: Step, k: &Knows, position: usize) -> Option<Knows> {
        let mut next = k.clone();
        match step {
            Create { m, .. } => {
                next.may_hold[m as usize] = true;
                next.knows[m as usize] |= 1 << position;
            }
            Write { m, .. }
            | Drop { m, .. }
            | Index { m, .. }
            | DropIndex { m, .. }
            | Delete { m, .. }
            | DropDb { m } => {
                if !k.may_hold[m as usize] {
                    return None;
                }
                next.knows[m as usize] |= 1 << position;
            }
            Pull { to, from }
            | PullShort { to, from }
            | Whole { to, from }
            | Scoped { to, from, .. } => {
                let (to, from) = (to as usize, from as usize);
                if k.knows[from] & !k.knows[to] == 0 {
                    return None;
                }
                next.knows[to] |= k.knows[from];
                next.may_hold[to] |= k.may_hold[from];
            }
            Restart { m } | Gc { m } | AgeOut { m } | Purge { m } => {
                if k.knows[m as usize] == 0 {
                    return None;
                }
            }
            PurgerOff => {}
        }
        Some(next)
    }
    let mut out = Vec::new();
    let mut frontier: Vec<(Vec<Op>, Knows)> =
        vec![(Vec::new(), Knows { knows: [0; 3], may_hold: [false; 3] })];
    for position in 0..depth {
        let mut next_frontier = Vec::new();
        for (ops, k) in &frontier {
            for step in alphabet {
                if let Some(k2) = effective(*step, k, position) {
                    let mut ops2 = ops.clone();
                    ops2.push(Op { step: *step, t: 100 + 10 * position as u32 });
                    out.push(ops2.clone());
                    next_frontier.push((ops2, k2));
                }
            }
        }
        frontier = next_frontier;
    }
    out
}

#[test]
#[ignore = "timing probe"]
fn timing_probe() {
    let n = env_u64("CONV_N", 10);
    let _ = templates();
    for _ in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let t0 = std::time::Instant::now();
        std::fs::copy(&templates()[0], &path).unwrap();
        let copied = t0.elapsed();
        let e = Engine::open(&path).unwrap();
        let opened = t0.elapsed();
        let c = e.commits();
        drop(e);
        println!(
            "copy {copied:?} open {opened:?} commits at open {c} size {}",
            std::fs::metadata(&path).unwrap().len()
        );
    }
    let started = std::time::Instant::now();
    let mut commits = 0;
    for seed in 0..n {
        let ops = generate(seed, 10, Track::Full);
        let t0 = std::time::Instant::now();
        let mut cluster = Cluster::new(base());
        let opened = t0.elapsed();
        cluster.run(&ops).unwrap();
        let ran = t0.elapsed();
        let c0: u64 = (0..3).map(|m| cluster.e(m).commits()).sum();
        cluster.quiesce(Route::Entries).unwrap();
        let q = t0.elapsed();
        let c1: u64 = (0..3).map(|m| cluster.e(m).commits()).sum();
        commits += c1;
        println!("seed {seed}: open {opened:?} run {ran:?} quiesce {q:?} commits {c0} -> {c1}");
    }
    println!("{n} in {:?}, {commits} commits", started.elapsed());
}

#[test]
fn the_enumeration_counts() {
    let alphabet = [Create { m: A, c: ORDERS }, Pull { to: B, from: A }];
    // A.create; A.create,A.create(no: held is still "may do"); B<-A after a create.
    let all = enumerate(&alphabet, 2);
    assert!(
        all.iter().all(|ops| ops[0].step == Create { m: A, c: ORDERS }),
        "a first delivery does nothing"
    );
}
