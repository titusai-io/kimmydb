//! The idempotency model test (ADR-240, design "Tests > Model test"): three or four engines
//! run random, seeded schedules of keyed writes, retries, crashes, pulls, snapshots,
//! outages, drops, restores and retention, and invariants I1 to I7 are asserted after
//! every step and at quiescence.
//!
//! **Members.** Three engines in one process, or four (a quarter of the schedules), opened
//! from template stores whose node identities are fixed (`0x11..` to `0x44..`), so stamps
//! tie-break the same way on every run; a member reset is a fresh store with a spare
//! fixed identity (`0x55..` on), as a wiped node is given a new one. Every engine commits
//! `multi` writes in chunks of two documents.
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
//! before the schedule and replicated everywhere; keyed `$inc` requests change them
//! (every counter, chunked by two; two counters in one chunk; or one, the single form),
//! unkeyed writes write on top of them. `orders` is dropped and created again too, a life
//! beginning with its counters fresh, and a drop made on one member can reach another
//! between two commits of a keyed `$inc` there, so a multi-chunk request is applied on
//! both sides of a concurrent drop. `shop.items` takes keyed inserts with and without
//! `_id` (the derived one), bulk inserts of two, replaces and deletes by id, unkeyed
//! writes, and is dropped and created again. `shop.big`, in
//! named rows only, holds more documents than three snapshot pages.
//!
//! **Key sets.** Every version a keyed request writes carries, in its `ks` field, the
//! requests whose applications it contains: the base's list and the request itself,
//! appended even when present (so a second application is never an unchanged no-op).
//! An unkeyed write keeps the list it wrote on. A merge keeps the winner's body, and so
//! the winner's set. A tombstone's set is kept by the test, by its stamp.
//!
//! **Steps.** Send a request (a first send or a retry) to a member, with a crash at
//! one of its commits (`AfterCommit`: the drain deadline passes, so the request's next
//! transaction is refused; `BeforeCommit`: the next commit's first write fails;
//! `FsyncFails`: the next commit's fsync fails, its outcome unknown), after which the
//! member is reopened, and with steps run between any two of its commits (pulls in any
//! direction, a concurrent attempt of the same request on another member, an unkeyed
//! write, a page, a restart of another member, its own clock moving); pull a window, or a
//! short one (cut at two entries); write unkeyed; take a scoped or converge pull of a
//! collection, optionally followed by a restart; reset a member (a fresh store, then a
//! whole-database snapshot), with writes on the sender between its pages; take a member
//! down and up; restart or crash a member; back a member up and restore it (from its
//! latest backup or an earlier one, and the sender of a page or a snapshot between two
//! of its pages), which then replays its own origin from its peers (ADR-212, each page
//! cut as the transport's `cut_replay_page` cuts it, in a frame of a few units: the
//! spanning records, and records alone under a records cursor), with steps between the
//! replay's pages; rewind a member (`restore --until`, offline), as the documented
//! procedure has it (alone when no peer holds a write the rewind undoes, else every
//! member to the same target), each then replaying its own origin the same way; leave orphan rows by stamp (records past their window removed, as
//! a retention pass stopped between chunks leaves them); drop or create `items` or
//! `orders` (a drop of `orders` on another member also between two commits); and a
//! retention pass with a moved clock. "Between two commits" is the
//! engine's own commit point (`barrier_hooks::Point::Released`, after every commit,
//! durable by then), so it is between any two commits whatever the code under test makes
//! them.
//!
//! **Like the transport and the server.** A pull applies the sender's verdict: a refusal
//! (`BeyondHorizon`, which `StateHeld` reads as) sets the peer aside once, the other live
//! contacts are pulled, and a second refusal takes the peer's whole-database snapshot
//! (ADR-224); windows are served by `serve_entries_to_peer`. Quiescence runs the
//! divergence check's repairs and the cut digest's converge pages. No keyed request goes
//! to a member that is down, nor to one catching up after a whole-database snapshot
//! (until its witnessed vector covers every peer's version vector; a named row can send
//! there, as the gate allows once it opens as `unknown`), nor once its key's memory
//! ended: a retention step closes an era, and a request of an earlier era is not sent
//! again (a retry then is a new request, guarantee 7).
//!
//! **Tracks.** `Home`: every `$inc` request and every retry of it goes to one member
//! (the home), which is never reset or restored, each is driven to completion there
//! before another `$inc`, an unkeyed write of a counter, or a retention step, and I5
//! holds exactly. `Anywhere`: retries land on any member, concurrent attempts on two
//! members included, and I6 bounds the counters.
//!
//! **Records outlive a drop** (ADR-240): a record lives its window, whatever happens to
//! its collection, so a retry after a drop (and a creation again) replays its first
//! answer and writes nothing to the life that stands. The invariants hold across lives.
//!
//! **The invariants**, after every step unless said otherwise:
//! - **I1**, never on its own effect: no application of K writes on a base whose set
//!   holds K. Checked where it happens (the `$inc`'s `apply` sees its base; the test
//!   reads the base of an insert, replace or delete just before it) and on every
//!   stored version (no list holds a request twice).
//! - **I2**, the record with the effect: a member that processed an entry of K by
//!   history (its own, one a pull shipped it and its coverage then passed, or one an
//!   own-origin replay read back) holds K's live record with `first` at or below that
//!   entry, unless K's memory ended, whatever life the entry is of; and a member that
//!   completed a snapshot holds every live record the sender held for what the snapshot
//!   carried, merged (`first` at or below, `last` at or above), save the keys written on
//!   the sender during it.
//! - **I3**, once per member: on one member, a request writes each document at most
//!   once across all its attempts, in every life of the collection (counted from the
//!   entries each node minted).
//! - **I4**, after quiescence: identical documents, identical live records, and at
//!   most one document per (request, position) of a keyed insert.
//! - **I5** (`Home`), after quiescence: each counter is the number of `$inc` requests
//!   accepted for it. Not in a run with a drop of `orders` (a life begins with fresh
//!   counters, and a drop keeps the versions stamped after it, ADR-236): I8 instead.
//! - **I6**: no counter is ever above the number of `$inc` requests sent for it.
//! - **I7**: a keyed insert never lands on a derived `_id` another request's insert
//!   landed on, and no live inserted document at a derived `_id` was written by
//!   another request.
//! - **I8**, at each completion of an `$inc` on a member (not a replay): every counter it
//!   targets that the member holds in a version older than the request's merged `first`
//!   holds its application. The stamp rule leaves what is stored at or above the merged
//!   `first` (written meanwhile by another, or, after a drop, the later life's
//!   documents), counted as left; and a request never completes on a record whose
//!   `first` is another request's (such a record is contested).
//! - **Life**: a member that holds K's live `done` record replays it (a delete by id
//!   while its document's row is at or above the delete, or the row is gone and the
//!   collection's drop or floor is at or above it), and no replay answers from a record
//!   past its window; an `applying` record of any kind but a filter's replays too. A
//!   rewind leaves no record of a request whose every write it discarded; one it cut
//!   part-way goes only as a bulk insert's (its standing prefix bare by design)
//!   and is reopened `applying` as any other kind's, its rows kept (a filter's with no
//!   cursor); and it leaves no row by stamp naming a record it removed.
//!   A keyed delete by id is judged by the record's `first`. A record past its window
//!   merges into a live record of the same request and is refused otherwise.
//! - **Contested** (the contested flag): the test keeps, per member and key, the requests
//!   merged into its live record (a local commit adds its own; a stored record adds its
//!   sender's; an absent record has none), so the merge algebra `a.contested ||
//!   b.contested || a.fingerprint != b.fingerprint` is the set's size: a live record is
//!   contested exactly when its set holds two or more. A lookup refuses (422) exactly
//!   where the record before the call is contested or another request's, and writes
//!   nothing; a request whose record turns contested at a commit point writes no later
//!   chunk and answers `partially_applied`.
//! - **Carried**: every record a frame or a page carries stands on its sender as carried
//!   and rides at or above its `first`: an orphan row ships nothing, and a row below a
//!   record's `first` (an earlier record's under the key) vouches for nothing of it.
//!
//! **Findings the model classifies**, each counted in every run's summary line:
//! - **Relay** (relays after a snapshot, of a losing entry, of a coverage-only window,
//!   across origins, and by a replay): an effect or an entry of K reached a member without
//!   K's record (an application on its own effect by a member that held no live record and
//!   had taken no entry of K; an entry shipped without the record by a sender that held it;
//!   a record missing at quiescence where a document holds K). Fixed by records travelling
//!   with coverage. **Fails every run** (generated and named) by default; `IDEM_LENIENT=1`
//!   counts it instead, for a tree without that fix.
//! - **KR**, a contested key completed on (see I8): fails every run.
//! - **S1**: a record at quiescence that the merged record dominates, or none where no
//!   document holds the key: a record state carried only by entries that lost where the
//!   member pulled them from, or by a record-only `done` (G8's ruled residual). Harmless
//!   while I2 holds. Counted in a generated run; fails it with `IDEM_STRICT=1`, and fails
//!   every named row.
//! - **R1** (counted, never failed; a convergence matter outside idempotency keys): a reset
//!   identity's writes, or a restored identity's writes after its backup, kept by peers
//!   only as page state, never converge to the reset or restored member.
//!
//! Under the life rule the extended model found a retry after a drop doubling a counter a
//! later write carried over, a resume across a drop leaving the later life's counters, and
//! a reused key's merged records. With records outliving a drop the first is a Life and I1
//! violation and the second the stamp rule's contract; the third is refused by the
//! `contested` flag, and the rows `row_d2_*`, `row_d3_*` and `row_kr_*` hold the engine to
//! it.
//!
//! **Findings RW1 and BR1** (2026-10-18, outside the idempotency code, fixed on main by #209 and
//! #211): a rewind after a retention pass lowered the version vectors below the writes
//! retention had collected, and a restore counted the rows a page left as held state as
//! covered. Either left a member's coverage past writes it never took, and their records
//! never reached it. `row_rw1_*` reproduces the first, and the restore check (a restore
//! never claims more of another origin than its backup held) sees the second where it
//! happens; both now hold, and every generated schedule takes rewinds and restores.
//!
//! **Reproducers**, both run (not ignored) and pass since their fixes landed:
//! `finding_g1_*` (the relay as first reported: its pull serves the window without the
//! sender's verdict, which production refuses as `StateHeld`; kept as the mechanism's
//! minimal form) and `finding_l1_*`. The named rows `row_*` write out the breaks an
//! analysis of relays without the record found, and the shapes the design's mutants need,
//! each run strictly; those ignored name what they wait for.

// Members are indices into small per-member arrays (`LETTERS`, the members, a step's
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
const NAMES: [&str; 3] = ["orders", "items", "big"];
const ORDERS: u8 = 0;
const ITEMS: u8 = 1;
/// A collection of more documents than three snapshot pages hold, for the rows whose
/// snapshot must span pages within one collection. Not in generated schedules.
const BIG: u8 = 2;
const BIG_DOCS: i64 = 3 * crate::SNAPSHOT_PAGE as i64 + 40;
const LETTERS: [char; 4] = ['A', 'B', 'C', 'D'];
/// Members a schedule can have.
const MEMBERS_MAX: usize = 4;
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
/// The model's frame for a replay page, in units: an entry or a record costs one. Small,
/// so that a page whose first entry needs more records than a frame holds carries
/// records alone in generated schedules too.
const REPLAY_FRAME: usize = 4;
/// The bytes a replay page's records delta under a cursor takes (`budget / 2` of the
/// transport's frame): a few records, so a delta that does not reach the log's end
/// moves the cursor's base.
const REPLAY_DELTA_BYTES: usize = 800;
/// A restore of this member, between two pages of a replay, restores the member that
/// served the page just applied (its change log begins again, with another epoch).
const SENDER: u8 = u8::MAX;

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
    /// `$inc` of counters `d` and `d + 1`: a multi-document request of one chunk.
    IncRange {
        d: u8,
    },
    /// `$inc` of document `d` of `big`, the single-document form.
    IncBig {
        d: u16,
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
            Kind::IncMulti | Kind::IncSingle { .. } | Kind::IncRange { .. } => ORDERS,
            Kind::IncBig { .. } => BIG,
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
            Kind::IncRange { d: x } => i64::from(*x) == d || i64::from(*x) + 1 == d,
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
    /// A scoped snapshot of collection `c` (a converge pull with `converge`), then a
    /// restart of the receiver with `restart`. `inner`: steps run after its `j`-th page.
    Page {
        to: u8,
        from: u8,
        c: u8,
        converge: bool,
        restart: bool,
        inner: Vec<(u8, Vec<Op>)>,
    },
    /// A fresh store with a new identity, then a whole-database snapshot from `from`,
    /// with `inner` steps run after its `j`-th page.
    Reset {
        m: u8,
        from: u8,
        inner: Vec<(u8, Vec<Op>)>,
    },
    /// An unkeyed write on `m`: counter `d` of `orders`, `ITEM_BASE + d` of `items`
    /// (upserted), or document `d` of `big`. It keeps the version's key set: a write
    /// on top of what `m` holds.
    Touch {
        m: u8,
        c: u8,
        d: u16,
    },
    /// The member stops being reached: no pull to or from it, no request to it.
    Down {
        m: u8,
    },
    Up {
        m: u8,
    },
    /// Create `big` on `m` with its documents.
    Big {
        m: u8,
    },
    /// Take a backup of `m`, kept for a later restore.
    Backup {
        m: u8,
    },
    /// Restore `m` from a backup (its identity kept), the latest or the `nth` before it,
    /// then replay its own origin from every member that is up, as the transport does
    /// after a restore (ADR-212), with `inner` steps run after its `j`-th replay page.
    Restore {
        m: u8,
        nth: u8,
        inner: Vec<(u8, Vec<Op>)>,
    },
    /// Rewind `m` to `t` (`kimmyd restore --until`, offline: the store is reopened), as
    /// the documented procedure has it: alone when no peer holds a write it undoes, else
    /// every member to the same target; then each replays its own origin, as after a
    /// restore.
    Rewind {
        m: u8,
    },
    /// The forbidden procedure, in one named row only: `m` rewound alone although a peer
    /// holds a write it undoes, then rejoining the cluster.
    RewindAlone {
        m: u8,
    },
    /// Leave the rows by stamp of every record `m` holds past its window and remove the
    /// records, as a retention pass stopped between its chunks leaves them: orphan rows.
    Orphan {
        m: u8,
    },
    Restart {
        m: u8,
    },
    /// Dropped without a close: the next open repairs.
    Crash {
        m: u8,
    },
    /// Drop collection `c` (`items` or `orders`).
    Drop {
        m: u8,
        c: u8,
    },
    /// Create collection `c`; `orders` is created with its counters, fresh.
    Create {
        m: u8,
        c: u8,
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
    /// Members: three, or four.
    pub members: u8,
    pub requests: Vec<Request>,
    pub ops: Vec<Op>,
    /// The budgets a schedule runs under: small ones in some generated schedules, so
    /// that the paths behind them are reached (see [`Bias`]).
    pub bias: Bias,
}

/// The budgets of one schedule. A generated schedule takes small ones now and then, so
/// that the long run reaches the paths behind them in a real share of its seeds (the
/// coverage assertion of `generated_schedules_long` holds it to that).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Bias {
    /// The model's frame for a replay page, in units ([`REPLAY_FRAME`]).
    pub frame: usize,
    /// Every member's room for a snapshot page's rows, in bytes
    /// (`Engine::set_snapshot_room_for_test`), so a collection of a few documents spans
    /// pages, for the schedule's first step alone (the pull a restore splits): later documents grow
    /// past any small room as their key sets do; `None` for the real one.
    pub page_room: Option<usize>,
    /// The bytes of a replay page's records delta under a cursor ([`REPLAY_DELTA_BYTES`]).
    pub delta_bytes: usize,
}

impl Default for Bias {
    fn default() -> Self {
        Bias { frame: REPLAY_FRAME, page_room: None, delta_bytes: REPLAY_DELTA_BYTES }
    }
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
                show_inner(f, inner)
            }
            Step::Pull { to, from } => write!(f, "{}<-{}", l(*to), l(*from)),
            Step::PullShort { to, from } => write!(f, "{}<-{} short", l(*to), l(*from)),
            Step::Page { to, from, c, converge, restart, inner } => {
                write!(
                    f,
                    "{}<-{} {}({}){}",
                    l(*to),
                    l(*from),
                    if *converge { "converge" } else { "scoped" },
                    NAMES[*c as usize],
                    if *restart { " then restart" } else { "" }
                )?;
                show_inner(f, inner)
            }
            Step::Reset { m, from, inner } => {
                write!(f, "{}.reset from {}", l(*m), l(*from))?;
                show_inner(f, inner)
            }
            Step::Touch { m, c, d } => write!(f, "{}.touch({}/{d})@{t}", l(*m), NAMES[*c as usize]),
            Step::Down { m } => write!(f, "{}.down", l(*m)),
            Step::Up { m } => write!(f, "{}.up", l(*m)),
            Step::Big { m } => write!(f, "{}.big", l(*m)),
            Step::Backup { m } => write!(f, "{}.backup", l(*m)),
            Step::Restore { m, nth, inner } => {
                if *m == SENDER {
                    write!(f, "the replay page's sender.restore")?;
                } else {
                    write!(f, "{}.restore", l(*m))?;
                }
                if *nth > 0 {
                    write!(f, " from the backup {nth} before the latest")?;
                }
                show_inner(f, inner)
            }
            Step::Rewind { m } => write!(f, "{}.rewind to {t}", l(*m)),
            Step::RewindAlone { m } => write!(f, "{}.rewind alone to {t} (forbidden)", l(*m)),
            Step::Orphan { m } => write!(f, "{}.orphan rows", l(*m)),
            Step::Restart { m } => write!(f, "{}.restart", l(*m)),
            Step::Crash { m } => write!(f, "{}.crash", l(*m)),
            Step::Drop { m, c } => write!(f, "{}.drop({})@{t}", l(*m), NAMES[*c as usize]),
            Step::Create { m, c } => write!(f, "{}.create({})@{t}", l(*m), NAMES[*c as usize]),
            Step::Retention { m } => write!(f, "{}.retention@{t}", l(*m)),
            Step::Clock { m } => write!(f, "{}.clock@{t}", l(*m)),
        }
    }
}

fn show_inner(f: &mut std::fmt::Formatter<'_>, inner: &[(u8, Vec<Op>)]) -> std::fmt::Result {
    for (j, ops) in inner {
        let shown: Vec<String> = ops.iter().map(|op| op.to_string()).collect();
        write!(f, " [at {j}: {}]", shown.join(", "))?;
    }
    Ok(())
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

/// The member stores and the spares, empty, with fixed identities, opened once and
/// copied per run.
fn templates() -> &'static [PathBuf; MEMBERS_MAX + SPARES] {
    static TEMPLATES: OnceLock<(tempfile::TempDir, [PathBuf; MEMBERS_MAX + SPARES])> =
        OnceLock::new();
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
    /// After a restore or a rewind, until it holds what its peers hold.
    restored_behind: bool,
}

/// A member's backup: the identity it was taken under, its bytes, and the contested
/// algebra's sets of that identity's records as they stood.
#[derive(Clone)]
struct Backup {
    node: NodeId,
    bytes: Vec<u8>,
    fps: Vec<((u8, u8), BTreeSet<u8>)>,
    /// The member's witnessed vector when the backup was taken: a restore of it claims
    /// no more of any origin (finding BR1).
    witnessed: kimmy_core::VersionVector,
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
    /// A keyed `$inc` that completed on a member left a counter it targets without its
    /// application, where the counter's version there is older than the request's first
    /// write in the member's current life of `orders` (see the module docs).
    I8,
    Life,
    /// The contested algebra (the contested flag, 2026-10-18): a member's live record
    /// is contested exactly when the requests merged into it are two or more, a lookup
    /// on a contested record (or another request's) refuses, and a multi-chunk request
    /// whose record turns contested between its chunks stops there, partially applied.
    Contested,
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
    /// Effects or entries that reached a member without their record: the relay
    /// class (relays after a snapshot, of a losing entry, of a coverage-only window, and
    /// across origins).
    pub relay: usize,
    /// Settled records that differ only by a dominated, earlier state: finding S1.
    pub s1: usize,
    pub replays: usize,
    pub resumes: usize,
    pub crashes: usize,
    /// Steps run between two commits of a request.
    pub between: usize,
    /// Settled documents that differ only by a retired identity's versions (R1).
    pub retired: usize,
    /// `$inc` completions I8 checked: those with a live record after the call.
    pub i8_checks: usize,
    /// Counters a completed `$inc` left because their version was at or above its merged
    /// `first` (the stamp rule, by design; after a drop, the later life's documents).
    pub left_by_first: usize,
    /// Calls of a keyed request of `orders` during which a drop of `orders` reached
    /// their member (between two of their commits, or before the first).
    pub drop_mid_request: usize,
    /// `$inc` requests with writes on both sides of a drop of `orders`, on any members.
    pub straddled: usize,
    /// Calls whose own writes lie on both sides of a drop of `orders` (made anywhere):
    /// a drop between two chunks of one application.
    pub call_straddles: usize,
    /// Home-track runs whose exact I5 a drop of `orders` turned off.
    pub i5_off: usize,
    /// A contested key completed on, a failure: an `$inc` completed on a record whose
    /// `first` is another request's under a reused key (see `check_i8`).
    pub kr: usize,
    /// Lookups refused on a record the model holds contested, as the algebra says.
    pub contested_refused: usize,
    /// Members' records the algebra held contested, each once.
    pub contested_records: usize,
    /// Multi-chunk requests stopped partially applied because their record turned
    /// contested between two of their chunks, as the algebra says.
    pub contested_stopped: usize,
    /// Snapshot pages refused because they were asked with the zero records base of a
    /// sender whose change log began since (the zero-base guard).
    pub records_restarts: usize,
    /// Replay pages that carried records alone, under a records cursor.
    pub records_alone: usize,
    /// Replay pages at a records cursor's position the sender's log no longer honoured
    /// (another epoch: the sender was restored, the cursor's guard).
    pub cursor_restarts: usize,
    /// Records a frame offered a receiver past their window there (refused, unless
    /// of the live record's own request).
    pub past_window_offered: usize,
    /// Of them, those merged into a live record of the same request, lowering its
    /// `first`.
    pub past_window_merged: usize,
    /// Rewinds taken (each member rewound counts), and the records they removed and
    /// reopened.
    pub rewinds: usize,
    /// Generated rewinds of one member that no peer held a write of (the procedure's
    /// first shape), and those widened to every member because a peer held one (its
    /// second shape; a single-member rewind there is the forbidden procedure).
    pub rewinds_alone: usize,
    pub rewinds_widened: usize,
    /// Rewinds not taken: on the home track, widened, they would rewind the home.
    pub rewinds_skipped: usize,
    /// Member rewinds past a drop the member holds (entries judged across a life a drop
    /// ended, ADR-248), and refusals by kind: a document with no value at the target this
    /// member holds (an ended life's version, a replacement, a collected entry), and a
    /// schema change after the target.
    pub rewinds_across_a_drop: usize,
    pub rewind_refused_unrecoverable: usize,
    pub rewind_refused_schema: usize,
    /// Writes a member rewound alone undid (`Step::RewindAlone`) that came back to it.
    pub undone_returned: usize,
    pub rewound_removed: usize,
    pub rewound_reopened: usize,
    /// Records removed with their rows left (orphan rows).
    pub orphaned: usize,
    /// Replay pages whose cursor's delta met a changed record too large for it.
    pub delta_too_large: usize,
    /// Records cursors dropped for that, the position's records read again from a fresh
    /// base.
    pub cursor_dropped: usize,
    /// Replay pages that could not carry what they must (the model's whole-frame
    /// refusal: F3 records more than a frame, or a page of records alone that sends
    /// nothing new): the replay stays at its position for the contact.
    pub replay_unfit: usize,
}

type Res<T> = std::result::Result<T, String>;

/// A member's live records under the keys a frame carries, read before it is offered:
/// by (collection, key).
type LiveBefore = Vec<((u8, u8), Option<IdempotencyRecord>)>;

/// Every record a member stores, by (collection id, scope).
type StoredRecords = BTreeMap<(u64, [u8; 32]), IdempotencyRecord>;

/// A member's records, and its rows by stamp that name none.
type RecordsAndOrphans = (StoredRecords, BTreeSet<Vec<u8>>);

fn err(what: &str, e: impl std::fmt::Display) -> String {
    format!("{what}: {e}")
}

/// `IDEM_TRACE=1`: every window, its entries and its records, in the notes.
fn trace() -> bool {
    std::env::var("IDEM_TRACE").is_ok_and(|v| v == "1")
}

/// Whether the relay class fails a generated run: yes, by default, since fix (e)
/// landed; `IDEM_LENIENT=1` counts it instead, for a tree without that fix.
fn strict() -> bool {
    !std::env::var("IDEM_LENIENT").is_ok_and(|v| v == "1")
}

/// Whether S1 fails a generated run as well: `IDEM_STRICT=1`. Counted by default, since
/// it holds G8's ruled residual (a record-only `done` keeps `last`, and never reaches a
/// member that covers it).
fn strict_s1() -> bool {
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
    /// The documents it changes, by `_id`, both ends included; all, without.
    target: Option<(i64, i64)>,
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
            Some((lo, hi)) => doc.get_i64("_id").is_ok_and(|id| (lo..=hi).contains(&id)),
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
    /// (collection, document key, request) of every application on its own effect that
    /// is a classified finding (the relay class): a stored double it explains.
    explained: HashSet<(u8, Vec<u8>, u8)>,
    /// Every drop of a collection minted, as its tombstone's stamp.
    drops: Vec<(u8, Stamp)>,
    /// (node, entry) a sender holding the record shipped without it (the relay class).
    g1b: HashSet<(NodeId, Stamp)>,
    /// Those relay entries I2 has counted.
    g1b_counted: HashSet<(NodeId, Stamp)>,
    /// The contested algebra, by (node, collection, key): the requests whose records the
    /// node's live record under the key has merged. A local commit of request `r` adds
    /// `r`; a stored record from a peer adds the sender's set; a record that is absent
    /// (past its window, removed) has none, and the next store starts it again. The
    /// record is contested exactly when the set holds two or more (`a.contested ||
    /// b.contested || a.fingerprint != b.fingerprint` is the union's size).
    fps: HashMap<(NodeId, u8, u8), BTreeSet<u8>>,
    /// Requests a rewind cut part-way whose record it removed by design (a bulk
    /// insert whose prefix stands, every request merged into its record): their standing
    /// entries hold no record, and a retry meets the first standing document as a
    /// duplicate and writes nothing. I2 does not hold them to a record.
    bare: HashSet<u8>,
    /// Every (node, collection, key) whose live record the algebra has held contested.
    contested: HashSet<(NodeId, u8, u8)>,
    /// Every (node, collection, key) [`World::check_effects`] found holding an effect
    /// without its record: counted once.
    effect_bare: HashSet<(NodeId, u8, u8)>,
}

impl Ghost {
    fn mark_processed(&mut self, node: NodeId, stamp: Stamp) {
        if !self.processed.contains_key(&(node, stamp)) {
            self.seq += 1;
            self.processed.insert((node, stamp), self.seq);
        }
    }
}

// -- A replay page ---------------------------------------------------------------------------

/// One page of a replay as cut to fit the model's frame (`transport::ReplayCut`).
struct ReplayCut {
    entries: Vec<kimmy_core::OplogEntry>,
    records: Vec<crate::idempotency::CarriedRecord>,
    scanned_to: Hlc,
    exhausted: bool,
    replay_records: Option<crate::ReplayRecords>,
}

/// How much of a page fits a frame (`transport::Fits`).
enum Fit {
    All,
    Only(usize),
    Nothing,
}

/// `transport::how_many_fit` with unit costs: the F3 records first, then each entry with
/// the records riding it (carried at or before its stamp), then the rest at the end.
fn fits(
    entries: &[kimmy_core::OplogEntry],
    records: &[crate::idempotency::CarriedRecord],
    unanchored: usize,
    budget: usize,
) -> Fit {
    let unanchored = unanchored.min(records.len());
    let mut total = unanchored;
    if total > budget {
        return Fit::Nothing;
    }
    let mut riders: Vec<Stamp> = Vec::new();
    let mut at_end = 0usize;
    for record in &records[unanchored..] {
        match record.carrying {
            Some(at) => riders.push(at),
            None => at_end += 1,
        }
    }
    riders.sort();
    let mut next = 0usize;
    for (i, entry) in entries.iter().enumerate() {
        let mut size = 1;
        while next < riders.len() && riders[next] <= entry.stamp {
            size += 1;
            next += 1;
        }
        if total + size > budget {
            return Fit::Only(i);
        }
        total += size;
    }
    if total + at_end + riders.len() - next > budget {
        return match entries.len() {
            0 => Fit::Nothing,
            n => Fit::Only(n - 1),
        };
    }
    Fit::All
}

// -- A prepared request ----------------------------------------------------------------------

enum Call {
    Insert(Document),
    Bulk(Vec<Document>),
    Inc { target: Option<(i64, i64)>, stop: Option<usize> },
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
    /// A delete by id's target row before the call, live or a tombstone: its stamp.
    row_before: Option<Stamp>,
    /// The member's tombstone or floor of the collection before the call.
    floor: Hlc,
    /// The greatest `last` of the request's records seen before the call.
    last_before: Option<Stamp>,
    on_own_effect: OwnEffect,
    /// The commit points whose steps have run, and the ghost's order after each (the
    /// first before the call): what a member had taken when it wrote.
    phase: Arc<std::sync::atomic::AtomicUsize>,
    phase_seq: Rc<RefCell<Vec<u64>>>,
    /// The algebra's verdict before the call: the live record under the key is contested
    /// (its set holds two requests or more) or another request's, so the lookup refuses;
    /// and whether it is contested.
    refused_pre: bool,
    contested_pre: bool,
    /// After each commit point's steps, whether the algebra says the record refuses the
    /// request's next chunk: one point that says so and a commit after it is a chunk
    /// written on a contested record.
    refusing: Rc<RefCell<Vec<bool>>>,
    /// I8's reading at the last commit point, taken before its steps when it ran any
    /// (an `$inc` only); `None` when the last point ran none.
    i8_at: Rc<RefCell<Option<I8State>>>,
}

/// What I8 reads of a member at a request's completion.
struct I8State {
    floor: Hlc,
    record: Option<IdempotencyRecord>,
    counters: Vec<Option<(Stamp, Option<Document>)>>,
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
/// duplicate `_id`, a collection that is gone, or a multi-chunk request stopped because
/// its record turned contested between two chunks (whether it should have is the
/// algebra's check, in `after_send`). Anything else fails the run.
fn expected_failure(e: &StorageError) -> bool {
    matches!(e, StorageError::IdempotencyKeyReused | StorageError::Core(_)) || stopped_contested(e)
}

/// A multi-chunk request's answer when its record turned contested between two of its
/// chunks: what the earlier chunks wrote stands (ADR-192).
fn stopped_contested(e: &StorageError) -> bool {
    matches!(e, StorageError::PartiallyApplied { cause, .. }
        if matches!(**cause, StorageError::IdempotencyKeyReused))
}

/// The collection of the model a carried record names, and its key.
fn key_of(carried: &crate::idempotency::CarriedRecord) -> Option<(u8, u8)> {
    let c = (0..NAMES.len() as u8).find(|c| coll_id(*c) == carried.collection)?;
    Some((c, carried.scope.0[0]))
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
    /// The member a request with steps between its commits runs on, and the request.
    running: Option<usize>,
    running_req: Option<u8>,
    /// Identities a reset retired.
    retired: HashSet<NodeId>,
    /// Restored identities, and where their store's own origin was at the restore.
    restored: HashMap<NodeId, Hlc>,
    /// A drive is running: the home track's policy does not nest.
    driving: bool,
    /// The relay class and L1 fail the run ([`strict`], and every named row).
    strict: bool,
    /// S1 fails the run as well ([`strict_s1`], and every named row).
    strict_s1: bool,
    /// Members the schedule has taken down.
    down: Vec<bool>,
    /// Each member's backup, with the identity it was taken under.
    backups: HashMap<usize, Vec<Backup>>,
    /// Requests go to a member catching up after a whole-database snapshot: what the
    /// catching-up gate allows once it opens as `unknown`, for a row.
    retry_catching_up: bool,
    /// The model's frame for a replay page ([`REPLAY_FRAME`]; a row sets its own).
    replay_frame: usize,
    /// The page room every member opens with ([`Bias::page_room`], until the first step
    /// ends).
    page_room: Option<usize>,
    /// A rewind `rewind_to` made, for its checks.
    pending_rewind: Option<(RecordsAndOrphans, crate::rewind::RewindOutcome)>,
    /// The writes a member rewound alone undid that a peer held (`Step::RewindAlone`, the
    /// forbidden procedure): (member, stamp, collection, document).
    undone: Vec<(usize, Stamp, u8, DocId)>,
    step: String,
}

impl World {
    fn new(sched: Schedule, base: u64) -> Self {
        let n = sched.requests.len();
        let count = sched.members as usize;
        let members = templates()[..count]
            .iter()
            .map(|template| {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("kimmy.redb");
                std::fs::copy(template, &path).unwrap();
                let engine = Engine::open(&path).unwrap();
                Member {
                    _dir: dir,
                    path,
                    engine: Some(Arc::new(engine)),
                    catching_up: false,
                    restored_behind: false,
                }
            })
            .collect();
        let sched_frame = sched.bias.frame;
        let sched_room = sched.bias.page_room;
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
            running_req: None,
            retired: HashSet::new(),
            restored: HashMap::new(),
            driving: false,
            strict: strict(),
            strict_s1: strict_s1(),
            down: vec![false; count],
            retry_catching_up: false,
            replay_frame: sched_frame,
            page_room: sched_room,
            pending_rewind: None,
            undone: Vec::new(),
            backups: HashMap::new(),
            step: "setup".into(),
        };
        for m in 0..count {
            let e = world.e(m);
            world.configure(&e);
            world.labels.insert(e.node_id(), LETTERS[m].to_string());
        }
        world
    }

    fn n(&self) -> usize {
        self.members.len()
    }

    fn e(&self, m: usize) -> Arc<Engine> {
        Arc::clone(self.members[m].engine.as_ref().expect("the member is open"))
    }

    fn configure(&self, e: &Engine) {
        e.set_multi_chunk_docs(CHUNK);
        e.set_idempotency_window(WINDOW_SECS);
        e.set_idempotency_clock_for_test(self.now);
        e.set_snapshot_room_for_test(self.page_room);
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
    /// window. A record outlives a drop of its collection for its window.
    fn live_record(&self, m: usize, c: u8, key: u8) -> Res<Option<IdempotencyRecord>> {
        let record = self
            .e(m)
            .idempotency_record(coll_id(c), &ScopeHash([key; 32]))
            .map_err(|e| err("idempotency_record", e))?;
        Ok(record.filter(|r| !self.past_window(r)))
    }

    // -- The contested algebra ------------------------------------------------------------

    /// The requests merged into `m`'s live record under `key` in `c` (see `Ghost::fps`).
    fn fps(&self, m: usize, c: u8, key: u8) -> BTreeSet<u8> {
        let node = self.e(m).node_id();
        self.ghost.fps.get(&(node, c, key)).cloned().unwrap_or_default()
    }

    /// A local commit of request `r` on `m` stored its record, merged into the live one.
    fn fps_own(&mut self, m: usize, r: u8) {
        let req = self.sched.requests[r as usize];
        let node = self.e(m).node_id();
        self.ghost.fps.entry((node, req.kind.coll(), req.key)).or_default().insert(r);
    }

    /// Forget the set of every key whose record `m` no longer holds alive: the next store
    /// under it starts again (a record past its window is overwritten, never merged).
    fn fps_expire(&mut self, m: usize) -> Res<()> {
        let node = self.e(m).node_id();
        for (c, key) in self.keys() {
            if self.live_record(m, c, key)?.is_none() {
                self.ghost.fps.remove(&(node, c, key));
            }
        }
        Ok(())
    }

    /// `m`'s live records under the keys `records` carry, read before they are offered.
    fn live_before(
        &self,
        m: usize,
        records: &[crate::idempotency::CarriedRecord],
    ) -> Res<LiveBefore> {
        let mut out = Vec::new();
        for carried in records {
            let Some((c, key)) = key_of(carried) else { continue };
            if out.iter().any(|(k, _)| *k == (c, key)) {
                continue;
            }
            out.push(((c, key), self.live_record(m, c, key)?));
        }
        Ok(out)
    }

    /// Records from `sender` were offered to `to`, whose live records were `before`: a
    /// key whose live record changed stored the sender's, merged into its own when it
    /// held one alive (the union of the two sets), or in its place. A store that changes
    /// nothing changes no set: the record was held already, or the sender's set is in it
    /// (a contested record stays contested, whatever more merges in).
    fn fps_carried(&mut self, sender: NodeId, to: usize, before: LiveBefore) -> Res<()> {
        let node = self.e(to).node_id();
        for ((c, key), was) in before {
            let now = self.live_record(to, c, key)?;
            if now == was {
                continue;
            }

            let theirs = self.ghost.fps.get(&(sender, c, key)).cloned().unwrap_or_default();
            let mine = self.ghost.fps.entry((node, c, key)).or_default();
            if was.is_none() {
                mine.clear();
            }
            mine.extend(theirs);
        }
        Ok(())
    }

    /// Every record a frame or a page of `from` carries stands on `from` under its key, as
    /// carried, and rides at or above its `first`: a row with no record (an orphan, left by
    /// a retention pass stopped between chunks) ships nothing, and a row below the record's
    /// `first` (left by an earlier record under the key) vouches for nothing of it.
    fn check_carried(
        &mut self,
        from: usize,
        records: &[crate::idempotency::CarriedRecord],
    ) -> Res<()> {
        for carried in records {
            let Some((c, key)) = key_of(carried) else { continue };
            let stored = self
                .e(from)
                .idempotency_record(coll_id(c), &carried.scope)
                .map_err(|e| err("idempotency_record", e))?;
            let sent = IdempotencyRecord::decode(&carried.record).ok().flatten();
            let fine = match (&stored, &sent) {
                (Some(stored), Some(sent)) => {
                    stored == sent && carried.carrying.is_none_or(|at| at >= stored.first)
                }
                _ => false,
            };
            if !fine {
                let s = format!(
                    "{} carried key {key} in {} at {:?}: {:?}, where it holds {:?}",
                    LETTERS[from],
                    NAMES[c as usize],
                    carried.carrying.map(|s| self.stamp(s)),
                    sent.map(|r| (self.stamp(r.first), self.stamp(r.last))),
                    stored.map(|r| (self.stamp(r.first), self.stamp(r.last)))
                );
                self.fail(Inv::I2, s);
            }
        }
        Ok(())
    }

    /// Whether the algebra says `m`'s lookup refuses request `r`: the live record under
    /// its key merged two requests or more, or is another request's.
    fn refuses(&self, m: usize, r: u8) -> Res<(bool, bool)> {
        let req = self.sched.requests[r as usize];
        let Some(record) = self.live_record(m, req.kind.coll(), req.key)? else {
            return Ok((false, false));
        };
        let contested = self.fps(m, req.kind.coll(), req.key).len() >= 2;
        Ok((contested || record.fingerprint != fingerprint(r), contested))
    }

    /// The algebra against the engine, on every member's live record of every key: the
    /// record is contested exactly when its set holds two requests or more, and an
    /// uncontested record is the one request's of its set.
    fn check_contested(&mut self) -> Res<()> {
        for m in 0..self.n() {
            for (c, key) in self.keys() {
                let Some(record) = self.live_record(m, c, key)? else { continue };
                let set = self.fps(m, c, key);
                if set.len() >= 2 {
                    let node = self.e(m).node_id();
                    if self.ghost.contested.insert((node, c, key)) {
                        self.out.contested_records += 1;
                    }
                }
                let of = record.fingerprint.0[0].wrapping_sub(1);
                let agrees = if set.len() >= 2 {
                    record.contested
                } else {
                    !record.contested && set.len() == 1 && set.contains(&of)
                };
                if !agrees {
                    let s = format!(
                        "{}'s record under key {key} in {} (first {} last {}, r{of}'s, contested \
                         {}) where the requests merged into it are {set:?}",
                        LETTERS[m],
                        NAMES[c as usize],
                        self.stamp(record.first),
                        self.stamp(record.last),
                        record.contested
                    );
                    self.fail(Inv::Contested, s);
                }
            }
        }
        Ok(())
    }

    /// Whether request `r` is of an era a retention step has closed: its key's memory
    /// ended.
    fn memory_ended(&self, r: u8) -> bool {
        self.req_era[r as usize].is_some_and(|era| era < self.era)
    }

    fn finish_purges(&self) -> Res<()> {
        for m in 0..self.n() {
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

    fn reset(&mut self, m: usize, from: usize, inner: &[(u8, Vec<Op>)]) -> Res<()> {
        if self.spare >= SPARES {
            self.note(format!("{} has no spare identity left: restarted instead", LETTERS[m]));
            return self.reopen(m, true);
        }
        if self.down[from] {
            self.note(format!("{} is down: no reset from it", LETTERS[from]));
            return Ok(());
        }
        let arc = self.members[m].engine.take().expect("open");
        self.retired.insert(arc.node_id());
        drop(Arc::into_inner(arc).ok_or("the engine is held elsewhere")?);
        let path = self.members[m].path.clone();
        std::fs::remove_file(&path).map_err(|e| err("remove", e))?;
        std::fs::copy(&templates()[MEMBERS_MAX + self.spare], &path).map_err(|e| err("copy", e))?;
        self.spare += 1;
        self.open(m)?;
        let node = self.e(m).node_id();
        let label = format!("{}{}", LETTERS[m], "'".repeat(self.spare));
        self.labels.insert(node, label.clone());
        self.note(format!("{} reset as {label}", LETTERS[m]));
        self.transfer(m, from, None, None, inner)
    }

    // -- Replication --------------------------------------------------------------------------

    /// One pull, as the transport makes it (`convergence_model_tests::pull`), with the
    /// window's records: a whole-database snapshot beyond the horizon, a scoped page for
    /// a collection the batch stopped at.
    fn pull(&mut self, to: usize, from: usize, limit: usize) -> Res<()> {
        self.pull_from(to, from, limit, true)
    }

    /// [`Self::pull`]. A refusal (`BeyondHorizon`, which `StateHeld` reads as on the
    /// wire) is set aside once when `aside` allows it, as the transport's loop does
    /// (ADR-224): the tick's other contacts are pulled first, then the peer is asked
    /// again, and a second refusal takes its whole-database snapshot.
    fn pull_from(&mut self, to: usize, from: usize, limit: usize, aside: bool) -> Res<()> {
        if self.down[to] || self.down[from] {
            self.note(format!("{}<-{}: one of them is down", LETTERS[to], LETTERS[from]));
            return Ok(());
        }
        let mut stopped_at: Option<Stamp> = None;
        let mut set_aside = !aside;
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
                verdict if !set_aside => {
                    set_aside = true;
                    let others: Vec<usize> = (0..self.n())
                        .filter(|o| *o != to && *o != from && !self.down[*o])
                        .collect();
                    self.note(format!(
                        "{}<-{}: {verdict:?}, set aside: the other contacts first",
                        LETTERS[to], LETTERS[from]
                    ));
                    for other in others {
                        self.pull_from(to, other, BATCH, false)?;
                    }
                    continue;
                }
                verdict => {
                    self.note(format!(
                        "{}<-{}: {verdict:?}, a whole-database snapshot",
                        LETTERS[to], LETTERS[from]
                    ));
                    self.transfer(to, from, None, None, &[])?;
                    break;
                }
            }
            // As the listener serves it.
            let window = src
                .serve_entries_to_peer(start, limit, Some(&mine), &marked, None)
                .map_err(|e| err("serve_entries_to_peer", e))?;
            self.check_carried(from, &window.records)?;
            self.out.past_window_offered += window
                .records
                .iter()
                .filter_map(|c| IdempotencyRecord::decode(&c.record).ok().flatten())
                .filter(|r| self.past_window(r))
                .count();
            let before = self.live_before(to, &window.records)?;
            // A record past its window merges into a live record of
            // the same request (`first` the lower of the two), and is refused otherwise.
            let mut same: Vec<(u8, u8, Stamp)> = Vec::new();
            for carried in &window.records {
                let Some((c, key)) = key_of(carried) else { continue };
                let Some(theirs) = IdempotencyRecord::decode(&carried.record).ok().flatten() else {
                    continue;
                };
                let mine = before.iter().find(|(k, _)| *k == (c, key)).and_then(|(_, r)| r.clone());
                if self.past_window(&theirs)
                    && mine.is_some_and(|mine| mine.fingerprint == theirs.fingerprint)
                {
                    same.push((c, key, theirs.first));
                }
            }
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
            self.fps_carried(src.node_id(), to, before)?;
            for (c, key, first) in same {
                let held = self.live_record(to, c, key)?;
                if held.as_ref().is_some_and(|held| held.first <= first) {
                    self.out.past_window_merged += 1;
                } else {
                    let s = format!(
                        "m2: {} refused key {key}'s record of its own request past its window \
                         (first {}), where it holds the live one ({:?})",
                        LETTERS[to],
                        self.stamp(first),
                        held.map(|h| self.stamp(h.first))
                    );
                    self.fail(Inv::I2, s);
                }
            }
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
                self.transfer(to, from, Some(unknown.id), None, &[])?;
                continue;
            }
            if limit < BATCH || window.exhausted {
                break;
            }
        }
        self.maybe_caught_up(to)
    }

    /// The keyed write the ghost knows by `entry`'s stamp, when the entry is that write's:
    /// of its collection and document. A stamp a member re-mints after a restore lost the
    /// write that held it everywhere (its own-origin replay found no peer holding it) is
    /// another write's, and no keyed request's.
    fn ghost_entry(&self, entry: &kimmy_core::OplogEntry) -> Option<(u8, u8, DocId)> {
        let (r, c, id) = self.ghost.entries.get(&entry.stamp).cloned()?;
        (entry.collection == coll_id(c) && entry.doc_id.as_ref() == Some(&id)).then_some((r, c, id))
    }

    /// The entries of a window `dst` took by history: those its coverage passed, of a
    /// collection it holds (one it does not hold takes them as nothing, and holds no
    /// effect of them).
    ///
    /// An entry of a keyed request shipped without its request's record by a sender
    /// that holds the record alive is the relay class at its source: the window carries
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
            let Some((r, c, _)) = self.ghost_entry(entry) else { continue };
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
        if !self.members[m].catching_up && !self.members[m].restored_behind {
            return Ok(());
        }
        let mine = self.e(m).witnessed_vector().map_err(|e| err("witnessed_vector", e))?;
        for peer in 0..self.n() {
            if peer == m {
                continue;
            }
            let theirs = self.e(peer).version_vector().map_err(|e| err("version_vector", e))?;
            if mine.behind(&theirs).is_some() {
                return Ok(());
            }
        }
        if self.members[m].catching_up {
            self.note(format!("{} caught up", LETTERS[m]));
        }
        self.members[m].catching_up = false;
        self.members[m].restored_behind = false;
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
        inner: &[(u8, Vec<Op>)],
    ) -> Res<()> {
        let mut expected = self.sender_records(from, scope)?;
        let mut sender_restored = false;
        // Keys a step between the pages sends: a keyed write the sender makes during the
        // snapshot is the stated residual for the snapshot half of I2.
        let during: HashSet<u8> = inner
            .iter()
            .flat_map(|(_, ops)| ops.iter())
            .filter_map(|op| match op.step {
                Step::Send { r, .. } => Some(self.sched.requests[r as usize].key),
                _ => None,
            })
            .collect();
        let mut progress = match (scope, converge) {
            (None, _) => SnapshotProgress::whole_database(),
            (Some(id), None) => SnapshotProgress::of_collection(id),
            (Some(id), Some(cut)) => SnapshotProgress::of_collection_converging(id, cut),
        };
        let mut waits = 0;
        let mut restarts = 0;
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
            self.check_carried(from, &page.records)?;
            let before = self.live_before(to, &page.records)?;
            let applied = dst
                .apply_snapshot_page(src.node_id(), &mut progress, &page)
                .map_err(|e| err("apply_snapshot_page", e))?;
            self.fps_carried(src.node_id(), to, before)?;
            if applied.records_restarted {
                // Asked with the zero records base of a sender whose change log has begun
                // since (the zero-base guard): nothing applied, the collection asked for again
                // from its records phase.
                self.out.records_restarts += 1;
                self.note(format!(
                    "{}<-{}: page refused, the sender's change log began since the zero base",
                    LETTERS[to], LETTERS[from]
                ));
                restarts += 1;
                if restarts > 4 {
                    return Err("a snapshot whose records restart for ever".into());
                }
                continue;
            }
            if applied.sender_dropped || applied.history {
                // Refused whole (the sender's life is dropped, or one this member
                // buried): the pull ends with nothing taken.
                refused = true;
                break;
            }
            if applied.purge_pending.is_some() {
                waits += 1;
                if waits > 4 {
                    return Err("a page that waits for a purge that never finishes".into());
                }
                dst.finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
                continue;
            }
            // Let go of both members first: a step between the pages may restore the
            // sender, which reopens its store.
            drop((src, dst));
            for (j, ops) in inner {
                if usize::from(*j) == pages && !progress.is_complete() {
                    for op in ops {
                        sender_restored |=
                            matches!(op.step, Step::Restore { m, .. } if m as usize == from);
                        self.apply_between_pages(op, to, from, scope.is_none())?;
                    }
                }
            }
        }
        if scope.is_none() {
            self.members[to].catching_up = true;
        }
        if refused {
            return Ok(());
        }
        // A sender restored between the pages holds what its backup held, from then on:
        // the receiver is owed every record the sender holds at the end (a page asked
        // with a base the restore made stale starts its collection's records again), and
        // none the restore took away.
        if sender_restored {
            expected = self.sender_records(from, scope)?;
        }
        // I2, a completed snapshot: the receiver holds every live record the sender held
        // for what it carried, merged.
        for (c, key, theirs) in expected {
            if during.contains(&key) || self.e(to).get_collection(DB, NAMES[c as usize]).is_err() {
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
        if (self.members[m].catching_up && !self.retry_catching_up) || self.down[m] {
            self.note(format!("r{r} not sent: {} is catching up or down", LETTERS[m]));
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
        if pre_record.is_none() {
            self.ghost.fps.remove(&(engine.node_id(), c, req.key));
        }
        let (refused_pre, contested_pre) = self.refuses(m, r)?;
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
        let mut row_before = None;
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
            Kind::IncSingle { d } => {
                Call::Inc { target: Some((i64::from(d), i64::from(d))), stop: Some(1) }
            }
            Kind::IncRange { d } => {
                Call::Inc { target: Some((i64::from(d), i64::from(d) + 1)), stop: None }
            }
            Kind::IncBig { d } => {
                Call::Inc { target: Some((i64::from(d), i64::from(d))), stop: Some(1) }
            }
            Kind::Replace { at } => {
                let target = target_id(at);
                let (ks, at) = base_of(&target);
                let body = with_ks(doc! { "v": rid, "req": rid, "rep": true }, &appended(&ks));
                base.push((target.clone(), ks, at));
                Call::Replace(target, body)
            }
            Kind::Delete { at } => {
                let target = target_id(at);
                row_before = stored(&engine, c, &target).map(|(stamp, _)| stamp);
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
            row_before,
            floor,
            last_before,
            on_own_effect: Arc::new(Mutex::new(Vec::new())),
            phase: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            phase_seq: Rc::new(RefCell::new(vec![self.ghost.seq])),
            refused_pre,
            contested_pre,
            refusing: Rc::new(RefCell::new(Vec::new())),
            i8_at: Rc::new(RefCell::new(None)),
        }))
    }

    /// Classify an application of request `r` on member `m` whose base already holds
    /// it (I1): the relay class (the member held no record of it and had taken no entry
    /// of it), or a violation. No drop explains one: a record outlives a drop of its
    /// collection for its window, so a retry after a drop replays.
    fn on_own_effect(
        &mut self,
        p: &Prepared,
        id: &DocId,
        ks: &[i64],
        phase: usize,
        minted: &HashSet<Stamp>,
    ) {
        // What the member had taken by the phase it wrote in.
        let taken_by = p.phase_seq.borrow().get(phase).copied().unwrap_or(u64::MAX);
        let r = p.r;
        let c = self.sched.requests[r as usize].kind.coll();
        let key = crate::docs::doc_key(id).unwrap_or_default();
        let processed_before = self.ghost.processed.iter().any(|((n, s), seq)| {
            *n == p.node
                && *seq <= taken_by
                && !minted.contains(s)
                && self.ghost.entries.get(s).is_some_and(|(q, ..)| *q == r)
        });
        let detail = format!(
            "r{r} applied on {} to {id:?} whose key set {ks:?} already holds it (live record \
             before: {:?}; an entry of r{r} processed there before: {processed_before}; its \
             greatest last {:?}; the member's floor {})",
            LETTERS[p.m],
            p.pre_record.as_ref().map(|rec| (
                self.stamp(rec.first),
                self.stamp(rec.last),
                rec.state
            )),
            p.last_before.map(|l| self.stamp(l)),
            self.hlc(p.floor)
        );
        if p.pre_record.is_none() && !processed_before {
            self.out.relay += 1;
            self.ghost.explained.insert((c, key, r));
            self.note(format!("RELAY: {detail}"));
            if self.strict {
                self.fail(Inv::I1, format!("relay (strict): {detail}"));
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
        self.check_answer_algebra(&p, &answer, &minted)?;
        // Applications on their own effect: seen by the `$inc` where they happened, or
        // an insert, replace or delete that wrote on a base holding it.
        let seen: Vec<(DocId, Vec<i64>, usize)> =
            std::mem::take(&mut *p.on_own_effect.lock().unwrap());
        for (id, ks, phase) in &seen {
            self.on_own_effect(&p, id, ks, *phase, &minted_set);
        }
        // An insert, replace or delete whose write landed (answered or not: a failed
        // fsync's may have) on a base already holding the request.
        if !req.kind.is_inc() {
            for (id, ks, _) in p.base.clone() {
                let landed = minted.iter().any(|e| e.doc_id.as_ref() == Some(&id));
                if landed && ks.contains(&i64::from(r)) {
                    self.on_own_effect(&p, &id, &ks, 0, &minted_set);
                }
            }
        }
        for entry in &minted {
            let id = entry.doc_id.clone().expect("a document entry");
            let key = crate::docs::doc_key(&id).unwrap_or_default();
            self.ghost.entries.insert(entry.stamp, (r, c, id.clone()));
            self.ghost.mark_processed(p.node, entry.stamp);
            // Writes across every life of the collection: a record outlives a drop
            // for its window, so a retry after one replays.
            let earlier: Vec<Stamp> =
                self.ghost.writes.get(&(p.node, r, c, key.clone())).cloned().unwrap_or_default();
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
        // One call's own writes on both sides of a drop of `orders` made anywhere.
        if c == ORDERS
            && self.ghost.drops.iter().any(|(dc, at)| {
                *dc == ORDERS
                    && minted.iter().any(|e| e.stamp.hlc <= at.hlc)
                    && minted.iter().any(|e| e.stamp.hlc > at.hlc)
            })
        {
            self.out.call_straddles += 1;
        }
        if c == ORDERS && self.floor(m, c)? > p.floor {
            self.out.drop_mid_request += 1;
            self.note(format!(
                "r{r} on {}: a drop of orders reached it during the call",
                LETTERS[m]
            ));
        }
        self.check_replay(&p, &answer);
        match answer {
            Answer::Fresh { wrote, resumed } => {
                if resumed {
                    self.out.resumes += 1;
                }
                if req.kind.is_inc() {
                    // At completion: the state the last commit point read before its
                    // steps, when it ran any; else as it stands.
                    let at_last = p.i8_at.borrow_mut().take();
                    match at_last {
                        Some(state) => self.check_i8_on(r, m, state)?,
                        None => self.check_i8(r, m)?,
                    }
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
                // Refused because its key names another request, or two (a contested
                // record), or stopped between chunks because its record turned contested:
                // nothing more can happen to it, every later retry is refused, and the
                // home track's drive must not wait for it.
                self.finished[r as usize] =
                    matches!(e, StorageError::IdempotencyKeyReused) || stopped_contested(&e);
                self.note(format!("r{r} on {}: {e}, minted [{shown}]", LETTERS[m]));
            }
        }
        Ok(())
    }

    /// The contested algebra's answers to a call of request `r` on `m`: a
    /// local commit adds `r` to the record's set; a lookup refuses (422) exactly where
    /// the record before the call is contested or another request's, writing nothing;
    /// and a multi-chunk request writes no chunk after a commit point whose steps left
    /// its record contested, its answer then `partially_applied` with the refusal.
    fn check_answer_algebra(
        &mut self,
        p: &Prepared,
        answer: &Answer,
        minted: &[kimmy_core::OplogEntry],
    ) -> Res<()> {
        let (r, m) = (p.r, p.m);
        let req = self.sched.requests[r as usize];
        let points = p.phase.load(std::sync::atomic::Ordering::SeqCst);
        let after = self.live_record(m, req.kind.coll(), req.key)?;
        // Whether the call stored its record: a commit point fired (each one added `r`
        // already), an entry landed (a failed fsync's may have), or a record-only commit
        // whose outcome was unknown left the record its own.
        let wrote = points > 0
            || !minted.is_empty()
            || (after.as_ref().is_some_and(|rec| rec.fingerprint == p.keyed.fingerprint)
                && after != p.pre_record);
        if wrote && !p.refused_pre {
            self.fps_own(m, r);
        }
        let lookup_refused = matches!(answer, Answer::Failed(StorageError::IdempotencyKeyReused));
        if p.refused_pre {
            if lookup_refused && minted.is_empty() {
                self.out.contested_refused += usize::from(p.contested_pre);
            } else {
                let s = format!(
                    "r{r} on {}: the algebra says its lookup refuses (the record before the call \
                     contested: {}, another request's: {}), and it answered {answer:?}, minting {}",
                    LETTERS[m],
                    p.contested_pre,
                    p.pre_record.as_ref().is_some_and(|rec| rec.fingerprint != fingerprint(r)),
                    minted.len()
                );
                self.fail(Inv::Contested, s);
            }
        } else if lookup_refused {
            let s = format!(
                "r{r} on {} was refused at its lookup, where the algebra holds the record under \
                 its key uncontested and its own, or none ({:?})",
                LETTERS[m],
                p.pre_record.as_ref().map(|rec| (self.stamp(rec.first), rec.contested))
            );
            self.fail(Inv::Contested, s);
        }
        let refusing = p.refusing.borrow().clone();
        for (i, refuses) in refusing.iter().enumerate() {
            if *refuses && i + 1 < refusing.len() {
                let s = format!(
                    "r{r} on {} committed again after its commit point {}, whose steps left its \
                     record contested",
                    LETTERS[m],
                    i + 1
                );
                self.fail(Inv::Contested, s);
            }
        }
        if let Answer::Failed(e) = answer
            && stopped_contested(e)
        {
            if refusing.last() == Some(&true) {
                self.out.contested_stopped += 1;
            } else {
                let s = format!(
                    "r{r} on {} stopped partially applied as contested, where the algebra holds \
                     its record uncontested after its last commit point ({refusing:?})",
                    LETTERS[m]
                );
                self.fail(Inv::Contested, s);
            }
        }
        Ok(())
    }

    /// Life (records outlive a drop): a request whose member held its `done` record, alive
    /// by its window alone (a drop of the collection since changes nothing), replays it. A
    /// delete by id replays while the row of its document is at or above the delete's
    /// stamp, or when the row is gone and the collection's drop or floor is at or above it;
    /// a row gone otherwise (retention collected the tombstone) lets it run afresh.
    fn check_replay(&mut self, p: &Prepared, answer: &Answer) {
        let Some(rec) = &p.pre_record else { return };
        // A contested record is refused, not replayed (the algebra's check).
        // A `done` record replays, and so does an `applying` one of any kind but a filter's
        // (a rewind reopens a cut insert, replace or delete by id; it replays).
        let filter = matches!(rec.outcome, crate::idempotency::RecordOutcome::Modify(_));
        let replays = rec.state == crate::idempotency::RecordState::Done || !filter;
        if p.refused_pre
            || rec.fingerprint != p.keyed.fingerprint
            || !replays
            || matches!(answer, Answer::Replayed(_))
        {
            return;
        }
        // A delete by id is judged by the record's `first`, the request's first write
        // (ADR-240): its tombstone or a version over it stands at or above it, or the row is
        // gone and a drop or floor at or above it took it.
        if let crate::idempotency::RecordOutcome::Delete { stamp: Some(_), .. } = &rec.outcome {
            let stands = match p.row_before {
                Some(row) => row >= rec.first,
                None => p.floor >= rec.first.hlc,
            };
            if !stands {
                return;
            }
        }
        let s = format!(
            "r{} on {} held its done record (first {} last {}, the member's floor {}) and \
             answered {answer:?}, not its replay",
            p.r,
            LETTERS[p.m],
            self.stamp(rec.first),
            self.stamp(rec.last),
            self.hlc(p.floor)
        );
        self.fail(Inv::Life, s);
    }

    /// I8, when `$inc` request `r` completes on member `m` (not a replay): every counter
    /// it targets that `m` holds in a version older than the request's merged `first`
    /// there holds its application. The stamp rule writes exactly those, and leaves a
    /// match stored at or above the merged `first` (by design, counted in `skipped`): a
    /// version written meanwhile by another, or a later life's document after a drop
    /// (records outlive a drop; counted here as `left_by_first`).
    ///
    /// A contested key completed on, a failure: the merged `first` is another request's
    /// under the same key (a key reused for another request, applied on a member without
    /// the first one's record, the two records merged). Such a record is `contested` and
    /// answers 422 on every path; completing on it reads the other request's `first`.
    fn check_i8(&mut self, r: u8, m: usize) -> Res<()> {
        let state = self.i8_state(r, m)?;
        self.check_i8_on(r, m, state)
    }

    /// What I8 reads of `m` for request `r`: the floor of `orders`, the live record
    /// under the key, and the stored rows of the counters, as they stand now.
    fn i8_state(&self, r: u8, m: usize) -> Res<I8State> {
        let floor = self.floor(m, ORDERS)?;
        let key = self.sched.requests[r as usize].key;
        let record = self.live_record(m, ORDERS, key)?;
        let e = self.e(m);
        let counters = (0..COUNTERS).map(|d| stored(&e, ORDERS, &DocId::Int64(d))).collect();
        Ok(I8State { floor, record, counters })
    }

    /// I8 on `state`, read at the request's completion: right after its last commit,
    /// before the steps run there (a document or a record a step brings after the
    /// request ended is no part of what it completed on).
    fn check_i8_on(&mut self, r: u8, m: usize, state: I8State) -> Res<()> {
        let I8State { floor, record, counters } = state;
        let Some(record) = record else { return Ok(()) };
        self.out.i8_checks += 1;
        let first = record.first;
        let first_of_another = self.ghost.entries.get(&first).map(|(q, ..)| *q).filter(|q| *q != r);
        if let Some(q) = first_of_another {
            self.out.kr += 1;
            let s = format!(
                "KR: r{r} completed on {} on a record whose first {} is r{q}'s",
                LETTERS[m],
                self.stamp(first)
            );
            self.note(s.clone());
            self.fail(Inv::I8, s);
            return Ok(());
        }
        let kind = self.sched.requests[r as usize].kind;
        for d in 0..COUNTERS {
            if !kind.targets(d) {
                continue;
            }
            // A version at or below the floor is of the ended life, swept by the drop.
            let Some((stamp, Some(doc))) = counters[d as usize].clone() else { continue };
            if stamp.hlc <= floor || ks_of(&doc).contains(&i64::from(r)) {
                continue;
            }
            if stamp >= first {
                self.out.left_by_first += 1;
                continue;
            }
            let s = format!(
                "r{r} completed on {} and left counter {d} at {} ({doc}) without it, below its \
                 merged first {} (record last {}, {:?}; the member's floor {})",
                LETTERS[m],
                self.stamp(stamp),
                self.stamp(first),
                self.stamp(record.last),
                record.state,
                self.hlc(floor),
            );
            self.fail(Inv::I8, s);
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
            // A drop of `orders` took the request's collection from its home: nothing to
            // drive it on until a creation reaches it.
            let c = self.sched.requests[r as usize].kind.coll();
            if self.e(home).get_collection(DB, NAMES[c as usize]).is_err() {
                return Ok(());
            }
            self.out.steps += 1;
            let t = self.latest_t(home);
            self.note(format!("drive r{r} on {}", LETTERS[home]));
            self.send_plain(r, home, t)?;
        }
        let c = self.sched.requests[r as usize].kind.coll();
        if !self.finished[r as usize] && self.e(home).get_collection(DB, NAMES[c as usize]).is_ok()
        {
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
            Step::Page { to, from, c, converge, restart, inner } => {
                let (to, from) = (*to as usize, *from as usize);
                if self.down[to] || self.down[from] {
                    self.note("no page: a member is down");
                } else if self.e(from).get_collection(DB, NAMES[*c as usize]).is_err() {
                    self.note(format!("{} lacks {}: no page", LETTERS[from], NAMES[*c as usize]));
                } else {
                    let cut = converge.then(kimmy_core::VersionVector::new);
                    self.transfer(to, from, Some(coll_id(*c)), cut, inner)?;
                }
                if *restart && !self.down[to] {
                    self.reopen(to, true)?;
                }
            }
            Step::Reset { m, from, inner } => self.reset(*m as usize, *from as usize, inner)?,
            Step::Touch { m, c, d } => self.touch(*m as usize, *c, *d, op.t)?,
            Step::Down { m } => {
                self.down[*m as usize] = true;
                self.note(format!("{} is down", LETTERS[*m as usize]));
            }
            Step::Up { m } => {
                self.down[*m as usize] = false;
                self.note(format!("{} is up", LETTERS[*m as usize]));
            }
            Step::Backup { m } => {
                let m = *m as usize;
                let mut bytes = Vec::new();
                let e = self.e(m);
                e.backup_to(&mut bytes, WalkScope::Background).map_err(|e| err("backup", e))?;
                let node = e.node_id();
                let fps = self
                    .ghost
                    .fps
                    .iter()
                    .filter(|((n, ..), _)| *n == node)
                    .map(|((_, c, key), set)| ((*c, *key), set.clone()))
                    .collect();
                let witnessed = e.witnessed_vector().map_err(|e| err("witnessed_vector", e))?;
                self.backups.entry(m).or_default().push(Backup { node, bytes, fps, witnessed });
            }
            Step::Restore { m, nth, inner } => self.restore(*m as usize, *nth, inner)?,
            Step::Rewind { m } => self.rewind(*m as usize, op.t)?,
            Step::RewindAlone { m } => self.rewind_alone(*m as usize, op.t)?,
            Step::Orphan { m } => self.orphan(*m as usize)?,
            Step::Big { m } => {
                let m = *m as usize;
                let e = self.e(m);
                let big =
                    e.create_collection(DB, NAMES[BIG as usize]).map_err(|e| err("big", e))?;
                let docs: Vec<Document> = (0..BIG_DOCS)
                    .map(|i| doc! { "_id": i, "n": 0i64, "ks": Vec::<Bson>::new() })
                    .collect();
                e.insert_many(&big, docs).map_err(|e| err("big", e.source))?;
            }
            Step::Restart { m } => self.reopen(*m as usize, true)?,
            Step::Crash { m } => self.reopen(*m as usize, false)?,
            Step::Drop { m, c } => {
                let (m, c) = (*m as usize, *c);
                self.clock(m, op.t);
                match self.e(m).drop_collection(DB, NAMES[c as usize]) {
                    Ok(held) => {
                        let e = self.e(m);
                        let at = e
                            .collection_dropped_at(coll_id(c))
                            .map_err(|e| err("dropped_at", e))?;
                        if let Some(at) = at {
                            self.ghost.drops.push((c, at));
                        }
                        let s =
                            format!("{} dropped {} (held {held})", LETTERS[m], NAMES[c as usize]);
                        self.note(s)
                    }
                    Err(e) => self.note(format!("{} drop refused: {e}", LETTERS[m])),
                }
            }
            Step::Create { m, c } => {
                let (m, c) = (*m as usize, *c);
                self.clock(m, op.t);
                for attempt in 0..2 {
                    match self.e(m).create_collection(DB, NAMES[c as usize]) {
                        Ok(coll) => {
                            self.note(format!("{} created {}", LETTERS[m], NAMES[c as usize]));
                            // A life of `orders` begins with its counters, fresh.
                            if c == ORDERS {
                                self.e(m)
                                    .insert_many(&coll, fresh_counters())
                                    .map_err(|e| err("insert_many", e.source))?;
                            }
                        }
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
                for j in 0..self.n() {
                    self.e(j).set_idempotency_clock_for_test(Some(now));
                    self.fps_expire(j)?;
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

    /// Restore `m` from a backup taken under its identity (the latest, or the `nth` before
    /// it), then replay its own origin from every member that is up, with `inner` steps
    /// between the replay's pages.
    fn restore(&mut self, m: usize, nth: u8, inner: &[(u8, Vec<Op>)]) -> Res<()> {
        let held = self.backups.get(&m).cloned().unwrap_or_default();
        let Some(backup) = held.iter().rev().nth(nth as usize).cloned() else {
            self.note(format!("{} has no such backup: no restore", LETTERS[m]));
            return Ok(());
        };
        let node = backup.node;
        if node != self.e(m).node_id() {
            self.note(format!("{} was reset since its backup: no restore", LETTERS[m]));
            return Ok(());
        }
        let arc = self.members[m].engine.take().expect("open");
        drop(Arc::into_inner(arc).ok_or("the engine is held elsewhere")?);
        let path = self.members[m].path.clone();
        let dir = path.parent().expect("a member's directory").to_path_buf();
        for entry in std::fs::read_dir(&dir).map_err(|e| err("read_dir", e))? {
            let entry = entry.map_err(|e| err("read_dir", e))?;
            std::fs::remove_file(entry.path()).map_err(|e| err("remove", e))?;
        }
        crate::backup::restore(&path, &mut backup.bytes.as_slice())
            .map_err(|e| err("restore", e))?;
        self.open(m)?;
        self.note(format!("{} restored from its backup", LETTERS[m]));
        // Coverage after a restore: never more of an origin than the store held when
        // the backup was taken. Finding BR1 (fixed by #211): a restore counted the rows a
        // page left as held state as covered, so the member's pulls started past writes
        // it never took, their records with them.
        // Its own origin aside: what a member wrote itself is in position, and the
        // replay of its own origin reads back what it lost.
        let now = self.e(m).witnessed_vector().map_err(|e| err("witnessed_vector", e))?;
        for (origin, at) in now.iter() {
            let then = backup.witnessed.get(origin);
            if origin != node && at > then {
                let s = format!(
                    "{}'s restore claims {}'s origin through {}, past the {} its backup held \
                     (BR1)",
                    LETTERS[m],
                    self.label(origin),
                    self.hlc(at),
                    self.hlc(then)
                );
                self.fail(Inv::I2, s);
            }
        }
        // The algebra's sets are the backup's: what the store held then.
        self.ghost.fps.retain(|(n, ..), _| *n != node);
        for ((c, key), set) in backup.fps {
            self.ghost.fps.insert((node, c, key), set);
        }
        self.fps_expire(m)?;
        self.after_losing_history(m)?;
        self.replay_from_peers(m, inner)
    }

    /// What `m` took and wrote and no longer holds (a restore, a rewind) is gone with its
    /// store; its own writes past where its store now leaves off, which a peer holds only
    /// as page state, never come back (R1).
    fn after_losing_history(&mut self, m: usize) -> Res<()> {
        // Until its coverage of every other origin has caught up again, it may hold an
        // effect ahead of its record (see `check_effects`).
        self.members[m].restored_behind = true;
        let e = self.e(m);
        let node = e.node_id();
        let at = e.own_position_at_open();
        let held = self.restored.entry(node).or_insert(at);
        *held = (*held).min(at);
        let mut gone = Vec::new();
        for (n, stamp) in self.ghost.processed.keys() {
            if *n == node && !e.has_oplog_entry(stamp).map_err(|e| err("has_oplog_entry", e))? {
                gone.push(*stamp);
            }
        }
        for stamp in &gone {
            self.ghost.processed.remove(&(node, *stamp));
        }
        for ((n, ..), written) in self.ghost.writes.iter_mut() {
            if *n == node {
                written.retain(|stamp| !gone.contains(stamp));
            }
        }
        Ok(())
    }

    /// The replay of `m`'s own origin from every member that is up, as the transport
    /// starts it after a restore (ADR-212), with `inner` steps after the `j`-th page of
    /// them all.
    fn replay_from_peers(&mut self, m: usize, inner: &[(u8, Vec<Op>)]) -> Res<()> {
        let mut pages = 0usize;
        for peer in 0..self.n() {
            if peer != m && !self.down[peer] {
                self.replay_own_origin(m, peer, inner, &mut pages)?;
            }
        }
        // Steps whose page never came run after the replay.
        for (j, ops) in inner {
            if usize::from(*j) > pages {
                for op in ops {
                    self.note(format!("after the replay: {op}"));
                    self.apply_between_replay_pages(op, m, None)?;
                }
            }
        }
        Ok(())
    }

    /// Every record `m` stores, by (collection id, scope), as it stands, and the rows by
    /// stamp that name no record (orphans).
    fn stored_records(&self, m: usize) -> Res<(StoredRecords, BTreeSet<Vec<u8>>)> {
        use redb::{ReadableDatabase, ReadableTable};
        let e = self.e(m);
        let txn = e.db().begin_read().map_err(|e| err("begin_read", e))?;
        let records = txn.open_table(tables::IDEMPOTENCY).map_err(|e| err("records", e))?;
        let mut out = BTreeMap::new();
        for row in records.iter().map_err(|e| err("records", e))? {
            let (key, value) = row.map_err(|e| err("records", e))?;
            let (id, scope) = key.value();
            let Some(scope) = ScopeHash::from_slice(scope) else { continue };
            if let Ok(Some(record)) = IdempotencyRecord::decode(value.value()) {
                out.insert((id, scope.0), record);
            }
        }
        let rows = txn.open_table(tables::IDEMPOTENCY_BY_STAMP).map_err(|e| err("rows", e))?;
        let mut orphans = BTreeSet::new();
        for row in rows.iter().map_err(|e| err("rows", e))? {
            let (key, value) = row.map_err(|e| err("rows", e))?;
            let value = value.value();
            if value.len() != 40 {
                continue;
            }
            let id = u64::from_be_bytes(value[..8].try_into().expect("eight bytes"));
            let scope: [u8; 32] = value[8..].try_into().expect("32 bytes");
            if records.get((id, scope.as_slice())).map_err(|e| err("records", e))?.is_none() {
                orphans.insert(key.value().to_vec());
            }
        }
        Ok((out, orphans))
    }

    /// A rewind of `m` to `t` (`kimmyd restore --until`), offline, then the replay of its
    /// own origin a restore starts. Right after it, before any replay: no record of a
    /// request whose every write it discarded stands (`first` after the target), a record
    /// it cut part-way goes as a bulk's and stands reopened `applying` as any other kind's
    /// (a filter's with no cursor), and it leaves no row by stamp naming a record it
    /// removed.
    fn rewind(&mut self, m: usize, t: u32) -> Res<()> {
        // The documented procedure (operations, "Point-in-time restore"): a rewound
        // database never rejoins a cluster that still holds the writes it undid. A member
        // is rewound alone only when no peer holds any entry it discards; otherwise every
        // member is rewound to the same target.
        let until = Self::until(self.base, t);
        let shared = self.discards_shared(m, until)?;
        if shared.is_empty() {
            self.out.rewinds_alone += 1;
            if self.rewind_to(m, until, t)? {
                self.rewound(m, until, t)?;
                self.replay_from_peers(m, &[])?;
            }
            return Ok(());
        }
        if self.sched.track == Track::Home {
            // Every member rewound would undo `$inc`s the home answered, which the home
            // track's exact count does not allow for (as it never restores the home).
            self.out.rewinds_skipped += 1;
            self.note(format!("{}'s rewind would widen to the home: not taken", LETTERS[m]));
            return Ok(());
        }
        self.out.rewinds_widened += 1;
        self.note(format!(
            "{}'s rewind to t{t} discards entries a peer holds ({}): every member is rewound",
            LETTERS[m],
            shared.iter().map(|(s, ..)| self.stamp(*s)).collect::<Vec<_>>().join(" ")
        ));
        self.rewind_every_member(until, t)
    }

    /// The forbidden procedure, for the row that shows it: `m` rewound alone although a
    /// peer holds what it discards, then rejoining (`Step::RewindAlone`).
    fn rewind_alone(&mut self, m: usize, t: u32) -> Res<()> {
        let until = Self::until(self.base, t);
        let shared = self.discards_shared(m, until)?;
        if self.rewind_to(m, until, t)? {
            self.undone.extend(shared.into_iter().map(|(stamp, c, id)| (m, stamp, c, id)));
            self.rewound(m, until, t)?;
            self.replay_from_peers(m, &[])?;
        }
        Ok(())
    }

    /// The first stamp minted in millisecond `t` is kept, the rest of it discarded: a
    /// batch of that millisecond is cut part-way (`--until` is a millisecond).
    fn until(base: u64, t: u32) -> Hlc {
        Hlc::new(base + u64::from(t), 1)
    }

    /// The entries a rewind of `m` to `until` discards that a peer holds: in its oplog,
    /// or as the document's row at that stamp (a page's state).
    fn discards_shared(&self, m: usize, until: Hlc) -> Res<Vec<(Stamp, u8, DocId)>> {
        let discarded: Vec<kimmy_core::OplogEntry> = self
            .e(m)
            .read_oplog_from(until, 100_000, WalkScope::Request)
            .map_err(|e| err("read_oplog_from", e))?
            .into_iter()
            .filter(|entry| entry.stamp.hlc > until)
            .collect();
        let mut shared = Vec::new();
        for entry in &discarded {
            let Some(c) = (0..NAMES.len() as u8).find(|c| coll_id(*c) == entry.collection) else {
                continue;
            };
            let Some(id) = entry.doc_id.clone() else { continue };
            let held = (0..self.n()).filter(|peer| *peer != m).any(|peer| {
                let e = self.e(peer);
                e.has_oplog_entry(&entry.stamp).unwrap_or(false)
                    || stored(&e, c, &id).is_some_and(|(at, _)| at == entry.stamp)
            });
            if held {
                shared.push((entry.stamp, c, id));
            }
        }
        Ok(shared)
    }

    /// Every member rewound to `until`, as the procedure asks of a rewind whose undone
    /// writes a peer holds: all or none (a member that refuses leaves every store as it
    /// was), then each one's checks, then their replays.
    fn rewind_every_member(&mut self, until: Hlc, t: u32) -> Res<()> {
        let n = self.n();
        // Each store as it stands, closed, so a refusal puts every one back.
        let mut saved = Vec::new();
        for m in 0..n {
            let arc = self.members[m].engine.take().expect("open");
            let engine = Arc::into_inner(arc).ok_or("the engine is held elsewhere")?;
            engine.close().map_err(|e| format!("close: {e:?}"))?;
            let copy = self.members[m].path.with_extension("before-rewind");
            std::fs::copy(&self.members[m].path, &copy).map_err(|e| err("copy", e))?;
            saved.push(copy);
            self.open(m)?;
        }
        let mut befores = Vec::new();
        for m in 0..n {
            let before = self.stored_records(m)?;
            // Bound first: the engine's handle must be let go before a refusal reopens it.
            let past_a_drop = self.rewinds_past_a_drop(m, until)?;
            let rewound = self.e(m).rewind_to(until);
            match rewound {
                Ok(outcome) => {
                    self.out.rewinds_across_a_drop += usize::from(past_a_drop);
                    befores.push((before, outcome))
                }
                Err(e) => {
                    self.note_rewind_refusal(&e);
                    self.note(format!(
                        "{} refuses the rewind to t{t} ({e}): no member is rewound",
                        LETTERS[m]
                    ));
                    for (m, copy) in saved.iter().enumerate() {
                        let arc = self.members[m].engine.take().expect("open");
                        drop(Arc::into_inner(arc).ok_or("the engine is held elsewhere")?);
                        std::fs::copy(copy, &self.members[m].path).map_err(|e| err("copy", e))?;
                        self.open(m)?;
                    }
                    return Ok(());
                }
            }
        }
        for (m, (before, outcome)) in befores.into_iter().enumerate() {
            self.reopen(m, true)?;
            self.check_rewound(m, until, t, before, outcome)?;
        }
        for m in 0..n {
            self.replay_from_peers(m, &[])?;
        }
        Ok(())
    }

    /// Rewind `m` alone; whether it was (a refusal writes nothing).
    fn rewind_to(&mut self, m: usize, until: Hlc, t: u32) -> Res<bool> {
        let before = self.stored_records(m)?;
        let past_a_drop = self.rewinds_past_a_drop(m, until)?;
        let rewound = self.e(m).rewind_to(until);
        let outcome = match rewound {
            Ok(outcome) => outcome,
            Err(e) => {
                self.note_rewind_refusal(&e);
                self.note(format!("{} rewind to t{t} refused: {e}", LETTERS[m]));
                return Ok(false);
            }
        };
        self.out.rewinds_across_a_drop += usize::from(past_a_drop);
        self.reopen(m, true)?;
        self.pending_rewind = Some((before, outcome));
        Ok(true)
    }

    /// Whether a rewind of `m` to `until` lies past a drop `m` holds (a collection's drop
    /// tombstone or incarnation floor at or below the target): one that judges entries
    /// across a life the drop ended, and puts none of its versions back.
    fn rewinds_past_a_drop(&self, m: usize, until: Hlc) -> Res<bool> {
        for c in [ORDERS, ITEMS] {
            let floor = self.floor(m, c)?;
            if floor != Hlc::ZERO && floor <= until {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Count a rewind's refusal by its kind: a document whose value at the target this
    /// member does not hold (retention collected it, only a page's state holds it, or its
    /// entries at or below the target are of a life a drop ended, so a later write that
    /// replaces it has nothing to go back to), or a schema change after the target (a
    /// drop, including one known only as a page's state).
    fn note_rewind_refusal(&mut self, e: &StorageError) {
        let text = e.to_string();
        if text.contains("changed after the target") {
            self.out.rewind_refused_unrecoverable += 1;
        } else if text.contains("schema change") {
            self.out.rewind_refused_schema += 1;
        }
    }

    /// The checks of a rewind `rewind_to` made.
    fn rewound(&mut self, m: usize, until: Hlc, t: u32) -> Res<()> {
        let (before, outcome) = self.pending_rewind.take().expect("a rewind made");
        self.check_rewound(m, until, t, before, outcome)
    }

    /// Right after a rewind of `m`, before any replay: no record of a request whose every
    /// write it discarded stands, a record it cut part-way goes as a bulk's and stands
    /// reopened as any other kind's, and it leaves no row by stamp naming a record it
    /// removed. Then the ghost forgets what the member no longer holds.
    fn check_rewound(
        &mut self,
        m: usize,
        until: Hlc,
        t: u32,
        before: RecordsAndOrphans,
        outcome: crate::rewind::RewindOutcome,
    ) -> Res<()> {
        let (before, orphans_before) = before;
        self.out.rewinds += 1;
        self.out.rewound_removed += outcome.records_removed;
        self.out.rewound_reopened += outcome.records_reopened;
        let (after, orphans_after) = self.stored_records(m)?;
        self.note(format!(
            "{} rewound to t{t}: {} entries discarded, {} records removed, {} reopened",
            LETTERS[m], outcome.oplog_discarded, outcome.records_removed, outcome.records_reopened
        ));
        for (key, was) in &before {
            let now = after.get(key);
            let cut = was.last.hlc > until;
            // Only a cut bulk insert goes; every other kind cut
            // part-way is reopened, its rows kept.
            let bulk = matches!(was.outcome, crate::idempotency::RecordOutcome::Bulk { .. });
            if cut && bulk && was.first.hlc <= until {
                // Every request merged into the record (a contested one holds two or
                // more): the writes of each that stand are bare.
                let r = was.fingerprint.0[0].wrapping_sub(1);
                let mut merged = BTreeSet::from([r]);
                if let Some(c) = (0..NAMES.len() as u8).find(|c| coll_id(*c).0 == key.0) {
                    merged.extend(self.fps(m, c, key.1[0]));
                }
                for q in &merged {
                    self.ghost.bare.insert(*q);
                }
                self.note(format!("{merged:?}'s record removed by the rewind, what stands bare"));
            }
            let fine = if was.first.hlc > until || (cut && bulk) {
                now.is_none()
            } else if cut {
                now.is_some_and(|now| {
                    let mut open = was.clone();
                    open.state = crate::idempotency::RecordState::Applying;
                    if let crate::idempotency::RecordOutcome::Modify(o) = &mut open.outcome {
                        o.cursor = None;
                    }
                    *now == open
                })
            } else {
                now == Some(was)
            };
            if !fine {
                let s = format!(
                    "{}'s rewind to t{t} left key {} of collection {:x} as {:?}, from first {} \
                     last {} {:?}",
                    LETTERS[m],
                    key.1[0],
                    key.0,
                    now.map(|r| (self.stamp(r.first), self.stamp(r.last), r.state)),
                    self.stamp(was.first),
                    self.stamp(was.last),
                    was.state
                );
                self.fail(Inv::Life, s);
            }
        }
        for row in orphans_after.difference(&orphans_before) {
            let at = crate::idempotency::decode_row_key(row).map(|s| self.stamp(s));
            self.fail(
                Inv::I2,
                format!("{}'s rewind left a row by stamp at {at:?} naming none", LETTERS[m]),
            );
        }
        self.fps_expire(m)?;
        self.after_losing_history(m)
    }

    /// Orphan rows on `m`: every record past its window is removed and its rows by stamp
    /// are left, as a retention pass stopped between its chunks leaves them. Every path
    /// passes over them ([`Self::check_carried`]).
    fn orphan(&mut self, m: usize) -> Res<()> {
        let mut removed = 0;
        for (c, key) in self.keys() {
            let scope = ScopeHash([key; 32]);
            let e = self.e(m);
            let Some(record) =
                e.idempotency_record(coll_id(c), &scope).map_err(|e| err("record", e))?
            else {
                continue;
            };
            if self.past_window(&record)
                && e.remove_idempotency_record_for_test(coll_id(c), &scope)
                    .map_err(|e| err("remove_idempotency_record_for_test", e))?
            {
                removed += 1;
            }
        }
        self.out.orphaned += removed;
        self.note(format!(
            "{}: {removed} records past their window removed, their rows left",
            LETTERS[m]
        ));
        Ok(())
    }

    /// A step run between two pages of `m`'s replay: never one that closes, sends to or
    /// pulls into the member replaying (it is marked restored until it has caught up).
    fn apply_between_replay_pages(&mut self, op: &Op, m: usize, sender: Option<usize>) -> Res<()> {
        // A restore of [`SENDER`]: of the member that served the page just applied.
        let resolved;
        let op = match (&op.step, sender) {
            (Step::Restore { m: SENDER, nth, inner }, Some(sender)) => {
                if self.sched.track == Track::Home && sender == self.sched.home as usize {
                    self.note("skipped between replay pages: the sender is the home");
                    return Ok(());
                }
                let step = Step::Restore { m: sender as u8, nth: *nth, inner: inner.clone() };
                resolved = Op { step, t: op.t };
                &resolved
            }
            (Step::Restore { m: SENDER, .. }, None) => {
                self.note("skipped after the replay: no page's sender to restore");
                return Ok(());
            }
            _ => op,
        };
        let skip = match &op.step {
            Step::Send { m: at, .. } | Step::Touch { m: at, .. } => *at as usize == m,
            Step::Pull { to, .. } | Step::PullShort { to, .. } | Step::Page { to, .. } => {
                *to as usize == m
            }
            Step::Backup { m: at } | Step::Restore { m: at, .. } | Step::Clock { m: at } => {
                *at as usize == m
            }
            _ => true,
        };
        if skip {
            self.note(format!("skipped between replay pages: {op}"));
            return Ok(());
        }
        self.out.between += 1;
        let saved = self.step.clone();
        self.step = format!("{saved} > {op}");
        let out = match &op.step {
            Step::Send { r, m, .. } => self.send_plain(*r, *m as usize, op.t),
            _ => self.apply(op),
        };
        self.step = saved;
        out
    }

    /// The replay of `m`'s own origin from `peer` after a restore (ADR-212), as the
    /// transport makes it: the peer serves `m`'s entries above where `m`'s store left
    /// off, every other origin passed over, and cuts each page as `cut_replay_page` does
    /// ([`Self::cut_replay_page`]); `m` folds each stamp into its clock, applies the
    /// entries it no longer holds with the page's records, and stores the records of a
    /// page with none it lacks all the same (`replay_apply`). `inner` steps run after the
    /// `j`-th page, counted in `pages` across the peers.
    fn replay_own_origin(
        &mut self,
        m: usize,
        peer: usize,
        inner: &[(u8, Vec<Op>)],
        pages: &mut usize,
    ) -> Res<()> {
        let me = self.e(m).node_id();
        let mut from = self.e(m).own_position_at_open();
        let mut cursor: Option<crate::ReplayRecords> = None;
        for _ in 0..64 {
            if self.down[peer] {
                return Ok(());
            }
            let (src, dst) = (self.e(peer), self.e(m));
            let ours = src.version_vector().map_err(|e| err("version_vector", e))?;
            if ours.get(me) <= from && cursor.is_none() {
                return Ok(());
            }
            let mut held = ours.clone();
            for (node, _) in ours.iter() {
                if node != me {
                    held.insert(node, Hlc::MAX);
                }
            }
            held.insert(me, from);
            if src.serve_verdict(&held).map_err(|e| err("serve_verdict", e))? != ServeVerdict::Serve
            {
                self.note(format!("{}'s replay from {}: refused", LETTERS[m], LETTERS[peer]));
                return Ok(());
            }
            let window = src
                .serve_entries_to_peer(from, BATCH, Some(&held), &[], None)
                .map_err(|e| err("serve_entries_to_peer", e))?;
            let Some(cut) = self.cut_replay_page(peer, me, from, window, cursor.take())? else {
                self.note(format!(
                    "{}'s replay from {}: the records do not fit one page, it stays",
                    LETTERS[m], LETTERS[peer]
                ));
                return Ok(());
            };
            self.check_carried(peer, &cut.records)?;
            if trace() {
                let s = format!(
                    "{}'s replay page from {} at {}: entries [{}] records [{}]{}",
                    LETTERS[m],
                    LETTERS[peer],
                    self.hlc(from),
                    cut.entries.iter().map(|e| self.stamp(e.stamp)).collect::<Vec<_>>().join(" "),
                    cut.records
                        .iter()
                        .map(|c| format!(
                            "key {} at {:?}",
                            c.scope.0[0],
                            c.carrying.map(|s| self.stamp(s))
                        ))
                        .collect::<Vec<_>>()
                        .join("; "),
                    if cut.replay_records.is_some() { " (records alone)" } else { "" }
                );
                self.note(s);
            }
            let own: Vec<kimmy_core::OplogEntry> = cut
                .entries
                .iter()
                .filter(|entry| entry.stamp.node == me && entry.stamp.hlc > from)
                .cloned()
                .collect();
            for entry in &own {
                dst.advance_clock_past(&entry.stamp);
            }
            let mut lost = Vec::new();
            for entry in &own {
                if !dst.has_oplog_entry(&entry.stamp).map_err(|e| err("has_oplog_entry", e))? {
                    lost.push(entry.clone());
                }
            }
            let before = self.live_before(m, &cut.records)?;
            if !lost.is_empty() || !cut.records.is_empty() {
                let outcome = dst
                    .apply_batch_carrying(&lost, &cut.records, RecordsVia::Replay)
                    .map_err(|e| err("apply_batch_carrying", e))?;
                if outcome.unknown.is_some() || outcome.purge_pending > 0 {
                    // The replay stalls (`ReplayStall`), and goes on at the next contact.
                    self.fps_carried(src.node_id(), m, before)?;
                    dst.finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
                    self.note(format!(
                        "{}'s replay from {} stalled at an unknown or purging collection",
                        LETTERS[m], LETTERS[peer]
                    ));
                    return Ok(());
                }
            }
            self.fps_carried(src.node_id(), m, before)?;
            if !lost.is_empty() {
                let s = format!(
                    "{} read back {} of its own entries from {} ({} records)",
                    LETTERS[m],
                    lost.len(),
                    LETTERS[peer],
                    cut.records.len()
                );
                self.note(s);
                // Taken by history, so I2 holds them to their records; one the peer held
                // the record of and did not send is the relay class at its source.
                for entry in &lost {
                    let Some((r, c, _)) = self.ghost_entry(entry) else {
                        continue;
                    };
                    self.ghost.mark_processed(me, entry.stamp);
                    let key = self.sched.requests[r as usize].key;
                    let carried = cut
                        .records
                        .iter()
                        .any(|w| w.collection == coll_id(c) && w.scope == ScopeHash([key; 32]));
                    if !carried && self.live_record(peer, c, key)?.is_some() {
                        self.ghost.g1b.insert((me, entry.stamp));
                    }
                }
            }
            drop((src, dst));
            *pages += 1;
            for (j, ops) in inner {
                if usize::from(*j) == *pages {
                    for op in ops {
                        self.apply_between_replay_pages(op, m, Some(peer))?;
                    }
                }
            }
            if let Some(next) = cut.replay_records {
                // A page of records alone: the position stays, the cursor goes on.
                self.out.records_alone += 1;
                cursor = Some(next);
                continue;
            }
            let after = own.last().map_or(cut.scanned_to, |entry| entry.stamp.hlc).max(from);
            if cut.exhausted || after == from {
                return Ok(());
            }
            from = after;
        }
        Err("a replay that never ended".into())
    }

    /// One page of a replay as the transport cuts it (`transport::cut_replay_page`), with
    /// the model's frame: `replay_frame` units, an entry or a record one each. The F3
    /// records and those of the page's own advance (`records_of_replay`), the spanning records of
    /// the page's collections read by the engine (`Engine::replay_page_records`, with the
    /// records cursor of an earlier page of records alone at this position), each once at
    /// the lowest carrying stamp among them; all of it when it fits, else the leading
    /// entries that fit with the records riding them, else a page of records alone in key
    /// order with a cursor naming the last key sent. `None`: what must ride does not fit.
    fn cut_replay_page(
        &mut self,
        peer: usize,
        their: NodeId,
        from: Hlc,
        window: crate::OplogWindow,
        cursor: Option<crate::ReplayRecords>,
    ) -> Res<Option<ReplayCut>> {
        type RecordKey = (u64, [u8; 32]);
        let key =
            |r: &crate::idempotency::CarriedRecord| -> RecordKey { (r.collection.0, r.scope.0) };
        let budget = self.replay_frame;
        let mut cursor = cursor.filter(|cursor| cursor.at == from);
        let mut entries = window.entries;
        let mut scanned_to = window.scanned_to;
        let mut exhausted = window.exhausted && window.passed_through.is_none();
        if let Some(cursor) = &cursor
            && (scanned_to > cursor.through
                || entries.last().is_some_and(|e| e.stamp.hlc > cursor.through))
        {
            entries.retain(|entry| entry.stamp.hlc <= cursor.through);
            scanned_to = cursor.through.max(entries.last().map_or(from, |entry| entry.stamp.hlc));
            exhausted = false;
        }
        let through = entries.last().map_or(scanned_to, |entry| entry.stamp.hlc).max(scanned_to);
        let mut collections: Vec<CollectionId> = entries.iter().map(|e| e.collection).collect();
        collections.sort();
        collections.dedup();
        let read = match self.e(peer).replay_page_records(
            &collections,
            from,
            through,
            cursor.as_ref(),
            self.sched.bias.delta_bytes,
        ) {
            Ok(read) => read,
            // A record the change log names as changed since the cursor's base, larger
            // than the delta's half of a page (refused by name): as the transport does, the
            // cursor is dropped and the position's records are read again from a fresh
            // base, which carries the record as an ordinary one on a page of records
            // alone, what earlier pages at the position sent with it.
            Err(StorageError::SnapshotRowTooLarge { .. }) => {
                self.out.delta_too_large += 1;
                self.out.cursor_dropped += 1;
                cursor = None;
                self.e(peer)
                    .replay_page_records(
                        &collections,
                        from,
                        through,
                        None,
                        self.sched.bias.delta_bytes,
                    )
                    .map_err(|e| err("replay_page_records", e))?
            }
            Err(e) => return Err(err("replay_page_records", e)),
        };
        if cursor.is_some() && !read.resumed {
            self.out.cursor_restarts += 1;
            self.note(format!(
                "{}'s records cursor at {} starts over",
                LETTERS[peer],
                self.hlc(from)
            ));
        }
        let passed: HashSet<RecordKey> = read.passed.iter().map(|(c, s)| (c.0, s.0)).collect();
        let changed: HashSet<RecordKey> = read.changed.iter().map(key).collect();
        // `records_of_replay`: the F3 records, and those with a row of the requester's
        // origin inside the page.
        let unanchored = window.unanchored.min(window.records.len());
        let mut lead = Vec::new();
        let mut others = Vec::new();
        for (i, record) in window.records.into_iter().enumerate() {
            if i < unanchored {
                lead.push(record);
            } else if record
                .carrying
                .is_some_and(|at| at.node == their && at.hlc > from && at.hlc <= through)
            {
                others.push(record);
            }
        }
        let mut rest: BTreeMap<RecordKey, crate::idempotency::CarriedRecord> = BTreeMap::new();
        for record in others.into_iter().chain(read.records).chain(read.changed) {
            let k = key(&record);
            if passed.contains(&k) {
                continue;
            }
            match rest.entry(k) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert(record);
                }
                std::collections::btree_map::Entry::Occupied(mut slot) => {
                    let held = slot.get_mut();
                    held.carrying = held.carrying.min(record.carrying);
                    if changed.contains(&k) {
                        held.record = record.record;
                    }
                }
            }
        }
        for record in &mut lead {
            if let Some(other) = rest.remove(&key(record)) {
                record.carrying = record.carrying.min(other.carrying);
            }
        }
        if read.resumed
            && !read.changed_whole
            && let Some(cursor) = cursor
        {
            let mut records = lead;
            records.extend(rest.into_values().filter(|record| changed.contains(&key(record))));
            if records.len() > budget {
                return Ok(self.unfit());
            }
            let next = crate::ReplayRecords { base: read.base, ..cursor };
            return Ok(Some(ReplayCut {
                entries: Vec::new(),
                records,
                scanned_to: from,
                exhausted: false,
                replay_records: Some(next),
            }));
        }
        let unanchored = lead.len();
        let mut all = lead;
        all.extend(rest.into_values());
        match fits(&entries, &all, unanchored, budget) {
            Fit::All => Ok(Some(ReplayCut {
                entries,
                records: all,
                scanned_to,
                exhausted,
                replay_records: None,
            })),
            Fit::Only(n) if n > 0 => {
                entries.truncate(n);
                let end = entries[n - 1].stamp;
                let records = all
                    .into_iter()
                    .enumerate()
                    .filter(|(i, record)| {
                        *i < unanchored || record.carrying.is_some_and(|at| at <= end)
                    })
                    .map(|(_, record)| record)
                    .collect();
                Ok(Some(ReplayCut {
                    entries,
                    records,
                    scanned_to: end.hlc,
                    exhausted: false,
                    replay_records: None,
                }))
            }
            _ => {
                if unanchored > budget || all.len() == unanchored {
                    return Ok(self.unfit());
                }
                let sent = budget.min(all.len());
                if sent <= unanchored {
                    return Ok(self.unfit());
                }
                all.truncate(sent);
                let last = &all[sent - 1];
                if read.resumed
                    && cursor
                        .as_ref()
                        .is_some_and(|cursor| cursor.passed(last.collection, &last.scope))
                {
                    return Ok(self.unfit());
                }
                let next = match cursor {
                    Some(cursor) if read.resumed => crate::ReplayRecords {
                        collection: last.collection,
                        scope: last.scope,
                        base: read.base,
                        ..cursor
                    },
                    _ => crate::ReplayRecords {
                        at: from,
                        through,
                        collections,
                        collection: last.collection,
                        scope: last.scope,
                        base: read.base,
                    },
                };
                Ok(Some(ReplayCut {
                    entries: Vec::new(),
                    records: all,
                    scanned_to: from,
                    exhausted: false,
                    replay_records: Some(next),
                }))
            }
        }
    }
    /// A replay page that cannot carry what it must (the transport's `unfit`): counted,
    /// and the replay stays at its position for the contact.
    fn unfit(&mut self) -> Option<ReplayCut> {
        self.out.replay_unfit += 1;
        None
    }

    /// An unkeyed write on `m`, on top of what `m` holds: the version keeps its key
    /// set. On the home track every `$inc` ends first when it is a counter.
    fn touch(&mut self, m: usize, c: u8, d: u16, t: u32) -> Res<()> {
        if self.down[m] {
            return Ok(());
        }
        if c == ORDERS && self.sched.track == Track::Home && !self.driving && self.running.is_none()
        {
            self.driving = true;
            for r in self.open_incs() {
                self.drive(r)?;
            }
            self.driving = false;
        }
        let e = self.e(m);
        let Ok(coll) = e.get_collection(DB, NAMES[c as usize]) else {
            self.note(format!("{} lacks {}: no touch", LETTERS[m], NAMES[c as usize]));
            return Ok(());
        };
        let id = DocId::Int64(if c == ITEMS { ITEM_BASE + i64::from(d) } else { i64::from(d) });
        let body = match e.get(&coll, &id).map_err(|e| err("get", e))? {
            Some(mut doc) => {
                let u = doc.get_i64("u").unwrap_or(0);
                doc.insert("u", u + 1);
                doc
            }
            None if c == ITEMS => doc! { "u": 1i64, "ks": Vec::<Bson>::new() },
            None => return Ok(()),
        };
        self.clock(m, t);
        let out = e.replace(&coll, &id, body, true).map_err(|e| err("touch", e))?;
        let s = format!(
            "{} touched {}/{id:?} at {:?}",
            LETTERS[m],
            NAMES[c as usize],
            out.stamp.map(|s| self.stamp(s))
        );
        self.note(s);
        Ok(())
    }

    /// A step run between two pages of a snapshot into `to` from `from`: never one that
    /// closes or sends to the receiver, nor closes the sender.
    ///
    /// A scoped or converge receiver serves clients through its pull, so a request to it
    /// between two pages is allowed; a whole-database receiver is catching up.
    fn apply_between_pages(&mut self, op: &Op, to: usize, from: usize, whole: bool) -> Res<()> {
        let skip = match &op.step {
            Step::Send { m, .. } | Step::Touch { m, .. } => *m as usize == to && whole,
            Step::Pull { to: t, .. } | Step::PullShort { to: t, .. } => *t as usize == to,
            Step::Page { to: t, restart, .. } => *t as usize == to || *restart,
            Step::Clock { .. } => false,
            // The sender restored between two pages: its change log begins again
            // over the records its backup holds, or not at all.
            Step::Backup { m } | Step::Restore { m, .. } => *m as usize == to,
            _ => true,
        };
        if skip {
            self.note(format!("skipped between pages: {op}"));
            return Ok(());
        }
        let _ = from;
        self.out.between += 1;
        let saved = self.step.clone();
        self.step = format!("{saved} > {op}");
        let out = match &op.step {
            Step::Send { r, m, .. } => self.send_plain(*r, *m as usize, op.t),
            _ => self.apply(op),
        };
        self.step = saved;
        out
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
            | Step::Drop { m, .. }
            | Step::Create { m, .. }
            | Step::Restore { m, .. }
            | Step::Rewind { m }
            | Step::RewindAlone { m }
            | Step::Orphan { m }
            | Step::Retention { m } => *m as usize == running,
            Step::Page { to, restart, .. } => *restart && *to as usize == running,
            // An unkeyed write on the running member between its commits: the stamp
            // rule then leaves that document (`skipped`), which I5 does not allow for.
            Step::Touch { m, c, .. } => {
                *m as usize == running && *c == ORDERS && self.sched.track == Track::Home
            }
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
        self.check_contested()?;
        self.check_stored()?;
        self.check_i2()?;
        self.check_effects()
    }

    /// I2's other half, after every step: a member holding a live document whose key set
    /// holds request `r` (its effect, under any origin and in any life) holds a live
    /// record under `r`'s key, unless `r`'s memory ended or a rewind left its writes bare
    /// by design. The relay class where it happens, not only at quiescence: a member
    /// whose coverage passed an effect's entry without its record (as finding BR1's
    /// restore did, before #211) holds it bare from the step that brings it.
    fn check_effects(&mut self) -> Res<()> {
        let mut bare = Vec::new();
        for m in 0..self.n() {
            // A restored or rewound member, until its coverage of every other origin has
            // caught up, can hold an effect ahead of its record: a version its own-origin
            // replay reads back embeds another origin's write whose entry, and record, its
            // coverage has not reached; its pulls bring the record with the entry. (The
            // transport marks it restored until then, and the catching-up gate takes no
            // keyed request.)
            if self.members[m].restored_behind {
                continue;
            }
            let node = self.e(m).node_id();
            for ((c, shown), (stamp, doc)) in self.documents(m)? {
                for k in ks_of(&doc) {
                    let Some(req) = self.sched.requests.get(k as usize).copied() else { continue };
                    let r = k as u8;
                    if req.kind.coll() != c
                        || self.memory_ended(r)
                        || self.ghost.bare.contains(&r)
                        || self.ghost.effect_bare.contains(&(node, c, req.key))
                    {
                        continue;
                    }
                    if self.live_record(m, c, req.key)?.is_none() {
                        bare.push((m, node, c, req.key, r, shown.clone(), stamp));
                    }
                }
            }
        }
        for (m, node, c, key, r, shown, stamp) in bare {
            self.ghost.effect_bare.insert((node, c, key));
            self.out.relay += 1;
            let s = format!(
                "{} holds r{r}'s effect in {} {shown} at {} without a live record under key {key}",
                LETTERS[m],
                NAMES[c as usize],
                self.stamp(stamp)
            );
            self.note(format!("RELAY: {s}"));
            if self.strict {
                self.fail(Inv::I2, format!("(strict) {s}"));
            }
        }
        Ok(())
    }

    /// The greatest `last` of each request's records, on any member.
    fn observe_records(&mut self) -> Res<()> {
        for r in 0..self.sched.requests.len() as u8 {
            let req = self.sched.requests[r as usize];
            for m in 0..self.n() {
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
        for c in [ORDERS, ITEMS, BIG] {
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
        for m in 0..self.n() {
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
                    if n > bound && (unexplained || self.strict) {
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
        let mut failures = Vec::new();
        let nodes: Vec<NodeId> = (0..self.n()).map(|m| self.e(m).node_id()).collect();
        let mut processed: Vec<(NodeId, Stamp)> = self.ghost.processed.keys().copied().collect();
        processed.sort();
        for (node, stamp) in processed {
            let Some(m) = nodes.iter().position(|n| *n == node) else { continue };
            let Some((r, c, id)) = self.ghost.entries.get(&stamp).cloned() else { continue };
            // An entry of a life a drop ended needs its record too: a record outlives
            // a drop of its collection for its window.
            if self.memory_ended(r) || self.ghost.bare.contains(&r) {
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
                        self.out.relay += 1;
                        let s = format!(
                            "RELAY: {} took r{r}'s entry {} without its record, from a sender \
                             that held it",
                            LETTERS[m],
                            self.stamp(stamp)
                        );
                        self.note(s.clone());
                        if self.strict {
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
        for m in 0..self.n() {
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
        self.down.iter_mut().for_each(|d| *d = false);
        for _ in 0..QUIESCE_ROUNDS {
            let before = self.fingerprint()?;
            for to in 0..self.n() {
                for from in 0..self.n() {
                    if to != from {
                        self.pull(to, from, BATCH)?;
                        self.finish_purges()?;
                    }
                }
            }
            self.repair_round()?;
            self.finish_purges()?;
            if self.fingerprint()? == before {
                for m in 0..self.n() {
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
        for me in 0..self.n() {
            for peer in 0..self.n() {
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
                    self.transfer(me, peer, Some(id), None, &[])?;
                }
                // The cut digest (ADR-230): a collection whose digests differ at the
                // cut is repaired by a converge page (ADR-237).
                // Bytes only, no wall-clock part: the digest's 200 ms scan budget makes a
                // repair depend on how fast the host runs (a loaded or stalled host gives the
                // digest up and takes no converge page), and a schedule must decide the
                // same every run.
                let budget = crate::ScanBudget {
                    wall: std::time::Duration::MAX,
                    ..crate::ScanBudget::default()
                };
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
                        let cut = Some(plan.cut.clone());
                        self.transfer(me, peer, Some(verdict.collection), cut, &[])?;
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
                        None => self.transfer(me, peer, Some(id), None, &[])?,
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
            return self.transfer(to, from, Some(id), None, &[]);
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
            self.check_carried(from, &window.records)?;
            let before = self.live_before(to, &window.records)?;
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
            self.fps_carried(src.node_id(), to, before)?;
            self.processed_by(from, &dst, &window.entries, &window.records)?;
            if outcome.purge_pending_at.is_some() {
                dst.finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
                continue;
            }
            if let Some(unknown) = outcome.unknown {
                return self.transfer(to, from, Some(unknown.id), None, &[]);
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
        // The forbidden procedure's undone writes: anti-entropy puts them back on the
        // member that rejoined (the write, or a later version of its document).
        for (m, stamp, c, id) in self.undone.clone() {
            let e = self.e(m);
            let back = e.has_oplog_entry(&stamp).map_err(|e| err("has_oplog_entry", e))?
                || stored(&e, c, &id).is_some_and(|(at, _)| at >= stamp);
            self.out.undone_returned += usize::from(back);
        }
        // Identical documents.
        let docs: Vec<_> = (0..self.n()).map(|m| self.documents(m)).collect::<Res<_>>()?;
        for m in 1..self.n() {
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
                    // So is a restored identity's write it made after its backup, held by
                    // a peer only as page state: no own-origin replay reads back what a
                    // peer does not hold in position.
                    let gone = |s: &Stamp| {
                        self.retired.contains(&s.node)
                            || self.restored.get(&s.node).is_some_and(|at| s.hlc > *at)
                    };
                    let retired = |v: Option<&(Stamp, Document)>| v.is_none_or(|(s, _)| gone(s));
                    // A side without the document may hold a tombstone of such an identity.
                    let id = a.or(b).map(|(_, d)| id_of(d));
                    let tombstone = |member: usize| {
                        id.as_ref()
                            .and_then(|id| stored(&self.e(member), k.0, id))
                            .filter(|(_, body)| body.is_none())
                            .map(|(s, _)| s)
                    };
                    let tombstones =
                        [a.is_none().then(|| tombstone(0)), b.is_none().then(|| tombstone(m))];
                    // Or a drop of the collection, made by such an identity, at or above
                    // the version the other side holds: the life it ended is buried here.
                    let present = a.or(b).map(|(s, _)| s.hlc);
                    let buried_by = |member: usize| -> Option<Stamp> {
                        let floor = self.floor(member, k.0).ok()?;
                        if present.is_none_or(|at| floor < at) {
                            return None;
                        }
                        self.ghost
                            .drops
                            .iter()
                            .find(|(c, at)| *c == k.0 && at.hlc == floor)
                            .map(|(_, at)| *at)
                    };
                    let drops =
                        [a.is_none().then(|| buried_by(0)), b.is_none().then(|| buried_by(m))];
                    retired_only &= (retired(a) || retired(b))
                        && ([a, b].iter().flatten().any(|(s, _)| gone(s))
                            || tombstones.iter().flatten().flatten().any(gone)
                            || drops.iter().flatten().flatten().any(gone));
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
                (0..self.n()).map(|m| self.live_record(m, c, key)).collect::<Res<_>>()?;
            if records.iter().all(|r| *r == records[0]) {
                continue;
            }
            // Each member's record against the merge of them all. One the merged record
            // dominates (merging it in changes nothing), or none where no document holds
            // a request under the key: the S1 class, a record state carried only by entries
            // that lost where the member pulled them from (or a record-only `done`), which
            // never reaches it; harmless while I2 holds, since every entry the member
            // processed is at or above its own `first`. None where a document holds a
            // request under the key, every entry vetted by I2 as it was taken: the effect
            // came without its record, the relay class (or L1). Anything else fails.
            let merged = records
                .iter()
                .flatten()
                .cloned()
                .reduce(|a, b| a.merge(&b))
                .expect("a record differs, so one is present");
            let mut kinds = Vec::new();
            for m in 0..self.n() {
                // Whether a live document here holds a request under the key.
                let holds_effect = docs[m].iter().any(|((dc, _), (_, doc))| {
                    *dc == c
                        && ks_of(doc).iter().any(|k| {
                            self.sched.requests.get(*k as usize).is_some_and(|q| q.key == key)
                                && !self.ghost.bare.contains(&(*k as u8))
                        })
                });
                kinds.push(match &records[m] {
                    Some(r) if *r == merged => "same",
                    None if !holds_effect => "S1",
                    None => "relay",
                    Some(r) if r.merge(&merged) == merged => "S1",
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
            let mut lives = Vec::new();
            for m in 0..self.n() {
                let raw = self
                    .e(m)
                    .idempotency_record(coll_id(c), &ScopeHash([key; 32]))
                    .map_err(|e| err("idempotency_record", e))?;
                lives.push(format!(
                    "{}: floor {} stored last {:?}",
                    LETTERS[m],
                    self.hlc(self.floor(m, c)?),
                    raw.map(|r| self.stamp(r.last))
                ));
            }
            let detail = format!(
                "key {key} in {}: records differ: {} ({})",
                NAMES[c as usize],
                shown.join(" | "),
                lives.join(", ")
            );
            if kinds.contains(&"bad") {
                self.fail(Inv::I4, detail);
                continue;
            }
            if kinds.contains(&"relay") {
                self.out.relay += 1;
            }
            if kinds.contains(&"S1") {
                self.out.s1 += 1;
            }
            self.note(format!("finding: {detail}"));
            if (kinds.contains(&"relay") && self.strict)
                || (kinds.contains(&"S1") && self.strict_s1)
            {
                self.fail(Inv::I4, format!("(strict) {detail}"));
            }
        }
        // I5: on the home track every counter is the number of `$inc` requests accepted
        // for it. Not across a drop of `orders`: a life begins with fresh counters, and a
        // drop keeps the versions stamped after it (ADR-236), their counts with them; I8
        // checks each completion against the life it completed in instead.
        let orders_dropped = self.ghost.drops.iter().any(|(c, _)| *c == ORDERS);
        if self.sched.track == Track::Home && orders_dropped {
            self.out.i5_off += 1;
        }
        // Requests with writes on both sides of a drop of `orders`.
        for r in 0..self.sched.requests.len() as u8 {
            let written: Vec<Hlc> = self
                .ghost
                .entries
                .iter()
                .filter(|(_, (q, qc, _))| *q == r && *qc == ORDERS)
                .map(|(s, _)| s.hlc)
                .collect();
            let straddled = self.ghost.drops.iter().any(|(c, at)| {
                *c == ORDERS
                    && written.iter().any(|w| *w <= at.hlc)
                    && written.iter().any(|w| *w > at.hlc)
            });
            if straddled {
                self.out.straddled += 1;
            }
        }
        if self.sched.track == Track::Home && !orders_dropped {
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
                    if n != want && !(explained && !self.strict) {
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
    world.borrow_mut().running_req = Some(*r);
    let phases = (
        Arc::clone(&p.phase),
        Rc::clone(&p.phase_seq),
        Rc::clone(&p.refusing),
        Rc::clone(&p.i8_at),
    );
    arm(Rc::clone(world), Rc::clone(&pending), *crash, Arc::downgrade(&engine), phases, 1);
    drop(engine);
    let answer = call(&p);
    // Disarmed: a hook left armed would run at the next commit on this thread.
    barrier_hooks::at(Point::Released, || {});
    world.borrow_mut().running = None;
    world.borrow_mut().running_req = None;
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

/// What a request's commit points keep: how many have run, the ghost's order after each,
/// and whether the algebra says the record refuses the next chunk after each one's steps.
type Phases = (
    Arc<std::sync::atomic::AtomicUsize>,
    Rc<RefCell<Vec<u64>>>,
    Rc<RefCell<Vec<bool>>>,
    Rc<RefCell<Option<I8State>>>,
);

/// Arm the `j`-th commit point of the request on `engine`: its record's store in the
/// algebra, its steps, then its crash, then the next point.
fn arm(
    world: Rc<RefCell<World>>,
    pending: Rc<RefCell<BTreeMap<u8, Vec<Op>>>>,
    crash: Option<(u8, Crash)>,
    engine: std::sync::Weak<Engine>,
    phases: Phases,
    j: u8,
) {
    if j > 8 {
        return;
    }
    barrier_hooks::at(Point::Released, move || {
        let running = {
            let w = world.borrow();
            w.running.zip(w.running_req)
        };
        // Every commit of a keyed request stores its record, merged into the live one.
        if let Some((m, r)) = running {
            world.borrow_mut().fps_own(m, r);
        }
        let ops = pending.borrow_mut().remove(&j).unwrap_or_default();
        // I8 at completion, should this commit be the request's last: before the steps.
        let snapshot = match running {
            Some((m, r))
                if !ops.is_empty() && world.borrow().sched.requests[r as usize].kind.is_inc() =>
            {
                world.borrow().i8_state(r, m).ok()
            }
            _ => None,
        };
        *phases.3.borrow_mut() = snapshot;
        for op in &ops {
            let out = world.borrow_mut().apply_inner(op);
            if let Err(e) = out {
                world.borrow_mut().fail(Inv::Error, format!("between commits: {op}: {e}"));
            }
        }
        let refuses = match running {
            Some((m, r)) => world.borrow().refuses(m, r).map(|(refuses, _)| refuses),
            None => Ok(false),
        };
        match refuses {
            Ok(refuses) => phases.2.borrow_mut().push(refuses),
            Err(e) => world.borrow_mut().fail(Inv::Error, format!("after commit point {j}: {e}")),
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

/// The counters of a life of `orders`, as it begins.
fn fresh_counters() -> Vec<Document> {
    (0..COUNTERS).map(|i| doc! { "_id": i, "n": 0i64, "ks": Vec::<Bson>::new() }).collect()
}

/// `orders` with its counters and an empty `items`, on A, pulled by B and C.
fn setup(w: &mut World) -> Res<()> {
    let a = w.e(0);
    let orders = a.create_collection(DB, NAMES[ORDERS as usize]).map_err(|e| err("create", e))?;
    a.create_collection(DB, NAMES[ITEMS as usize]).map_err(|e| err("create", e))?;
    a.insert_many(&orders, fresh_counters()).map_err(|e| err("insert_many", e.source))?;
    for m in 1..w.n() {
        w.pull(m, 0, BATCH)?;
    }
    Ok(())
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
    run_with(sched, None, false)
}

/// [`run`], with every classified finding failing it when `all_strict` says so (as a
/// named row runs; `None`, as the environment says), and requests sent to a member
/// catching up (`retry_catching_up`).
pub(crate) fn run_with(
    sched: &Schedule,
    all_strict: Option<bool>,
    retry_catching_up: bool,
) -> Outcome {
    run_framed(sched, all_strict, retry_catching_up, sched.bias.frame)
}

/// [`run_with`], with the model's replay frame of `frame` units.
fn run_framed(
    sched: &Schedule,
    all_strict: Option<bool>,
    retry_catching_up: bool,
    frame: usize,
) -> Outcome {
    let world = Rc::new(RefCell::new(World::new(sched.clone(), base())));
    if let Some(all) = all_strict {
        world.borrow_mut().strict = all;
        world.borrow_mut().strict_s1 = all;
    }
    world.borrow_mut().retry_catching_up = retry_catching_up;
    world.borrow_mut().replay_frame = frame;
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
        // The small page room is the first step's alone (the pull a restore splits).
        if i == 0 && world.borrow().page_room.is_some() {
            let mut w = world.borrow_mut();
            w.page_room = None;
            for m in 0..w.n() {
                w.e(m).set_snapshot_room_for_test(None);
            }
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
struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Rng { state: seed ^ 0x5DEE_CE66_D1CE_4E5B }
    }
    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
    fn pick<T: Copy>(&mut self, from: &[T]) -> T {
        from[self.below(from.len() as u64) as usize]
    }
}

struct Gen {
    rng: Rng,
    track: Track,
    home: u8,
    /// Members in the schedule.
    n: u8,
    requests: Vec<Request>,
    /// Requests of the current era, which may be sent again.
    open: Vec<u8>,
    skew: [i64; MEMBERS_MAX],
    g: i64,
    resets: usize,
    /// Members the schedule has taken down.
    down: Vec<bool>,
}

const SKEWS: [i64; 7] = [-20, -10, 0, 0, 0, 10, 20];

impl Gen {
    fn t(&self, m: u8, extra: i64) -> u32 {
        (self.g + extra + self.skew[m as usize]).max(1) as u32
    }

    fn up(&self) -> Vec<u8> {
        (0..self.n).filter(|m| !self.down[*m as usize]).collect()
    }

    /// A member that is up.
    fn member(&mut self) -> u8 {
        let up = self.up();
        self.rng.pick(&up)
    }

    /// Another member that is up, or `m` itself when none is.
    fn other(&mut self, m: u8) -> u8 {
        let others: Vec<u8> = self.up().into_iter().filter(|o| *o != m).collect();
        if others.is_empty() { m } else { self.rng.pick(&others) }
    }

    fn pair(&mut self) -> (u8, u8) {
        let to = self.member();
        (to, self.other(to))
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
            let (r, pos) = self.rng.pick(&derived);
            Target::Derived(r, pos)
        } else {
            Target::Client(self.rng.below(3) as u8)
        }
    }

    fn kind(&mut self) -> Kind {
        match self.rng.below(100) {
            0..=24 => Kind::IncMulti,
            25..=34 => Kind::IncSingle { d: self.rng.below(COUNTERS as u64) as u8 },
            35..=42 => Kind::IncRange { d: self.rng.below(COUNTERS as u64 - 1) as u8 },
            43..=55 => Kind::Insert { id: None },
            56..=61 => Kind::Insert { id: Some(self.rng.below(3) as u8) },
            62..=68 => Kind::Bulk,
            69..=84 => Kind::Replace { at: self.target() },
            _ => Kind::Delete { at: self.target() },
        }
    }

    /// A new request: now and then a key reused for another request.
    fn new_request(&mut self) -> u8 {
        let r = self.requests.len() as u8;
        let kind = self.kind();
        let key = if !self.open.is_empty() && self.rng.chance(15) {
            let q = self.rng.pick(&self.open.clone());
            self.requests[q as usize].key
        } else {
            r + 1
        };
        self.requests.push(Request { key, kind });
        self.open.push(r);
        r
    }

    /// A request to send: one of the era's again, or a new one.
    fn request(&mut self, again: bool) -> u8 {
        if again && !self.open.is_empty() {
            self.rng.pick(&self.open.clone())
        } else {
            self.new_request()
        }
    }

    fn member_for(&mut self, r: u8) -> u8 {
        if self.track == Track::Home && self.requests[r as usize].kind.is_inc() {
            self.home
        } else {
            self.member()
        }
    }

    fn send(&mut self, r: u8) -> Op {
        let m = self.member_for(r);
        let crash = self.rng.chance(25).then(|| {
            let how = self.rng.pick(&[Crash::AfterCommit, Crash::BeforeCommit, Crash::FsyncFails]);
            (self.rng.below(4) as u8, how)
        });
        let mut inner = Vec::new();
        if self.rng.chance(40) {
            for _ in 0..1 + self.rng.below(2) {
                let j = 1 + self.rng.below(3) as u8;
                let mut ops = Vec::new();
                for _ in 0..1 + self.rng.below(2) {
                    let op = self.inner_op(r, m);
                    // A drop made elsewhere, taken by the running member between two of
                    // its commits.
                    let pulled = match op.step {
                        Step::Drop { m: by, .. } if by != m && self.rng.chance(70) => Some(by),
                        _ => None,
                    };
                    ops.push(op);
                    if let Some(by) = pulled {
                        ops.push(Op { step: Step::Pull { to: m, from: by }, t: 0 });
                    }
                }
                inner.push((j, ops));
            }
        }
        Op { step: Step::Send { r, m, crash, inner }, t: self.t(m, 0) }
    }

    /// An unkeyed write on `m` (the home's alone for a counter on the home track).
    fn touch(&mut self, m: u8, extra: i64) -> Op {
        let c = if self.rng.chance(50) { ORDERS } else { ITEMS };
        let m = if c == ORDERS && self.track == Track::Home { self.home } else { m };
        let d = if c == ORDERS { self.rng.below(COUNTERS as u64) } else { self.rng.below(3) };
        Op { step: Step::Touch { m, c, d: d as u16 }, t: self.t(m, extra) }
    }

    /// A step between two commits of request `r` on `m`.
    fn inner_op(&mut self, r: u8, m: u8) -> Op {
        let extra = 1 + self.rng.below(8) as i64;
        match self.rng.below(100) {
            0..=29 => {
                let (to, from) = self.pair();
                Op { step: Step::Pull { to, from }, t: 0 }
            }
            30..=37 => {
                let (to, from) = self.pair();
                Op { step: Step::PullShort { to, from }, t: 0 }
            }
            38..=59 => {
                // A concurrent attempt on another member: of the same request, or, on the
                // home track (where every `$inc` stays home), of one that is not an `$inc`.
                let other = self.other(m);
                let pick = if self.track == Track::Home && self.requests[r as usize].kind.is_inc() {
                    let not_inc: Vec<u8> = self
                        .open
                        .iter()
                        .copied()
                        .filter(|q| !self.requests[*q as usize].kind.is_inc())
                        .collect();
                    (!not_inc.is_empty()).then(|| self.rng.pick(&not_inc))
                } else {
                    Some(r)
                };
                match pick {
                    Some(q) if other != m => Op {
                        step: Step::Send { r: q, m: other, crash: None, inner: Vec::new() },
                        t: self.t(other, extra),
                    },
                    _ => Op { step: Step::Clock { m }, t: self.t(m, extra) },
                }
            }
            60..=69 => {
                // A racing or on-top unkeyed write, on another member.
                let other = self.other(m);
                self.touch(other, extra)
            }
            70..=74 => {
                // A drop of `orders` on another member, concurrent with the request (a
                // creation of it, now and then).
                let other = self.other(m);
                if self.rng.chance(70) {
                    Op { step: Step::Drop { m: other, c: ORDERS }, t: self.t(other, extra) }
                } else {
                    Op { step: Step::Create { m: other, c: ORDERS }, t: self.t(other, extra) }
                }
            }
            75..=82 => {
                let later = extra + self.rng.below(40) as i64;
                Op { step: Step::Clock { m }, t: self.t(m, later) }
            }
            83..=93 => {
                let (to, from) = self.pair();
                let c = if self.rng.chance(50) { ORDERS } else { ITEMS };
                let converge = self.rng.chance(40);
                let restart = to != m && self.rng.chance(50);
                Op { step: Step::Page { to, from, c, converge, restart, inner: Vec::new() }, t: 0 }
            }
            _ => Op { step: Step::Restart { m: self.other(m) }, t: 0 },
        }
    }

    /// A restore of `m` from its latest backup or the one before, with steps between the
    /// replay's pages now and then: a keyed write or a pull elsewhere, a backup or a
    /// restore of another member (a sender's change log begins again under a cursor).
    fn restore(&mut self, m: u8) -> Op {
        let nth = u8::from(self.rng.chance(20));
        let mut inner = Vec::new();
        if self.rng.chance(40) {
            let mut ops = Vec::new();
            for _ in 0..1 + self.rng.below(2) {
                let other = self.other(m);
                let extra = 1 + self.rng.below(8) as i64;
                ops.push(match self.rng.below(10) {
                    0..=3 => {
                        let again = self.rng.chance(50);
                        let r = self.request(again);
                        let at = if self.track == Track::Home
                            && self.requests[r as usize].kind.is_inc()
                        {
                            self.home
                        } else {
                            other
                        };
                        Op {
                            step: Step::Send { r, m: at, crash: None, inner: Vec::new() },
                            t: self.t(at, extra),
                        }
                    }
                    4..=5 => {
                        let by = self.other(other);
                        Op { step: Step::Pull { to: other, from: by }, t: 0 }
                    }
                    6 => Op { step: Step::Backup { m: other }, t: 0 },
                    7 if !(self.track == Track::Home && other == self.home) => {
                        Op { step: Step::Restore { m: other, nth: 0, inner: Vec::new() }, t: 0 }
                    }
                    _ => self.touch(other, extra),
                });
            }
            inner.push((1 + self.rng.below(3) as u8, ops));
        }
        Op { step: Step::Restore { m, nth, inner }, t: 0 }
    }

    /// Steps between a scoped or converge page's pages, now and then: a keyed or unkeyed
    /// write on the sender, its backup, or its restore (its change log begins again).
    fn between_pages(&mut self, from: u8) -> Vec<(u8, Vec<Op>)> {
        if !self.rng.chance(25) {
            return Vec::new();
        }
        let extra = 1 + self.rng.below(8) as i64;
        let op = match self.rng.below(10) {
            0..=3 => {
                let again = self.rng.chance(40);
                let r = self.request(again);
                let at = if self.track == Track::Home && self.requests[r as usize].kind.is_inc() {
                    self.home
                } else {
                    from
                };
                Op {
                    step: Step::Send { r, m: at, crash: None, inner: Vec::new() },
                    t: self.t(at, extra),
                }
            }
            4..=5 => self.touch(from, extra),
            6 => Op { step: Step::Backup { m: from }, t: 0 },
            _ if !(self.track == Track::Home && from == self.home) => {
                let nth = self.rng.below(2) as u8;
                Op { step: Step::Restore { m: from, nth, inner: Vec::new() }, t: 0 }
            }
            _ => self.touch(from, extra),
        };
        vec![(1, vec![op])]
    }

    /// A reset of `m` from `from`, with steps between the snapshot's pages now and then:
    /// writes on the sender, a pull between two others.
    fn reset(&mut self, m: u8, from: u8) -> Op {
        let mut inner = Vec::new();
        if self.rng.chance(50) {
            let mut ops = Vec::new();
            for _ in 0..1 + self.rng.below(3) {
                let extra = 1 + self.rng.below(8) as i64;
                ops.push(match self.rng.below(10) {
                    0..=4 => {
                        let again = self.rng.chance(40);
                        let r = self.request(again);
                        let at = if self.track == Track::Home
                            && self.requests[r as usize].kind.is_inc()
                        {
                            self.home
                        } else {
                            from
                        };
                        Op {
                            step: Step::Send { r, m: at, crash: None, inner: Vec::new() },
                            t: self.t(at, extra),
                        }
                    }
                    5..=7 => self.touch(from, extra),
                    8 if self.rng.chance(50) => Op { step: Step::Backup { m: from }, t: 0 },
                    8 if !(self.track == Track::Home && from == self.home) => {
                        let nth = self.rng.below(2) as u8;
                        Op { step: Step::Restore { m: from, nth, inner: Vec::new() }, t: 0 }
                    }
                    _ => {
                        let (to, by) = self.pair();
                        Op { step: Step::Pull { to, from: by }, t: 0 }
                    }
                });
            }
            inner.push((1 + self.rng.below(2) as u8, ops));
        }
        Op { step: Step::Reset { m, from, inner }, t: 0 }
    }
}

/// A schedule of `len` steps from `seed`: three members, or four now and then.
pub(crate) fn generate(seed: u64, track: Track, len: usize) -> Schedule {
    let mut rng = Rng::new(seed);
    let n = if rng.chance(25) { 4 } else { 3 };
    let home = rng.below(n as u64) as u8;
    let skew = std::array::from_fn(|_| rng.pick(&SKEWS));
    let mut plan = Gen {
        rng,
        track,
        home,
        n,
        requests: Vec::new(),
        open: Vec::new(),
        skew,
        g: 100,
        resets: 0,
        down: vec![false; n as usize],
    };
    let mut ops = Vec::with_capacity(len);
    for _ in 0..len {
        plan.g += 10;
        if plan.rng.below(10) == 0 {
            let m = plan.rng.below(n as u64) as usize;
            plan.skew[m] = plan.rng.pick(&SKEWS);
        }
        let op = match plan.rng.below(100) {
            0..=20 => {
                let r = plan.new_request();
                plan.send(r)
            }
            21..=38 => {
                let r = plan.request(true);
                plan.send(r)
            }
            39..=53 => {
                let (to, from) = plan.pair();
                Op { step: Step::Pull { to, from }, t: 0 }
            }
            54..=57 => {
                let (to, from) = plan.pair();
                Op { step: Step::PullShort { to, from }, t: 0 }
            }
            58..=64 => {
                let m = plan.member();
                plan.touch(m, 0)
            }
            65..=69 => {
                let (to, from) = plan.pair();
                let c = if plan.rng.chance(50) { ORDERS } else { ITEMS };
                let converge = plan.rng.chance(40);
                let restart = plan.rng.chance(60);
                let inner = plan.between_pages(from);
                Op { step: Step::Page { to, from, c, converge, restart, inner }, t: 0 }
            }
            70..=72 if plan.resets < SPARES => {
                plan.resets += 1;
                let from = plan.member();
                // Not the home on the home track: a reset of it loses what it alone held,
                // the `$inc`s it answered among them, data loss and not a retry's concern.
                let targets: Vec<u8> = plan
                    .up()
                    .into_iter()
                    .filter(|m| *m != from && !(track == Track::Home && *m == plan.home))
                    .collect();
                if targets.is_empty() {
                    Op { step: Step::Restart { m: from }, t: 0 }
                } else {
                    let m = plan.rng.pick(&targets);
                    plan.reset(m, from)
                }
            }
            70..=74 => Op { step: Step::Restart { m: plan.member() }, t: 0 },
            75..=76 => Op { step: Step::Crash { m: plan.member() }, t: 0 },
            77..=81 => {
                // Down while at least two stay up (the home stays up on the home track),
                // or a member back up.
                let can_go: Vec<u8> = plan
                    .up()
                    .into_iter()
                    .filter(|m| !(track == Track::Home && *m == plan.home))
                    .collect();
                let downed: Vec<u8> = (0..n).filter(|m| plan.down[*m as usize]).collect();
                if !downed.is_empty() && (plan.up().len() <= 2 || plan.rng.chance(50)) {
                    let m = plan.rng.pick(&downed);
                    plan.down[m as usize] = false;
                    Op { step: Step::Up { m }, t: 0 }
                } else if plan.up().len() > 2 && !can_go.is_empty() {
                    let m = plan.rng.pick(&can_go);
                    plan.down[m as usize] = true;
                    Op { step: Step::Down { m }, t: 0 }
                } else {
                    let (to, from) = plan.pair();
                    Op { step: Step::Pull { to, from }, t: 0 }
                }
            }
            82..=85 => {
                let m = plan.member();
                let c = if plan.rng.chance(40) { ORDERS } else { ITEMS };
                Op { step: Step::Drop { m, c }, t: plan.t(m, 0) }
            }
            86..=90 => {
                let m = plan.member();
                let c = if plan.rng.chance(50) { ORDERS } else { ITEMS };
                Op { step: Step::Create { m, c }, t: plan.t(m, 0) }
            }
            93 => Op { step: Step::Backup { m: plan.member() }, t: 0 },
            94 => {
                // Not the home on the home track: a restore loses what it wrote after the
                // backup, the `$inc`s it answered among them.
                let m = plan.member();
                if track == Track::Home && m == plan.home {
                    Op { step: Step::Restart { m }, t: 0 }
                } else {
                    plan.restore(m)
                }
            }
            95 => {
                // A rewind to a moment a little back: what it discards comes back from
                // the peers that hold it. Not the home on the home track, as a restore.
                let m = plan.member();
                if track == Track::Home && m == plan.home {
                    Op { step: Step::Restart { m }, t: 0 }
                } else {
                    let back = plan.rng.below(60) as i64;
                    Op { step: Step::Rewind { m }, t: plan.t(m, -back) }
                }
            }
            96..=97 => Op { step: Step::Orphan { m: plan.member() }, t: 0 },
            91..=92 => {
                // The window passes: an era ends.
                plan.g += WINDOW_SECS as i64 * 1000 + 500;
                plan.open.clear();
                Op { step: Step::Retention { m: plan.member() }, t: plan.g as u32 }
            }
            _ => {
                let r = plan.request(true);
                plan.send(r)
            }
        };
        ops.push(op);
    }
    let bias = plan.bias(&mut ops);
    Schedule { track, home: plan.home, members: n, requests: plan.requests, ops, bias }
}

/// The time of the step at `i` of `ops`: just after the latest time a step before it
/// carries, so an inserted step keeps every member's clock moving forward.
fn time_at(ops: &[Op], i: usize) -> u32 {
    ops[..i].iter().map(|op| op.t).max().unwrap_or(100).max(100) + 1
}

impl Gen {
    /// Small budgets and the steps that reach the paths behind them, each in a share of
    /// the seeds, so the long run reaches every one of them many times (its coverage
    /// assertion holds it to that). Each is spliced into the schedule made so far:
    ///
    /// - **A restore during a pull** (a third of the seeds): every member's page room is a
    ///   few documents, and the schedule begins with a scoped pull of `orders` whose sender
    ///   takes its first keyed write between two pages: its change log begins with an
    ///   epoch, and the receiver's next page, asked with the zero records base, is refused.
    /// - **A contested key between chunks** (a third of the `Anywhere` seeds): an `$inc` of
    ///   every counter on one member, then another under its key on a member without its
    ///   record, which pulls the first between its first two chunks: the record turns
    ///   contested there and the request stops partially applied.
    /// - **Records alone** (half the seeds): a frame of one to three units (one, most often: a
    ///   page whose unvouched records alone fill it is unfit), a
    ///   member and its first peer backed up at the start, and the member restored two
    ///   thirds in; in half of them its first peer is restored between the replay's first
    ///   two pages (a cursor of another epoch), and in a third of them the cursor's delta
    ///   takes a hundred bytes (a changed record too large for it).
    /// - **A rewind across a dropped life** (a fifth of the `Anywhere` seeds): a document
    ///   written and taken, its collection dropped and created again, the document replaced
    ///   by a member that never heard of the drop, and a rewind to between the creation and
    ///   the replacement, which the member that created it refuses (ADR-248).
    /// - **A rewind of a shared write** (a quarter of the `Anywhere` seeds): a keyed write
    ///   on a member, a peer's pull of it, and a rewind of the member to just before it,
    ///   which the documented procedure widens to every member.
    fn bias(&mut self, ops: &mut Vec<Op>) -> Bias {
        let mut bias = Bias::default();
        let len = ops.len();
        if self.rng.chance(33) {
            // Room for a fresh counter with the cursor that names it (P2 counts both at
            // the size they cross in), one or two of them a page: `orders` spans pages.
            bias.page_room = Some(self.rng.pick(&[700, 1100]));
            let (to, from) = self.pair();
            let r = self.requests.len() as u8;
            self.requests.push(Request { key: r + 1, kind: Kind::Insert { id: None } });
            self.open.push(r);
            let send =
                Op { step: Step::Send { r, m: from, crash: None, inner: Vec::new() }, t: 105 };
            let converge = self.rng.chance(30);
            let page = Step::Page {
                to,
                from,
                c: ORDERS,
                converge,
                restart: false,
                inner: vec![(1, vec![send])],
            };
            ops.insert(0, Op { step: page, t: 0 });
        }
        if self.track == Track::Anywhere && self.rng.chance(33) && len > 8 {
            let at = 2 + self.rng.below(len as u64 - 6) as usize;
            let (y, x) = self.pair();
            let p = self.requests.len() as u8;
            self.requests.push(Request { key: p + 1, kind: Kind::IncMulti });
            self.requests.push(Request { key: p + 1, kind: Kind::IncMulti });
            self.open.extend([p, p + 1]);
            let t = time_at(ops, at);
            let first = Op { step: Step::Send { r: p, m: x, crash: None, inner: Vec::new() }, t };
            let pull = Op { step: Step::Pull { to: y, from: x }, t: 0 };
            let second = Op {
                step: Step::Send { r: p + 1, m: y, crash: None, inner: vec![(1, vec![pull])] },
                t: t + 5,
            };
            ops.splice(at..at, [first, second]);
        }
        if self.track == Track::Anywhere && self.rng.chance(25) && len > 8 {
            // A rewind whose undone write a peer took: the procedure rewinds every member
            // (the second shape; the first, a write no peer holds, the generator's own
            // rewinds reach often).
            let at = len / 3 + self.rng.below(len as u64 / 3) as usize;
            let (peer, m) = self.pair();
            let r = self.requests.len() as u8;
            self.requests.push(Request { key: r + 1, kind: Kind::Insert { id: None } });
            self.open.push(r);
            let t = time_at(ops, at);
            let block = [
                Op { step: Step::Send { r, m, crash: None, inner: Vec::new() }, t: t + 1 },
                Op { step: Step::Pull { to: peer, from: m }, t: 0 },
                Op { step: Step::Rewind { m }, t },
            ];
            ops.splice(at..at, block);
        }
        if self.track == Track::Anywhere && self.n >= 3 && self.rng.chance(20) && len > 8 {
            // A rewind across a dropped life whose replacement it cannot undo (ADR-248):
            // P writes a document of `items` and R takes it; P drops `items` and Q takes the
            // drop and creates `items` again; R, which never heard of the drop, replaces the
            // document, and Q takes that; Q rewinds to between its creation and R's write,
            // which the procedure widens to every member, and Q refuses: the document's
            // only value at the target is of the life the drop ended.
            let at = len / 3 + self.rng.below(len as u64 / 3) as usize;
            let p = self.rng.below(self.n as u64) as u8;
            let q = (p + 1) % self.n;
            let r = (p + 2) % self.n;
            let d = self.rng.below(3) as u16;
            let t = time_at(ops, at);
            let block = [
                Op { step: Step::Touch { m: p, c: ITEMS, d }, t: t + 1 },
                Op { step: Step::Pull { to: r, from: p }, t: 0 },
                Op { step: Step::Drop { m: p, c: ITEMS }, t: t + 2 },
                Op { step: Step::Pull { to: q, from: p }, t: 0 },
                Op { step: Step::Create { m: q, c: ITEMS }, t: t + 3 },
                Op { step: Step::Touch { m: r, c: ITEMS, d }, t: t + 6 },
                Op { step: Step::Pull { to: q, from: r }, t: 0 },
                Op { step: Step::Rewind { m: q }, t: t + 4 },
            ];
            ops.splice(at..at, block);
        }
        if self.rng.chance(50) {
            bias.frame = self.rng.pick(&[1, 1, 1, 2, 3]);
            let m = self.rng.below(self.n as u64) as u8;
            let m = if self.track == Track::Home && m == self.home { (m + 1) % self.n } else { m };
            let peer = (0..self.n).find(|o| *o != m).expect("two members");
            let mut inner = Vec::new();
            let at = (ops.len() * 2 / 3).max(2);
            match self.rng.below(10) {
                // The sender restored between two replay pages: a cursor of another epoch.
                0..=3 => {
                    let again =
                        Op { step: Step::Restore { m: SENDER, nth: 0, inner: Vec::new() }, t: 0 };
                    inner.push((1 + self.rng.below(2) as u8, vec![again]));
                }
                // A keyed write on the sender between two replay pages, with a delta too
                // small for the record it changes.
                4..=8 => {
                    bias.delta_bytes = 100;
                    let r = self.new_request();
                    let at_peer =
                        if self.track == Track::Home && self.requests[r as usize].kind.is_inc() {
                            self.home
                        } else {
                            peer
                        };
                    let t = time_at(ops, at) + 2;
                    let send = Op {
                        step: Step::Send { r, m: at_peer, crash: None, inner: Vec::new() },
                        t,
                    };
                    inner.push((1, vec![send]));
                }
                _ => {}
            }
            // Every member backed up (the page's sender restored between two replay
            // pages may be any of them), then keyed writes on `m` of `items` (records of
            // one collection, which ride its replay's first page together) and on a
            // peer, `m`'s writes pulled by its first peer, and `m` restored: it lost
            // them, and its replay reads them back.
            let t = time_at(ops, at);
            let mut block: Vec<Op> =
                (0..self.n).map(|o| Op { step: Step::Backup { m: o }, t: 0 }).collect();
            for (i, kind) in [Kind::Insert { id: None }, Kind::Bulk, Kind::Insert { id: None }]
                .into_iter()
                .enumerate()
            {
                let r = self.requests.len() as u8;
                self.requests.push(Request { key: r + 1, kind });
                self.open.push(r);
                let on = if i == 2 { peer } else { m };
                block.push(Op {
                    step: Step::Send { r, m: on, crash: None, inner: Vec::new() },
                    t: t + i as u32,
                });
            }
            block.push(Op { step: Step::Pull { to: peer, from: m }, t: 0 });
            block.push(Op { step: Step::Restore { m, nth: 0, inner }, t: 0 });
            ops.splice(at..at, block);
        }
        bias
    }
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
    relay: usize,
    s1: usize,
    replays: usize,
    resumes: usize,
    crashes: usize,
    i8_checks: usize,
    /// Schedules where a drop of `orders` reached a member during a keyed call of it.
    with_drop_mid_request: usize,
    /// Schedules with an `$inc` written on both sides of a drop of `orders`.
    with_straddle: usize,
    /// Schedules with one call's writes on both sides of a drop of `orders`.
    with_call_straddle: usize,
    i5_off: usize,
    left_by_first: usize,
    kr: usize,
    contested_refused: usize,
    contested_records: usize,
    contested_stopped: usize,
    records_restarts: usize,
    records_alone: usize,
    cursor_restarts: usize,
    past_window_offered: usize,
    rewinds: usize,
    rewinds_alone: usize,
    rewinds_widened: usize,
    rewinds_skipped: usize,
    rewinds_across_a_drop: usize,
    rewind_refused_unrecoverable: usize,
    rewind_refused_schema: usize,
    rewound_removed: usize,
    rewound_reopened: usize,
    orphaned: usize,
    delta_too_large: usize,
    cursor_dropped: usize,
    replay_unfit: usize,
}

/// Run each seed of `seeds` on both tracks, `threads` at a time; panic with the first
/// failing schedule's report.
fn sweep(seeds: std::ops::Range<u64>, len: usize, threads: usize) -> Sweep {
    let keep_going = std::env::var("IDEM_KEEP_GOING").is_ok_and(|v| v == "1");
    let (totals, failed) = sweep_listing(seeds, len, threads, keep_going);
    assert!(failed.is_empty(), "{} schedules failed:\n{}", failed.len(), failed.join("\n"));
    totals
}

/// [`sweep`], handing back the schedules `IDEM_KEEP_GOING=1` listed as failed rather than
/// failing on them, so the caller can check its coverage first.
fn sweep_listing(
    seeds: std::ops::Range<u64>,
    len: usize,
    threads: usize,
    keep_going: bool,
) -> (Sweep, Vec<String>) {
    let started = std::time::Instant::now();
    let jobs: Vec<(u64, Track)> =
        seeds.clone().flat_map(|seed| [(seed, Track::Home), (seed, Track::Anywhere)]).collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let totals = Mutex::new(Sweep::default());
    let failure: Mutex<Option<String>> = Mutex::new(None);
    // `keep_going` (`IDEM_KEEP_GOING=1`): every failing schedule is listed, one line each,
    // and the sweep runs to its end (to classify failures); it still fails at the end.
    let failed: Mutex<Vec<String>> = Mutex::new(Vec::new());
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
                        t.relay += out.relay;
                        t.s1 += out.s1;
                        t.replays += out.replays;
                        t.resumes += out.resumes;
                        t.crashes += out.crashes;
                        t.i8_checks += out.i8_checks;
                        t.with_drop_mid_request += usize::from(out.drop_mid_request > 0);
                        t.with_straddle += usize::from(out.straddled > 0);
                        t.with_call_straddle += usize::from(out.call_straddles > 0);
                        t.i5_off += out.i5_off;
                        t.left_by_first += out.left_by_first;
                        t.kr += out.kr;
                        t.contested_refused += out.contested_refused;
                        t.contested_records += out.contested_records;
                        t.contested_stopped += out.contested_stopped;
                        t.records_restarts += out.records_restarts;
                        t.records_alone += out.records_alone;
                        t.cursor_restarts += out.cursor_restarts;
                        t.past_window_offered += out.past_window_offered;
                        t.rewinds += out.rewinds;
                        t.rewinds_alone += out.rewinds_alone;
                        t.rewinds_widened += out.rewinds_widened;
                        t.rewinds_skipped += out.rewinds_skipped;
                        t.rewinds_across_a_drop += out.rewinds_across_a_drop;
                        t.rewind_refused_unrecoverable += out.rewind_refused_unrecoverable;
                        t.rewind_refused_schema += out.rewind_refused_schema;
                        t.rewound_removed += out.rewound_removed;
                        t.rewound_reopened += out.rewound_reopened;
                        t.orphaned += out.orphaned;
                        t.delta_too_large += out.delta_too_large;
                        t.cursor_dropped += out.cursor_dropped;
                        t.replay_unfit += out.replay_unfit;
                    }
                    // `IDEM_LIST=1`: each counted finding's schedule, by seed and track.
                    if std::env::var("IDEM_LIST").is_ok_and(|v| v == "1")
                        && out.relay + out.s1 + out.kr > 0
                    {
                        for n in out.notes.iter().filter(|n| {
                            n.contains("finding:") || n.contains("RELAY:") || n.contains("KR:")
                        }) {
                            eprintln!("FINDING seed {seed} {track:?}: {n}");
                        }
                    }
                    if !out.failures.is_empty() {
                        if keep_going {
                            let first = &out.failures[0];
                            let line = format!(
                                "FAILED seed {seed} track {track:?}: {:?}: {}",
                                first.inv, first.detail
                            );
                            eprintln!("{line}");
                            failed.lock().unwrap().push(line);
                            continue;
                        }
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
         between commits), {} replays, {} resumes, {} crashes, {} relay, {} S1, {} KR; drops \
         of orders: {} schedules with one during a call, {} with an `$inc` written on both \
         sides of one, {} with one call's writes on both sides; {} I8 checks, {} counters \
         left at or above a merged first, {} home runs without exact I5; the contested \
         algebra: {} records contested, {} lookups refused, {} requests stopped between chunks; {} snapshot pages \
         refused at the zero records base, {} replay pages of records alone, {} \
         records cursors started over, {} records offered past their window; {} \
         member rewinds ({} of one member alone, {} widened to every member, {} not taken \
         on the home track; {} past a drop; refused: {} with no value at the target, {} \
         past a schema change; {} records removed, {} reopened), {} records orphaned; {} replay deltas \
         too large for their page ({} cursors dropped, their position read again), {} replay \
         pages unfit; in {:.1?}",
        totals.schedules,
        totals.steps,
        totals.between,
        totals.replays,
        totals.resumes,
        totals.crashes,
        totals.relay,
        totals.s1,
        totals.kr,
        totals.with_drop_mid_request,
        totals.with_straddle,
        totals.with_call_straddle,
        totals.i8_checks,
        totals.left_by_first,
        totals.i5_off,
        totals.contested_records,
        totals.contested_refused,
        totals.contested_stopped,
        totals.records_restarts,
        totals.records_alone,
        totals.cursor_restarts,
        totals.past_window_offered,
        totals.rewinds,
        totals.rewinds_alone,
        totals.rewinds_widened,
        totals.rewinds_skipped,
        totals.rewinds_across_a_drop,
        totals.rewind_refused_unrecoverable,
        totals.rewind_refused_schema,
        totals.rewound_removed,
        totals.rewound_reopened,
        totals.orphaned,
        totals.delta_too_large,
        totals.cursor_dropped,
        totals.replay_unfit,
        started.elapsed()
    );
    if let Some(report) = failure.into_inner().unwrap() {
        panic!("{report}");
    }
    (totals, failed.into_inner().unwrap())
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
    let count = env_u64("IDEM_CI_SEEDS", 16);
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
    let keep_going = std::env::var("IDEM_KEEP_GOING").is_ok_and(|v| v == "1");
    let (totals, failed) = sweep_listing(start..start + count, LEN, threads(), keep_going);
    // The coverage first: a run whose schedules failed still says what it reached.
    assert_coverage(&totals, count);
    assert!(failed.is_empty(), "{} schedules failed:\n{}", failed.len(), failed.join("\n"));
}

/// The long run's coverage: every path the generator's bias exists for is reached at
/// least `IDEM_MIN_EVENTS` times (default 20) per 1,000 seeds, scaled to the run, so a
/// later change to the generator cannot lose one without the run saying so.
fn assert_coverage(totals: &Sweep, seeds: u64) {
    let per_thousand = env_u64("IDEM_MIN_EVENTS", 20);
    let floor = (per_thousand * seeds).div_ceil(1000) as usize;
    let events = [
        ("requests stopped between chunks (contested)", totals.contested_stopped),
        ("lookups refused on a contested record", totals.contested_refused),
        ("snapshot pages refused at the zero records base", totals.records_restarts),
        ("replay pages of records alone", totals.records_alone),
        ("records cursors started over (another epoch)", totals.cursor_restarts),
        (
            "replay deltas too large, the cursor dropped and the position read again",
            totals.cursor_dropped,
        ),
        ("replay pages unfit, the whole-frame refusal", totals.replay_unfit),
        ("records offered past their window", totals.past_window_offered),
        ("rewinds of one member, no peer holding a write it undoes", totals.rewinds_alone),
        ("rewinds widened to every member", totals.rewinds_widened),
        (
            "member rewinds past a drop (a dropped life judged, ADR-248)",
            totals.rewinds_across_a_drop,
        ),
        (
            "rewinds refused for a value not held at the target (a replacement, an ended life)",
            totals.rewind_refused_unrecoverable,
        ),
        ("records orphaned", totals.orphaned),
    ];
    let short: Vec<String> =
        events.iter().filter(|(_, n)| *n < floor).map(|(what, n)| format!("{what}: {n}")).collect();
    assert!(
        short.is_empty(),
        "the generator reached {} path(s) fewer than {floor} times in {seeds} seeds: {}",
        short.len(),
        short.join("; ")
    );
}

/// One seed, for a failure's report: `IDEM_SEED`, `IDEM_TRACK` (`home` or anything
/// else for anywhere), and `IDEM_DROP`, steps to leave out.
#[test]
#[ignore = "one seed of the model, printed: IDEM_SEED and IDEM_TRACK"]
fn one_seed() {
    let seed = env_u64("IDEM_SEED", 0);
    let track = match std::env::var("IDEM_TRACK").as_deref() {
        Ok("home") => Track::Home,
        _ => Track::Anywhere,
    };
    let mut sched = generate(seed, track, LEN);
    // `IDEM_DROP=3,17`: the schedule without those steps, to minimise a failure.
    if let Ok(drop) = std::env::var("IDEM_DROP") {
        let gone: HashSet<usize> = drop.split(',').filter_map(|i| i.trim().parse().ok()).collect();
        sched.ops = sched
            .ops
            .into_iter()
            .enumerate()
            .filter(|(i, _)| !gone.contains(i))
            .map(|(_, op)| op)
            .collect();
    }
    let out = run(&sched);
    eprintln!("{}", render(Some(seed), &sched, &out));
    assert!(out.failures.is_empty());
}

// -- Named rows ------------------------------------------------------------------------------
//
// Schedules written out, each run strictly (every classified finding fails it). The
// first group are the breaks an analysis of relays without the record found, and must
// fail until their fix lands: records travel with coverage (relays after a snapshot, of a
// losing entry, of a coverage-only window, across origins), a records delta
// on every page (R1, R1b, R1c). The second group are the shapes the design's mutants
// need, and pass today.

fn req(key: u8, kind: Kind) -> Request {
    Request { key, kind }
}

fn ask(r: u8, m: u8, t: u32) -> Op {
    Op { step: Step::Send { r, m, crash: None, inner: Vec::new() }, t }
}

fn ask_crash(r: u8, m: u8, t: u32, j: u8, how: Crash) -> Op {
    Op { step: Step::Send { r, m, crash: Some((j, how)), inner: Vec::new() }, t }
}

fn ask_between(r: u8, m: u8, t: u32, inner: Vec<(u8, Vec<Op>)>) -> Op {
    Op { step: Step::Send { r, m, crash: None, inner }, t }
}

fn pull(to: u8, from: u8) -> Op {
    Op { step: Step::Pull { to, from }, t: 0 }
}

fn touch(m: u8, c: u8, d: u16, t: u32) -> Op {
    Op { step: Step::Touch { m, c, d }, t }
}

fn down(m: u8) -> Op {
    Op { step: Step::Down { m }, t: 0 }
}

fn big(m: u8) -> Op {
    Op { step: Step::Big { m }, t: 0 }
}

fn scoped(to: u8, from: u8, c: u8, inner: Vec<(u8, Vec<Op>)>) -> Op {
    Op { step: Step::Page { to, from, c, converge: false, restart: false, inner }, t: 0 }
}

fn reset(m: u8, from: u8, inner: Vec<(u8, Vec<Op>)>) -> Op {
    Op { step: Step::Reset { m, from, inner }, t: 0 }
}

fn backup(m: u8) -> Op {
    Op { step: Step::Backup { m }, t: 0 }
}

fn restore(m: u8, nth: u8, inner: Vec<(u8, Vec<Op>)>) -> Op {
    Op { step: Step::Restore { m, nth, inner }, t: 0 }
}

fn retention(m: u8, t: u32) -> Op {
    Op { step: Step::Retention { m }, t }
}

fn orphan(m: u8) -> Op {
    Op { step: Step::Orphan { m }, t: 0 }
}

fn rewind(m: u8, t: u32) -> Op {
    Op { step: Step::Rewind { m }, t }
}

fn row(name: &str, members: u8, requests: Vec<Request>, ops: Vec<Op>) {
    row_with(name, members, requests, ops, false);
}

fn row_with(name: &str, members: u8, requests: Vec<Request>, ops: Vec<Op>, at_completion: bool) {
    let sched =
        Schedule { track: Track::Anywhere, home: 0, members, requests, ops, bias: Bias::default() };
    let out = run_with(&sched, Some(true), at_completion);
    if trace() {
        eprintln!("row {name}:\n{}", render(None, &sched, &out));
    }
    assert!(out.failures.is_empty(), "row {name}:\n{}", render(None, &sched, &out));
}

/// A named row (three members, run strictly) whose outcome must also pass `holds`.
fn row_holding(name: &str, requests: Vec<Request>, ops: Vec<Op>, holds: impl Fn(&Outcome) -> bool) {
    row_framed(name, REPLAY_FRAME, requests, ops, holds);
}

/// [`row_holding`], with the model's replay frame of `frame` units.
fn row_framed(
    name: &str,
    frame: usize,
    requests: Vec<Request>,
    ops: Vec<Op>,
    holds: impl Fn(&Outcome) -> bool,
) {
    let bias = Bias { frame, ..Bias::default() };
    let sched = Schedule { track: Track::Anywhere, home: 0, members: 3, requests, ops, bias };
    let out = run_framed(&sched, Some(true), false, frame);
    let shown = render(None, &sched, &out);
    if trace() {
        eprintln!("row {name}:\n{shown}");
    }
    assert!(out.failures.is_empty() && holds(&out), "row {name}:\n{shown}");
}

const A: u8 = 0;
const B: u8 = 1;
const C: u8 = 2;
const D: u8 = 3;

/// A losing entry relayed: B writes the counter just after A's keyed `$inc`, so the `$inc`
/// loses on B (its record stored, its entry not appended); A's write on top of the `$inc`
/// wins there. C pulls B, holds the effect, and its coverage of A passes the `$inc`'s
/// entry, which A then never serves it. A retry on C must replay.
#[test]
fn row_g2_a_loser_relay() {
    row(
        "g2",
        3,
        vec![req(1, Kind::IncSingle { d: 0 })],
        vec![
            ask(0, A, 100),
            touch(B, ORDERS, 0, 101),
            touch(A, ORDERS, 0, 110),
            pull(B, A),
            pull(C, B),
            ask(0, C, 120),
        ],
    );
}

/// A losing first entry: a keyed `$inc` of two counters, one commit; B's own write beats
/// its first entry, which carries the record, and its second entry wins on B. C pulls B and
/// holds the request's own effect on the second counter, bare. A retry on C must not apply
/// again.
#[test]
fn row_g2b_a_multi_document_request_whose_first_entry_lost() {
    row(
        "g2b",
        3,
        vec![req(1, Kind::IncRange { d: 0 })],
        vec![ask(0, A, 100), touch(B, ORDERS, 0, 101), pull(B, A), pull(C, B), ask(0, C, 120)],
    );
}

/// A coverage-only window: the `$inc` loses on B, which then appends A's write to another
/// collection; C's coverage of A passes the `$inc` in a window that carries no entry of its
/// collection. A's later write on top reaches C bare. A retry on C must replay.
#[test]
fn row_g3_a_coverage_only_window() {
    row(
        "g3",
        3,
        vec![req(1, Kind::IncSingle { d: 0 })],
        vec![
            ask(0, A, 100),
            touch(B, ORDERS, 0, 101),
            touch(A, ITEMS, 0, 105),
            pull(B, A),
            pull(C, B),
            touch(A, ORDERS, 0, 110),
            pull(C, A),
            ask(0, C, 120),
        ],
    );
}

/// A cross-origin relay: A's `$inc` loses on C to C's own write, so C's servable vector at
/// A stays below it; B took the `$inc` and wrote on top, and that version wins on C. With A
/// and B down, D pulls only C and holds B's version, which holds the `$inc`, with no
/// record, while its coverage of A does not move. A retry on D must replay.
#[test]
fn row_t2_a_cross_origin_relay_while_the_origin_is_down() {
    row(
        "t2",
        4,
        vec![req(1, Kind::IncSingle { d: 0 })],
        vec![
            ask(0, A, 100),
            touch(C, ORDERS, 0, 101),
            pull(B, A),
            touch(B, ORDERS, 0, 110),
            pull(C, A),
            pull(C, B),
            down(A),
            down(B),
            pull(D, C),
            ask(0, D, 120),
        ],
    );
}

/// A relay after a snapshot: A's keyed write and a write on top land between B's first
/// snapshot page and the collection's records phase, so B stores the record from the page
/// and holds the later version above its grant; B then pulls A, where the keyed entry
/// loses. C, at the grant, pulls B and takes the later version bare. A retry on C must
/// replay.
#[test]
fn row_g1b_a_write_between_snapshot_pages_then_a_relay() {
    // The keyed write goes to the collection the whole-database walk reaches second, so
    // that the first page ends before its records phase; an earlier keyed write gives
    // that collection records, so the page ends at its boundary.
    let (c, early, keyed) = if coll_id(ORDERS) < coll_id(ITEMS) {
        (ITEMS, Kind::Insert { id: Some(1) }, Kind::Replace { at: Target::Client(0) })
    } else {
        (ORDERS, Kind::IncSingle { d: 4 }, Kind::IncSingle { d: 0 })
    };
    row(
        "g1b",
        3,
        vec![req(1, early), req(2, keyed)],
        vec![
            ask(0, A, 100),
            pull(C, A),
            reset(B, A, vec![(1, vec![ask(1, A, 110), touch(A, c, 0, 120)])]),
            pull(B, A),
            pull(C, B),
            ask(1, C, 130),
        ],
    );
}

/// R1: a scoped pull of `big`: its records phase and first document page, then a keyed
/// `$inc` on the sender of a document on the second page. The receiver holds the effect
/// after the pull, and a retry there must replay.
#[test]
fn row_r1_a_keyed_write_between_the_records_phase_and_its_page() {
    row(
        "r1",
        3,
        vec![req(1, Kind::IncBig { d: 0 }), req(2, Kind::IncBig { d: 1000 })],
        vec![
            big(A),
            ask(0, A, 100),
            scoped(B, A, BIG, vec![(1, vec![ask(1, A, 110)])]),
            ask(1, B, 120),
        ],
    );
}

/// R1b: as R1, with the retry on the receiver between the page that brings the effect
/// and the end of the pull.
#[test]
fn row_r1b_a_retry_between_the_effects_page_and_the_pulls_end() {
    row(
        "r1b",
        3,
        vec![req(1, Kind::IncBig { d: 0 }), req(2, Kind::IncBig { d: 600 })],
        vec![
            big(A),
            ask(0, A, 100),
            scoped(B, A, BIG, vec![(1, vec![ask(1, A, 110)]), (2, vec![ask(1, B, 120)])]),
        ],
    );
}

/// R1b, relayed: a peer takes a scoped page of the collection from the receiver between
/// the effect's page and the pull's end (the receiver's own held entries are above its
/// servable vector, so an ordinary pull defers them), and a retry there must replay.
#[test]
fn row_r1b_a_peer_takes_a_page_from_the_receiver_mid_pull() {
    row(
        "r1b-relay",
        3,
        vec![req(1, Kind::IncBig { d: 0 }), req(2, Kind::IncBig { d: 600 })],
        vec![
            big(A),
            ask(0, A, 100),
            pull(C, A),
            scoped(
                B,
                A,
                BIG,
                vec![(1, vec![ask(1, A, 110)]), (2, vec![scoped(C, B, BIG, Vec::new())])],
            ),
            ask(1, C, 130),
        ],
    );
}

/// R2, by page: a member serving a scoped pull takes a keyed write's effect and record
/// by a page of its own scoped pull from the writer, between two pages it serves; the
/// effect reaches the requester on a later page, and its record must ride that page's
/// delta (the page store writes a change-log row). A retry on the requester must replay.
#[test]
fn row_r2_a_record_taken_by_page_rides_the_next_page_its_member_serves() {
    row(
        "r2-page",
        3,
        vec![req(1, Kind::IncBig { d: 0 }), req(2, Kind::IncBig { d: 1200 })],
        vec![
            big(A),
            ask(0, A, 100),
            scoped(B, A, BIG, Vec::new()),
            scoped(C, B, BIG, vec![(1, vec![ask(1, A, 110), scoped(B, A, BIG, Vec::new())])]),
            ask(1, C, 130),
        ],
    );
}

/// R2, by frame: as above, with the serving member taking the write by a pulled window
/// (the frame store writes a change-log row).
#[test]
fn row_r2_a_record_taken_by_frame_rides_the_next_page_its_member_serves() {
    row(
        "r2-frame",
        3,
        vec![req(1, Kind::IncBig { d: 0 }), req(2, Kind::IncBig { d: 1200 })],
        vec![
            big(A),
            ask(0, A, 100),
            scoped(B, A, BIG, Vec::new()),
            scoped(C, B, BIG, vec![(1, vec![ask(1, A, 110), pull(B, A)])]),
            ask(1, C, 130),
        ],
    );
}

/// R2 across a restart: B, its clock moved forward before it took a record by page,
/// serves C a scoped pull, restarts between C's pages, and takes a write stamped behind
/// that clock by frame; the record must ride the page that brings its effect. (Where
/// the clock resumes is pinned by `the_clock_resumes_above_the_change_logs_keys_after_a_restart`:
/// this row's other stamps at B's moved clock keep it above the base without the log.)
#[test]
fn row_r2_a_restart_between_the_pages_a_member_serves() {
    row(
        "r2-clock",
        3,
        vec![req(1, Kind::IncBig { d: 0 }), req(2, Kind::IncBig { d: 1200 })],
        vec![
            big(A),
            ask(0, A, 100),
            Op { step: Step::Clock { m: B }, t: 200 },
            scoped(B, A, BIG, Vec::new()),
            scoped(
                C,
                B,
                BIG,
                vec![(
                    1,
                    vec![Op { step: Step::Restart { m: B }, t: 0 }, ask(1, A, 115), pull(B, A)],
                )],
            ),
            ask(1, C, 130),
        ],
    );
}

/// R1c: a whole-database snapshot whose later page brings an effect whose record came
/// after the collection's records phase; a retry at completion (what the catching-up
/// gate allows once it opens as `unknown`) must replay.
#[test]
fn row_r1c_a_whole_database_retry_at_completion() {
    row_with(
        "r1c",
        3,
        vec![req(1, Kind::IncBig { d: 0 }), req(2, Kind::IncBig { d: 1500 })],
        vec![big(A), ask(0, A, 100), reset(B, A, vec![(2, vec![ask(1, A, 110)])]), ask(1, B, 120)],
        true,
    );
}

/// A replay: A is restored from a backup taken before its keyed `$inc` and the write it made on
/// top. B's copy of the `$inc` lost (C's write came first), and B holds A's write on top;
/// A's own-origin replay from B reads that write back, without the record. A is the
/// member guarantee 4 tells the client to retry on, and a retry there must replay.
#[test]
fn row_g5_an_own_origin_replay_after_a_restore() {
    row(
        "g5",
        3,
        vec![req(1, Kind::IncSingle { d: 0 })],
        vec![
            Op { step: Step::Backup { m: A }, t: 0 },
            ask(0, A, 100),
            touch(C, ORDERS, 0, 101),
            pull(B, C),
            touch(A, ORDERS, 0, 110),
            pull(B, A),
            Op { step: Step::Restore { m: A, nth: 0, inner: Vec::new() }, t: 0 },
            ask(0, A, 120),
        ],
    );
}

/// F1: a window cut inside a commit. A writes an unkeyed version, then a keyed `$inc`
/// of two counters in one commit, whose record's `last` is its second entry (L1). B's
/// short pull (two entries) ends at the `$inc`'s first entry, so B holds that counter's
/// effect and covers that stamp; the record's row there (its `first`) ships it with the
/// window. A retry on B must replay.
#[test]
fn row_f1_a_window_cut_inside_a_commit() {
    row(
        "f1",
        3,
        vec![req(1, Kind::IncRange { d: 0 })],
        vec![
            touch(A, ITEMS, 0, 100),
            ask(0, A, 110),
            Op { step: Step::PullShort { to: B, from: A }, t: 0 },
            ask(0, B, 120),
        ],
    );
}

/// A record of the life after a drop is not purged as the dropped life's: B drops
/// `items`; A, before it hears of the drop, replaces a document with a key, stamped
/// after the drop (so the version stands in the next life, ADR-236); B creates `items`
/// again; C takes the drop, the replace with its record, and the creation in one window.
/// The drop's purge on C must leave that record, and a retry on C must replay.
#[test]
fn row_a_record_of_the_life_after_a_drop_survives_its_purge() {
    row_holding(
        "purge",
        vec![req(1, Kind::Replace { at: Target::Client(0) })],
        vec![
            ask(0, C, 140),
            Op { step: Step::Drop { m: B, c: ITEMS }, t: 210 },
            ask(0, A, 220),
            Op { step: Step::Create { m: B, c: ITEMS }, t: 250 },
            pull(A, B),
            pull(C, A),
            ask(0, C, 260),
        ],
        |out| out.replays >= 1,
    );
}

/// G8: a keyed `$inc` of two counters fills its one chunk, and its lookahead finds the
/// end, so the chunk's own record says `done`. B pulls right after that commit: it holds
/// `done`, and a retry there replays (no record-only `done` it could never receive).
#[test]
fn row_g8_a_member_that_pulls_the_last_chunk_gets_done_with_it() {
    row(
        "g8",
        3,
        vec![req(1, Kind::IncRange { d: 0 })],
        vec![ask_between(0, A, 100, vec![(1, vec![pull(B, A)])]), ask(0, B, 110)],
    );
}

/// G8, past matches the stamp rule leaves: counter 4 is written after the request's
/// first write, so it is left; the second chunk fills its budget and its lookahead finds
/// only that left match before the end, so it writes `done`. B, pulling right after it,
/// holds `done`, and a retry there replays.
#[test]
fn row_g8_the_lookahead_ends_past_a_left_match() {
    row(
        "g8-left",
        3,
        vec![req(1, Kind::IncMulti)],
        vec![
            ask_between(
                0,
                A,
                100,
                vec![(1, vec![touch(A, ORDERS, 4, 103)]), (2, vec![pull(B, A)])],
            ),
            ask(0, B, 110),
        ],
    );
}

/// Concurrent attempts on two members (Chris, 2026-10-18): A's fresh multi-chunk attempt
/// commits its first chunk; B, without A's record, runs the request whole; A pulls B. A's
/// later chunks must leave every counter B's attempt wrote (the stamp rule applies to a
/// fresh attempt's later chunks, not only to a resume).
#[test]
fn row_concurrent_fresh_attempts_on_two_members() {
    row(
        "concurrent",
        3,
        vec![req(1, Kind::IncMulti)],
        vec![ask_between(0, A, 100, vec![(1, vec![ask(0, B, 105), pull(A, B)])])],
    );
}

/// The merged `first` is the earliest attempt's: A's attempt stops after a chunk, A
/// resumes and stops again; B, without A's record, starts afresh later and stops; after
/// the pulls the merged record's `last` is B's. A resume must still leave what A's resume
/// wrote, which is below B's `first`.
#[test]
fn row_independent_attempts_merge_their_firsts() {
    row(
        "merge-first",
        3,
        vec![req(1, Kind::IncMulti)],
        vec![
            ask_crash(0, A, 100, 1, Crash::AfterCommit),
            ask_crash(0, A, 105, 1, Crash::AfterCommit),
            ask_crash(0, B, 110, 1, Crash::AfterCommit),
            pull(A, B),
            pull(B, A),
            ask(0, B, 120),
            pull(C, B),
            ask(0, A, 130),
        ],
    );
}

/// The record is in the write's own transaction: a crash at the commit after a
/// single-document `$inc`'s, then a retry on the same member, must replay; and a pull
/// right after that commit carries the record.
#[test]
fn row_a_crash_or_a_pull_right_after_the_commit() {
    row(
        "crash-after-commit",
        3,
        vec![req(1, Kind::IncSingle { d: 0 }), req(2, Kind::IncSingle { d: 1 })],
        vec![
            ask_crash(0, A, 100, 1, Crash::BeforeCommit),
            ask(0, A, 110),
            ask_between(1, A, 120, vec![(1, vec![pull(B, A)])]),
            ask(1, B, 130),
        ],
    );
}

/// A record a scoped page brings from a sender whose clock is ahead: the receiver
/// witnesses its stamps and keeps them across a restart, so its resume and every later
/// one write above the request's `first`.
#[test]
fn row_a_page_carried_record_resumed_after_a_restart() {
    row(
        "page-record-restart",
        3,
        vec![req(1, Kind::IncMulti)],
        vec![
            ask_crash(0, A, 500, 1, Crash::AfterCommit),
            Op {
                step: Step::Page {
                    to: B,
                    from: A,
                    c: ORDERS,
                    converge: false,
                    restart: true,
                    inner: Vec::new(),
                },
                t: 0,
            },
            ask_crash(0, B, 120, 1, Crash::AfterCommit),
            pull(A, B),
            ask(0, A, 130),
            pull(C, A),
            ask(0, C, 140),
        ],
    );
}

/// A record outlives a drop of its collection for its window. A record of the
/// life a drop ended, arriving from a member that applied the request before it heard of
/// the drop, is replayed in the life after it, and nothing is written there.
#[test]
fn row_a_retry_after_a_drop_and_a_recreation_replays() {
    row_holding(
        "life",
        vec![req(1, Kind::Insert { id: Some(0) })],
        vec![
            ask(0, A, 100),
            ask(0, B, 105),
            Op { step: Step::Drop { m: A, c: ITEMS }, t: 110 },
            Op { step: Step::Create { m: A, c: ITEMS }, t: 120 },
            pull(A, B),
            ask(0, A, 130),
        ],
        |out| out.replays >= 1,
    );
}

/// The derived `_id` holds the request's fingerprint: a key reused for another
/// insert on a member without the first one's record lands on another document.
#[test]
fn row_a_reused_key_on_a_member_without_the_record() {
    row(
        "reuse",
        3,
        vec![req(1, Kind::Insert { id: None }), req(1, Kind::Insert { id: None })],
        vec![ask(0, A, 100), ask(1, B, 110)],
    );
}

/// The derived `_id` is the request's: a retry on a member without the record lands on
/// the same document key, and one document remains.
#[test]
fn row_a_retry_on_a_member_without_the_record_derives_the_same_id() {
    row(
        "derived",
        3,
        vec![req(1, Kind::Insert { id: None }), req(2, Kind::Bulk)],
        vec![ask(0, A, 100), ask(0, B, 110), ask(1, A, 120), ask(1, C, 130)],
    );
}

/// A record is stored when its entry loses: B holds a newer, independent version of the
/// counter, A's keyed `$inc` loses there, and B must hold its record.
#[test]
fn row_a_losing_entry_stores_its_record() {
    row(
        "loser",
        3,
        vec![req(1, Kind::IncSingle { d: 0 })],
        vec![ask(0, A, 100), touch(B, ORDERS, 0, 120), pull(B, A), ask(0, B, 130)],
    );
}

/// A snapshot carries the records: a scoped page of `orders` brings the `$inc`'s record
/// with its effect, and a retry on the receiver replays.
#[test]
fn row_a_scoped_page_carries_the_record() {
    row(
        "scoped",
        3,
        vec![req(1, Kind::IncSingle { d: 0 })],
        vec![ask(0, A, 100), scoped(B, A, ORDERS, Vec::new()), ask(0, B, 110)],
    );
}

/// Records merge as a join: a member that holds an attempt's `applying` record and then
/// pulls the request's resumed chunks ends with the `done` record every member holds.
#[test]
fn row_a_resumed_requests_record_reaches_every_member() {
    row(
        "join",
        3,
        vec![req(1, Kind::IncMulti)],
        vec![
            ask_crash(0, A, 100, 1, Crash::AfterCommit),
            pull(B, A),
            ask(0, A, 110),
            pull(B, A),
            pull(C, B),
            ask(0, C, 120),
        ],
    );
}

// -- Reproducers -------------------------------------------------------------------------------

/// A retry after a drop (records outlive a drop for their window). A applies an `$inc` of
/// every counter and B takes it with its record; A drops `orders`; B, before it hears of
/// the drop, writes counter 0 (unkeyed) on top of its version, which carries the `$inc`,
/// stamped after the drop. B then takes the drop, which keeps that write (ADR-236). A
/// retry on B replays the record: the request never lands twice on counter 0.
#[test]
fn row_d2_a_retry_after_a_drop_replays_and_never_doubles() {
    row_holding(
        "d2",
        vec![req(1, Kind::IncMulti)],
        vec![
            ask(0, A, 100),
            pull(B, A),
            Op { step: Step::Drop { m: A, c: ORDERS }, t: 150 },
            touch(B, ORDERS, 0, 160),
            pull(B, A),
            ask(0, B, 170),
        ],
        |out| out.replays >= 1,
    );
}

/// A contested key: r0 and r1 are two `$inc`s of every counter under one key. C applies r0; A,
/// without r0's record, applies r1 and stops after its first chunk (counters 0 and 1); A
/// then takes r0's effect and record from C, and the two records merge: the merged
/// record is `contested`, as the algebra says (two requests merged). A retry of r1 on A
/// is refused (422) as the algebra predicts, and nothing completes on r0's `first`.
#[test]
fn row_kr_a_reused_keys_merged_records_refuse() {
    row_holding(
        "kr",
        vec![req(1, Kind::IncMulti), req(1, Kind::IncMulti)],
        vec![
            ask(0, C, 100),
            ask_crash(1, A, 130, 1, Crash::AfterCommit),
            pull(A, C),
            ask(1, A, 140),
            pull(B, A),
            ask(0, B, 150),
        ],
        |out| out.contested_refused >= 2,
    );
}

/// A contested key between chunks: as above, with A taking r0's record from C between r1's
/// first and second chunks. The record turns contested there, and r1's next chunk is
/// refused: it stops partially applied, its first chunk standing, and every retry on A is
/// refused.
#[test]
fn row_kr_a_record_contested_between_chunks_stops_the_request() {
    row_holding(
        "kr-chunks",
        vec![req(1, Kind::IncMulti), req(1, Kind::IncMulti)],
        vec![
            ask(0, C, 100),
            ask_between(1, A, 130, vec![(1, vec![pull(A, C)])]),
            ask(1, A, 140),
            ask(0, A, 145),
        ],
        |out| out.contested_stopped == 1 && out.contested_refused >= 2,
    );
}

// -- Restores during a pull, the records cursor, past-window records, rewinds, orphan rows --

/// A restore during a pull, with records: A's backup B0 holds no record and B1 holds r0's
/// (its effect on a late page of `big`). A is restored to B0 (no record, no change log),
/// and B takes a scoped pull of `big` from it; between the first two pages A is restored to
/// B1, and opens over B1's records with a fresh epoch (the epoch begun at open). B's next
/// page, asked with the zero records base, comes from a sender whose log has begun since
/// and is refused (the zero-base guard): `big` is asked for again from its records phase,
/// which carries r0's record before its effect. A retry on B replays.
#[test]
fn row_m4_a_sender_restored_between_pages_onto_records() {
    row_holding(
        "m4-records",
        vec![req(1, Kind::IncBig { d: 1200 })],
        vec![
            big(A),
            backup(A),
            ask(0, A, 100),
            backup(A),
            restore(A, 1, Vec::new()),
            scoped(B, A, BIG, vec![(1, vec![restore(A, 0, Vec::new())])]),
            ask(0, B, 130),
        ],
        |out| out.records_restarts >= 1 && out.replays >= 1,
    );
}

/// Two restores of the sender during one pull: B's first page of `big` from A (r0's
/// record in its records phase, the base of A's log); A restored to B0 (no record, no
/// log: the base is of another epoch, and A starts `big` over); A restored to B1 (its
/// records, a fresh epoch: the zero base B now holds is refused, and `big` starts over
/// from its records). A retry on B replays.
#[test]
fn row_m4_two_restores_of_the_sender_during_one_pull() {
    row_holding(
        "m4-twice",
        vec![req(1, Kind::IncBig { d: 1200 })],
        vec![
            big(A),
            backup(A),
            ask(0, A, 100),
            backup(A),
            scoped(
                B,
                A,
                BIG,
                vec![(1, vec![restore(A, 1, Vec::new())]), (2, vec![restore(A, 0, Vec::new())])],
            ),
            ask(0, B, 130),
        ],
        |out| out.records_restarts >= 1 && out.replays >= 1,
    );
}

/// The records cursor: C applies r0 to r2 and B takes them; A applies r3, B takes it, and A
/// is restored from a backup before all of it. A's replay from B: its one lost entry needs
/// four records (three spanning records of C, carried at their `first` below it, and its
/// own), more than the model's frame of three holds with it, so the first page carries
/// three records alone under a cursor, and the next page at the same position the rest with
/// the entry. Retries on A replay.
#[test]
fn row_m1_a_replay_page_of_records_alone_under_a_cursor() {
    row_framed(
        "replay-cursor",
        3,
        replay_requests(),
        vec![
            backup(A),
            ask(0, C, 90),
            ask(1, C, 91),
            ask(2, C, 92),
            pull(B, C),
            ask(3, A, 100),
            pull(B, A),
            restore(A, 0, Vec::new()),
            ask(3, A, 120),
            ask(0, A, 121),
        ],
        |out| out.records_alone >= 1 && out.replays >= 2,
    );
}

/// The records cursor guarded by the epoch: as above, with B, the sender, restored from its
/// backup between A's two replay pages. B opens over its records with a fresh epoch, so
/// the cursor A holds is of another log and the position's records start over.
#[test]
fn row_m1_a_records_cursor_of_a_restored_sender_starts_over() {
    row_framed(
        "replay-cursor-epoch",
        3,
        replay_requests(),
        vec![
            backup(A),
            ask(0, C, 90),
            ask(1, C, 91),
            ask(2, C, 92),
            pull(B, C),
            ask(3, A, 100),
            pull(B, A),
            backup(B),
            restore(A, 0, vec![(1, vec![restore(B, 0, Vec::new())])]),
            ask(3, A, 120),
            ask(0, A, 121),
        ],
        |out| out.records_alone >= 2 && out.cursor_restarts >= 1 && out.replays >= 2,
    );
}

fn replay_requests() -> Vec<Request> {
    (0..4).map(|d| req(d + 1, Kind::IncSingle { d })).collect()
}

/// A record past its window is not stored. r0 on A; the window passes (a retention
/// pass on C moves every member's clock); r1, a new request under r0's key, on B, which
/// never held r0's record; B then pulls A, whose window still carries r0's record. B
/// refuses it: merged into r1's, it would lower `first` to r0's and mark the record
/// contested. A retry of r1 on B replays.
#[test]
fn row_m2_a_record_past_its_window_is_not_stored() {
    row_holding(
        "m2",
        vec![req(1, Kind::Insert { id: Some(0) }), req(1, Kind::Insert { id: Some(1) })],
        vec![ask(0, A, 100), retention(C, 1700), ask(1, B, 1710), pull(B, A), ask(1, B, 1720)],
        |out| out.past_window_offered >= 1 && out.replays >= 1,
    );
}

/// A record past its window of the live record's own request
/// merges. r0 on A at 100; B, without A's record, retries r0 at 1500; the window passes
/// for A's record (a retention pass on C at 1700) and not for B's; B pulls A, whose
/// window carries r0's record past its window: B merges it, its `first` now A's.
#[test]
fn row_m2_a_record_past_its_window_of_the_same_request_merges() {
    row_holding(
        "m2-same",
        vec![req(1, Kind::Replace { at: Target::Client(0) })],
        vec![ask(0, A, 100), ask(0, B, 1500), retention(C, 1700), pull(B, A)],
        |out| out.past_window_merged >= 1,
    );
}

/// The rewind as G narrowed it: a record of any kind but a bulk that a rewind cuts
/// part-way is reopened, its rows kept, and replays. r0, an insert, applies on A at 100
/// and, without A's record, on B at 200; A pulls B, and its record runs from A's 100 to
/// B's 200. A rewind of A to 150 cuts it: reopened `applying`, it replays a retry on A,
/// and A's insert at 100 stands with its record.
#[test]
fn row_rewind_reopens_a_merged_insert() {
    row_holding(
        "rewind-insert",
        vec![req(1, Kind::Insert { id: None })],
        vec![ask(0, A, 100), ask(0, B, 200), pull(A, B), rewind(A, 150), ask(0, A, 210)],
        |out| out.rewound_reopened >= 1 && out.replays >= 1,
    );
}

/// The rewind: r0, an `$inc` of every counter, stops after its first chunk
/// at 100 and resumes to its end at 200; r1 inserts at 140; r2, a bulk, at 160. A
/// rewind to 150 removes r2's record with its rows (every write discarded), keeps r1's,
/// and reopens r0's `applying` with no cursor (a filter's, cut part-way). The resume
/// writes what the rewind undid and nothing that stands; r2 runs afresh; r1 replays.
#[test]
fn row_rewind_removes_what_it_discards_and_reopens_a_filter() {
    row_holding(
        "rewind",
        vec![req(1, Kind::IncMulti), req(2, Kind::Insert { id: Some(0) }), req(3, Kind::Bulk)],
        vec![
            ask_crash(0, A, 100, 1, Crash::AfterCommit),
            ask(1, A, 140),
            ask(2, A, 160),
            ask(0, A, 200),
            rewind(A, 150),
            ask(0, A, 210),
            ask(2, A, 220),
            ask(1, A, 230),
        ],
        |out| {
            out.rewound_removed >= 1
                && out.rewound_reopened >= 1
                && out.replays >= 1
                && out.resumes >= 2
        },
    );
}

/// The rewind cuts a bulk part-way: its first document, minted first in the
/// millisecond the rewind keeps, stands, and its record goes with its rows; a retry is a
/// fresh attempt that meets that document as a duplicate and writes nothing.
#[test]
fn row_rewind_that_cuts_a_bulk_part_way_removes_its_record() {
    row_holding(
        "rewind-bulk",
        vec![req(1, Kind::Bulk)],
        vec![ask(0, A, 160), rewind(A, 160), ask(0, A, 170), pull(B, A), ask(0, B, 180)],
        |out| out.rewound_removed == 1,
    );
}

/// Orphan rows: B holds r0's record with its row at A's stamp; the window passes and B
/// removes the record and leaves its rows (a retention pass stopped between chunks). r1,
/// a new request under the key, on B: the orphan row now names r1's record, below its
/// `first`, and vouches for nothing of it. Every path passes over it and ships nothing
/// at it: C's pull and scoped page from B, B's retention pass, B's replay after a
/// restore. A retry of r1 on C replays.
#[test]
fn row_an_orphan_row_is_passed_over_everywhere() {
    row_holding(
        "orphan",
        vec![req(1, Kind::Insert { id: Some(0) }), req(1, Kind::Insert { id: Some(1) })],
        vec![
            backup(B),
            ask(0, A, 100),
            pull(B, A),
            retention(C, 1700),
            orphan(B),
            ask(1, B, 1710),
            pull(C, B),
            scoped(C, B, ITEMS, Vec::new()),
            ask(1, C, 1720),
            retention(B, 1730),
            restore(B, 0, Vec::new()),
        ],
        |out| out.orphaned >= 1 && out.replays >= 1,
    );
}

/// A resume across a drop, the stamp rule's contract: A applies an `$inc` of
/// every counter; B drops `orders` and creates it again, its counters fresh; C, before it
/// hears of either, applies the same request, stamped after the drop, and stops after its
/// first chunk. C takes A's effect and record (merged: A's `first`, before the drop),
/// then the drop and the new life: C's first chunk stands (ADR-236), and counters 2 to 4
/// are B's fresh ones. A retry on C resumes on the merged `first` and leaves those three,
/// stored at or above it (counted as `skipped`): nothing is written twice, and I8 holds.
#[test]
fn row_d3_a_resume_after_a_drop_leaves_what_is_at_or_above_its_merged_first() {
    row_holding(
        "d3",
        vec![req(1, Kind::IncMulti)],
        vec![
            ask(0, A, 100),
            Op { step: Step::Drop { m: B, c: ORDERS }, t: 150 },
            Op { step: Step::Create { m: B, c: ORDERS }, t: 152 },
            ask_crash(0, C, 160, 1, Crash::AfterCommit),
            pull(C, A),
            pull(C, B),
            ask(0, C, 170),
        ],
        |out| out.left_by_first == 3,
    );
}

/// Outside the procedure (operations, "Point-in-time restore": a rewound database must
/// not rejoin a cluster that still holds the undone writes): C upserts `items/101` with
/// a key at 210 and A takes it; C is rewound alone to 206 all the same, then rejoins: it
/// writes at 240, B takes C's origin past 210 by a short pull, a converge page brings A's
/// kept version back to C, and C writes the document again on top. The undone write comes
/// back, as the documentation says. No idempotency guarantee is asserted: after such a
/// rejoin a request's record need not come back with its undone write (in the long run's
/// seed 1000180, a member ended holding a request's effect without its record).
#[test]
fn row_a_rewound_member_that_rejoins_a_cluster_holding_the_undone_write_is_outside_the_procedure() {
    let sched = Schedule {
        track: Track::Anywhere,
        home: 0,
        members: 3,
        requests: vec![req(1, Kind::Replace { at: Target::Client(1) })],
        ops: vec![
            ask(0, C, 210),
            pull(A, C),
            Op { step: Step::RewindAlone { m: C }, t: 206 },
            touch(C, ITEMS, 2, 240),
            Op { step: Step::PullShort { to: B, from: C }, t: 0 },
            Op {
                step: Step::Page {
                    to: C,
                    from: A,
                    c: ITEMS,
                    converge: true,
                    restart: true,
                    inner: Vec::new(),
                },
                t: 0,
            },
            touch(C, ITEMS, 1, 376),
        ],
        bias: Bias::default(),
    };
    let out = run_framed(&sched, Some(false), false, REPLAY_FRAME);
    let shown = render(None, &sched, &out);
    if trace() {
        eprintln!("row forbidden-rewind:\n{shown}");
    }
    assert!(out.undone_returned >= 1, "the undone write did not come back:\n{shown}");
}

/// Finding RW1 (fixed by #209), minimised from the long run's seed 1000688: B drops
/// `items`; B's retention pass collects its own drop entry; B rewinds to a moment after
/// it (nothing discarded). A, which never heard of the drop, inserts into `items`. The
/// rewind keeps B's vectors at what retention collected, so A takes the drop and every
/// member ends with the same `items`.
#[test]
fn row_rw1_a_rewind_after_retention_keeps_the_members_collected_writes() {
    row(
        "rw1",
        3,
        vec![req(5, Kind::Bulk)],
        vec![
            Op { step: Step::Drop { m: B, c: ITEMS }, t: 170 },
            touch(C, ORDERS, 0, 190),
            pull(B, C),
            retention(B, 1730),
            rewind(B, 1757),
            ask(0, A, 1800),
        ],
    );
}

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

/// The relay as first reported, minimal: Y applies a keyed `$inc` and then writes the
/// counter again; X takes the database by snapshot (the record with it, rowed at its
/// `last`); M pulls from X and is sent the later version without the record, and its
/// coverage then passes the `$inc`'s own entry, so Y never serves it the record either, Y
/// serving throughout. A retry on M applies the `$inc` a second time (n 11 -> 12). Asserts
/// guarantee 2 (a member holding any effect of the write holds its record); it passes since
/// records travel with coverage (fix (e)).
#[test]
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

/// Finding L1, minimal: a keyed `$inc` of two documents commits once, at (t,1) and
/// (t,2); another member drops the collection at (t,1), in the same millisecond. The
/// drop buries the first entry (`is_history` compares the HLC, at or below) and with it
/// the record, whose `last` is that first entry, while the second document's version,
/// at (t,2), stands in the life after the drop (ADR-236). The member then holds that
/// document's effect with no live record, and a retry increments it a second time.
/// Asserts the design's intent ("judged by `last`" exists so that a drop between two
/// writes of one request never lets a retry write them again); it passes since a
/// record's `last` is the last entry its commit wrote.
#[test]
fn finding_l1_a_drop_in_the_millisecond_of_a_commit_buries_its_record() {
    let dir = tempfile::tempdir().unwrap();
    let a = Engine::open(&dir.path().join("a.redb")).unwrap();
    let b = Engine::open(&dir.path().join("b.redb")).unwrap();
    let ca = a.create_collection("shop", "notes").unwrap();
    a.insert_many(&ca, vec![doc! { "_id": 0i64, "n": 0i64 }, doc! { "_id": 1i64, "n": 0i64 }])
        .unwrap();
    pull_all(&b, &a);
    let t = (crate::physical_now_ms() / 1000 + 3_600) * 1000;
    let k = KeyedWrite::new(ScopeHash([7; 32]), Fingerprint([7; 32]));
    a.advance_clock_past(&Stamp::new(Hlc::new(t, 0), a.node_id()));
    let first = a.modify_where_keyed(&ca, &Candidates::Scan, &PlainInc, None, &k).unwrap();
    assert!(matches!(first, Keyed::Fresh(ref o) if o.commits == 1 && o.modified == 2));
    b.advance_clock_past(&Stamp::new(Hlc::new(t, 0), b.node_id()));
    b.drop_collection("shop", "notes").unwrap();
    assert_eq!(b.collection_dropped_at(ca.id).unwrap().map(|s| s.hlc), Some(Hlc::new(t, 1)));
    pull_all(&a, &b);
    a.finish_purges_now().unwrap();
    let cn = a.get_collection("shop", "notes").expect("the life after the drop stands");
    let n1 = |e: &Engine| e.get(&cn, &DocId::Int64(1)).unwrap().unwrap().get_i64("n").unwrap();
    assert_eq!(n1(&a), 1, "premise: the second document's version stands, with the `$inc`");
    let held = a.idempotency_record(cn.id, &k.scope).unwrap();
    let retry = a.modify_where_keyed(&cn, &Candidates::Scan, &PlainInc, None, &k).unwrap();
    assert_eq!(
        n1(&a),
        1,
        "the `$inc` was applied twice to a document it wrote in the life that stands: record \
         {held:?}, retry {retry:?}"
    );
}
