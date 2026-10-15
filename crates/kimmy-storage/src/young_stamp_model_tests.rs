//! The young-stamp model test (repair design v3.5, section 13.4): the one rule that lets
//! the divergence repair **create** a row.
//!
//! **The claim under test.** A row the `Converge` repair creates on a requester R from a
//! sender S, and a plain index the `Definitions` repair builds there, is never one that R or
//! S deleted or dropped: its stamp is above every tombstone either of them applied and then
//! collected, or forgot (design 13.3, pairwise).
//!
//! **Members.** Three engines in one process, A, B and C, opened from template stores with
//! fixed identities (`0x11..`, `0x22..`, `0x33..`; a wiped member comes back under another
//! fixed identity). Every repair is between a pair; the third member's deletes are measured
//! against what the pair created (the ADR-085 window of 13.3), never asserted away.
//!
//! **Clocks.** Every step carries a time `t` (virtual milliseconds past a base taken at
//! the start of the run), derived from the step sequence alone. Before a member's local
//! step its clock is moved to `base + k * (t + skew)` (`advance_clock_past`), so the stamps
//! a schedule mints are a function of the schedule. The base runs *ahead* of the wall clock
//! between starts, and every start waits for the wall clock to reach its `t`: the reset at
//! open reads the wall clock (`reset_at_open`), and a start that ran behind the stamps it
//! must cover would be a clock stepped back, the edge design 13.1 states and does not
//! close. A run whose wall clock overtakes a local step's time would mint a stamp from the
//! wall clock; it is detected and the whole schedule is run again with every time scaled
//! by two (`k`), which changes nothing in what the schedule means. The far skew (100 s)
//! is a member whose clock, and through it the cluster's HLC, runs ahead of every start in
//! the run.
//!
//! **Steps.** Client inserts/updates (an upsert) and deletes; collection create and drop;
//! database drop; plain index create and drop; a filler of 1001 documents (so a late drop's
//! sweep takes two chunks); a retention pass with the member's own retention (unequal per
//! member) and clock; a restart, optionally with a changed retention; a run of the real
//! previous release (`0.49.0`) over the store, from the step until the member's next start
//! (its passes collect without raising the key, its pages carry no horizon, it adopts
//! nothing and repairs nothing, and it leaves its sidecar); a start killed between the
//! pre-open sidecar write and the reset commit (the pre-open write landing and nothing
//! after it, or an fsync failure injected into the open), then a start; a backup and a
//! restore of an earlier one; a wipe with a whole-database resync; a skew change; pulls by
//! entries as the transport makes them, and a pull that loses an entry (witnessed, not
//! applied); whole-database pages (coverage and horizon adoption); the count repair's
//! scoped page; and the two repairs, invoked as the transport invokes them: the
//! `Converge` pull page by page, the `Definitions` pull's first page, a `Converge` with a
//! retention pass of the requester's committed at `Race::ConvergePage` (between the plan and
//! the page's write transaction), and either repair run between two chunks of a late
//! drop's sweep.
//!
//! **The oracle.** A ghost history per member: every document tombstone, collection,
//! database and index tombstone it has ever held, read after every step and kept after
//! it is collected, restored over or wiped (each event tagged with the identity it was seen
//! under). A collection or database drop is a delete of every document and every index of
//! the life it ended, at the drop's stamp. Only rows a repair **created** are judged (and
//! indexes a definition repair **built**): a created row of `(collection, id)` at stamp `s`
//! is a resurrection iff the union of the requester's and the sender's histories holds a
//! delete of that id, a drop of its collection or of its database, stamped after `s`. That
//! is the last-writer-wins statement: a client's re-insert after a delete is stamped above
//! it and is not flagged. Every client write is recorded too, and a created row must be one
//! of them.
//!
//! **What is excluded, and how.** The resurrection past retention through ordinary
//! replication and the count repair's pages (ADR-085, by design) is not judged: the oracle
//! looks only at rows the divergence repair created, never at rows entries, a whole-database
//! page or a scoped `All` page brought back. They are counted (`adr085`), and a repaired row
//! applied over a held one whose stamp a history says is dead is counted
//! (`spread_over_held`), not judged: the design's rows one to three are ordinary LWW.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

use bson::doc;
use kimmy_core::{CollectionId, DocId, Hlc, IndexField, Stamp};
use redb::{ReadableDatabase, ReadableTable};

use crate::format::BuildVersions;
use crate::{
    Engine, MarkedRange, PeerWindow, RetentionPolicy, ServeVerdict, SnapshotProgress, StorageError,
    SyncOutcome, WalkScope, tables,
};

const DB: &str = "shop";
const NAMES: [&str; 2] = ["orders", "items"];
const LETTERS: [char; 3] = ['A', 'B', 'C'];
/// The release before the one that keeps the horizon, by its real version string.
const PREVIOUS_RELEASE: &str = "0.49.0";
const BATCH: usize = 1024;
/// The filler's ids: more records than one chunk of a late drop's sweep examines.
const FILLER_FROM: i64 = 1_000;
const FILLER: i64 = 1_001;
const ORDERS: u8 = 0;

// -- Time -----------------------------------------------------------------------------------

/// The first step's time.
const LEAD: u32 = 30;
/// Between two steps.
const STEP: u32 = 3;
/// After a start, before the next step: the wall clock must stay behind it.
const OPEN_POST: u32 = 40;
/// Retentions, virtual ms. `NEVER` collects nothing.
const NEVER: u32 = u32::MAX;
const RETENTIONS: [u32; 6] = [2, 8, 24, 60, 200, NEVER];
/// Skews, virtual ms. `FAR` is a clock ahead of every start in the run.
const FAR: i32 = 100_000;
const SKEWS: [i32; 5] = [0, -3, 4, 12, FAR];
/// A restart's retention slot meaning "unchanged".
const SAME: u8 = u8::MAX;

// -- Schedules ------------------------------------------------------------------------------

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
    Filler {
        m: u8,
        c: u8,
    },
    Gc {
        m: u8,
    },
    /// A restart, with the retention slot `r` from then on (`SAME`: unchanged).
    Restart {
        m: u8,
        r: u8,
    },
    /// A start killed between the pre-open sidecar write and the reset commit, then a
    /// start: `how` 0 is the pre-open write landing and nothing after it, 1 an fsync
    /// failure injected into the open.
    Kill {
        m: u8,
        how: u8,
    },
    /// The 0.49.0 release runs the store from here until the member's next start.
    Foreign {
        m: u8,
    },
    Backup {
        m: u8,
    },
    Restore {
        m: u8,
        b: u8,
    },
    /// Wipe the data directory, start as a new identity and resync by whole-database pages
    /// from `from`.
    Wipe {
        m: u8,
        from: u8,
    },
    Skew {
        m: u8,
        s: u8,
    },
    Pull {
        to: u8,
        from: u8,
    },
    /// A pull whose first window loses what it carries for document `d` (or, for `d == 9`,
    /// every index creation): witnessed, not applied.
    Lose {
        to: u8,
        from: u8,
        d: u8,
    },
    Whole {
        to: u8,
        from: u8,
    },
    Scoped {
        to: u8,
        from: u8,
        c: u8,
    },
    Converge {
        to: u8,
        from: u8,
        c: u8,
    },
    /// A `Converge` with a retention pass of the requester's committed at
    /// `Race::ConvergePage`: after the plan and the page, before its write transaction.
    ConvergeRacingGc {
        to: u8,
        from: u8,
        c: u8,
    },
    Definitions {
        to: u8,
        from: u8,
        c: u8,
    },
    /// A pull to `to` from `from`; after the first chunk of any late-drop sweep it runs,
    /// the repair (`defs`: definitions, else documents) of every collection on `to` from
    /// `rp`, with the writer released between the chunks.
    PullSweepRace {
        to: u8,
        from: u8,
        rp: u8,
        defs: bool,
    },
}

impl Step {
    fn opens(&self) -> bool {
        matches!(
            self,
            Step::Restart { .. } | Step::Kill { .. } | Step::Restore { .. } | Step::Wipe { .. }
        )
    }
}

fn l(m: u8) -> char {
    LETTERS[m as usize]
}

fn n(c: u8) -> &'static str {
    NAMES[c as usize]
}

fn index_slot(ix: u8) -> (&'static str, &'static str) {
    if ix == 0 { ("k_1", "k") } else { ("j_1", "j") }
}

impl std::fmt::Display for Step {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Step::Create { m, c } => write!(f, "{}.create({})", l(m), n(c)),
            Step::Write { m, c, d } => write!(f, "{}.write({}/{d})", l(m), n(c)),
            Step::Delete { m, c, d } => write!(f, "{}.delete({}/{d})", l(m), n(c)),
            Step::Index { m, c, ix } => write!(f, "{}.index({},{})", l(m), n(c), index_slot(ix).0),
            Step::DropIndex { m, c, ix } => {
                write!(f, "{}.dropindex({},{})", l(m), n(c), index_slot(ix).0)
            }
            Step::Drop { m, c } => write!(f, "{}.drop({})", l(m), n(c)),
            Step::DropDb { m } => write!(f, "{}.dropdb", l(m)),
            Step::Filler { m, c } => write!(f, "{}.filler({})", l(m), n(c)),
            Step::Gc { m } => write!(f, "{}.gc", l(m)),
            Step::Restart { m, r } if r == SAME => write!(f, "{}.restart", l(m)),
            Step::Restart { m, r } => write!(f, "{}.restart(retention {})", l(m), ret(r)),
            Step::Kill { m, how } => {
                write!(f, "{}.kill({})", l(m), if how == 0 { "after-sidecar" } else { "fsync" })
            }
            Step::Foreign { m } => write!(f, "{}.run-0.49.0", l(m)),
            Step::Backup { m } => write!(f, "{}.backup", l(m)),
            Step::Restore { m, b } => write!(f, "{}.restore(#{b})", l(m)),
            Step::Wipe { m, from } => write!(f, "{}.wipe(resync from {})", l(m), l(from)),
            Step::Skew { m, s } => write!(f, "{}.skew({})", l(m), SKEWS[s as usize]),
            Step::Pull { to, from } => write!(f, "{}<-{} pull", l(to), l(from)),
            Step::Lose { to, from, d: 9 } => {
                write!(f, "{}<-{} pull losing index creations", l(to), l(from))
            }
            Step::Lose { to, from, d } => write!(f, "{}<-{} pull losing {d}", l(to), l(from)),
            Step::Whole { to, from } => write!(f, "{}<-{} whole", l(to), l(from)),
            Step::Scoped { to, from, c } => write!(f, "{}<-{} scoped({})", l(to), l(from), n(c)),
            Step::Converge { to, from, c } => {
                write!(f, "{}<-{} CONVERGE({})", l(to), l(from), n(c))
            }
            Step::ConvergeRacingGc { to, from, c } => {
                write!(f, "{}<-{} CONVERGE({}) racing {}.gc", l(to), l(from), n(c), l(to))
            }
            Step::Definitions { to, from, c } => {
                write!(f, "{}<-{} DEFINITIONS({})", l(to), l(from), n(c))
            }
            Step::PullSweepRace { to, from, rp, defs } => write!(
                f,
                "{}<-{} pull, {} from {} between sweep chunks",
                l(to),
                l(from),
                if defs { "DEFINITIONS" } else { "CONVERGE" },
                l(rp)
            ),
        }
    }
}

fn ret(r: u8) -> String {
    match RETENTIONS[r as usize] {
        NEVER => "never".into(),
        v => format!("{v}"),
    }
}

/// Per-schedule settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Config {
    /// Retention slot per member.
    pub retention: [u8; 3],
    /// Skew slot per member.
    pub skew: [u8; 3],
    /// Whether the member's passes keep the oplog (else they collect it at the same cutoff).
    pub keep_oplog: [bool; 3],
    /// This build calls itself `0.49.0` (the shape of review revA M1: a development build
    /// of the next release before the version bump).
    pub same_name: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config { retention: [1, 1, 5], skew: [0; 3], keep_oplog: [true; 3], same_name: false }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Schedule {
    pub cfg: Config,
    pub steps: Vec<Step>,
}

impl Schedule {
    /// Each step's time, from the sequence alone.
    fn times(&self) -> Vec<u32> {
        let max_skew = self
            .cfg
            .skew
            .iter()
            .map(|s| SKEWS[*s as usize])
            .chain(self.steps.iter().filter_map(|s| match s {
                Step::Skew { s, .. } => Some(SKEWS[*s as usize]),
                _ => None,
            }))
            .filter(|s| *s < FAR)
            .max()
            .unwrap_or(0)
            .max(0) as u32;
        let mut t = LEAD;
        let mut out = Vec::with_capacity(self.steps.len());
        for step in &self.steps {
            if step.opens() {
                // Every stamp minted so far, a fast member's included, is below a start.
                t += max_skew + STEP + 1;
                out.push(t);
                t += OPEN_POST;
            } else {
                t += STEP;
                out.push(t);
            }
        }
        out
    }

    fn show(&self) -> String {
        let mut out = format!(
            "config: retention {:?} skew {:?} keep_oplog {:?} same_name {}\n  steps:",
            self.cfg.retention.map(ret),
            self.cfg.skew.map(|s| SKEWS[s as usize]),
            self.cfg.keep_oplog,
            self.cfg.same_name
        );
        for (i, s) in self.steps.iter().enumerate() {
            let _ = write!(out, "\n    {i:>3}: {s}");
        }
        out
    }

    fn as_rust(&self) -> String {
        let mut out = format!(
            "Schedule {{ cfg: Config {{ retention: {:?}, skew: {:?}, keep_oplog: {:?}, same_name: {} }}, steps: vec![",
            self.cfg.retention, self.cfg.skew, self.cfg.keep_oplog, self.cfg.same_name
        );
        for s in &self.steps {
            let _ = write!(out, "Step::{s:?}, ");
        }
        out.push_str("] }");
        out
    }
}

// -- Templates --------------------------------------------------------------------------------

/// An empty store with the fixed identity `[id; 16]`, made once per process and copied.
fn template(id: u8) -> PathBuf {
    static T: OnceLock<(tempfile::TempDir, Mutex<HashMap<u8, PathBuf>>)> = OnceLock::new();
    let (dir, map) = T.get_or_init(|| (tempfile::tempdir().unwrap(), Mutex::new(HashMap::new())));
    let mut map = map.lock().unwrap();
    if let Some(path) = map.get(&id) {
        return path.clone();
    }
    let path = dir.path().join(format!("t{id:02x}.redb"));
    Engine::open(&path).unwrap().close().expect("a fresh store closes cleanly");
    {
        let db = redb::Database::open(&path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut meta = txn.open_table(tables::META).unwrap();
            meta.insert(tables::META_NODE_ID, [id; 16].as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }
    map.insert(id, path.clone());
    path
}

fn identity(m: usize, generation: u8) -> u8 {
    if generation == 0 { 0x11 * (m as u8 + 1) } else { 0x40 + 0x10 * m as u8 + generation }
}

/// Every document id the model writes, by its storage key.
fn id_of_key(key: &[u8]) -> Option<i64> {
    static KEYS: OnceLock<HashMap<Vec<u8>, i64>> = OnceLock::new();
    KEYS.get_or_init(|| {
        (1..=3)
            .chain(FILLER_FROM..FILLER_FROM + FILLER)
            .map(|d| (crate::docs::doc_key(&DocId::Int64(d)).unwrap(), d))
            .collect()
    })
    .get(key)
    .copied()
}

fn cid(c: u8) -> CollectionId {
    CollectionId::derive(DB, NAMES[c as usize])
}

fn coll_of(id: u64) -> Option<u8> {
    (0..NAMES.len() as u8).find(|c| cid(*c).0 == id)
}

// -- The ghost ----------------------------------------------------------------------------

/// A delete or a drop a member held.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Event {
    Del { c: u8, d: i64, t: Stamp },
    Drop { c: u8, t: Stamp },
    DbDrop { t: Stamp },
    IxDrop { c: u8, ix: u32, t: Stamp },
}

impl Event {
    /// Whether this event deletes the document `(c, d)` written at `s`.
    fn kills_doc(&self, c: u8, d: i64, s: Stamp) -> bool {
        match *self {
            Event::Del { c: ec, d: ed, t } => ec == c && ed == d && t > s,
            Event::Drop { c: ec, t } => ec == c && t > s,
            Event::DbDrop { t } => t > s,
            Event::IxDrop { .. } => false,
        }
    }

    /// Whether this event drops the index `ix` of `c` created at `created`.
    fn kills_index(&self, c: u8, ix: u32, created: Stamp) -> bool {
        match *self {
            Event::IxDrop { c: ec, ix: eix, t } => ec == c && eix == ix && t > created,
            Event::Drop { c: ec, t } => ec == c && t > created,
            Event::DbDrop { t } => t > created,
            Event::Del { .. } => false,
        }
    }

    fn stamp(&self) -> Stamp {
        match *self {
            Event::Del { t, .. }
            | Event::Drop { t, .. }
            | Event::DbDrop { t }
            | Event::IxDrop { t, .. } => t,
        }
    }
}

// -- Findings and statistics ----------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Cat {
    /// A step failed with an error the model does not expect.
    Error,
    /// A created document whose stamp a history of the pair's current identities says is dead.
    CreatedResurrection,
    /// A built index the pair's histories say was dropped after it was made.
    BuiltResurrection,
    /// A created row that is no client's write.
    CreatedUnknownWrite,
    /// The repair's own count of created rows differs from what it wrote.
    CreatedCount,
    /// Design 13.4, also asserted: a row created into a life that owes a sweep. Not the
    /// safety oracle (a row above the drop is live by LWW); the rule as the design states it.
    CreatedWhileOwed,
    /// Design 13.4, also asserted: a row created or an index built from a page without the
    /// sender's horizon.
    CreatedWithoutHorizon,
}

#[derive(Clone, Debug)]
pub(crate) struct Finding {
    pub cat: Cat,
    /// What a run deduplicates and a minimisation keeps: the category, and for a
    /// resurrection which side's history holds the delete and what step made it go.
    pub key: String,
    pub detail: String,
}

impl Finding {
    fn plain(cat: Cat, detail: String) -> Self {
        Finding { cat, key: format!("{cat:?}"), detail }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Stats {
    pub schedules: usize,
    pub steps: usize,
    pub converges: usize,
    pub definitions: usize,
    pub created: usize,
    pub built: usize,
    pub left: usize,
    pub ambiguous: usize,
    pub third_member: usize,
    pub third_member_lost: usize,
    pub third_window_max_ms: u64,
    /// Created or built where only a wiped identity's history says it is dead: the wipe
    /// discarded that identity's unpropagated deletes for every route, and the new identity
    /// deleted nothing. Counted, not judged.
    pub pre_wipe: usize,
    pub adr085: usize,
    pub spread_over_held: usize,
    pub racing_gc_fired: usize,
    pub racing_gc_collected: usize,
    pub sweep_race_fired: usize,
    pub sweep_owed_at_repair: usize,
    pub foreign_runs: usize,
    pub restores: usize,
    pub wipes: usize,
    pub kills: usize,
    pub kills_fsync_failed: usize,
    pub gc_removed: usize,
    pub retries: usize,
}

impl Stats {
    fn add(&mut self, o: &Stats) {
        macro_rules! sum { ($($f:ident),*) => { $( self.$f += o.$f; )* } }
        sum!(
            schedules,
            steps,
            converges,
            definitions,
            created,
            built,
            left,
            ambiguous,
            third_member,
            third_member_lost,
            pre_wipe,
            adr085,
            spread_over_held,
            racing_gc_fired,
            racing_gc_collected,
            sweep_race_fired,
            sweep_owed_at_repair,
            foreign_runs,
            restores,
            wipes,
            kills,
            kills_fsync_failed,
            gc_removed,
            retries
        );
        self.third_window_max_ms = self.third_window_max_ms.max(o.third_window_max_ms);
    }
}

// -- What a repair did ----------------------------------------------------------------------

type Rows = BTreeMap<i64, (Stamp, bool)>;

/// The raw rows of a collection, tombstones included: `id -> (stamp, live)`.
fn rows(e: &Engine, id: CollectionId) -> Rows {
    let txn = e.db().begin_read().unwrap();
    let docs = txn.open_table(tables::DOCS).unwrap();
    let empty: &[u8] = &[];
    let mut out = Rows::new();
    for row in docs
        .range::<(u64, &[u8])>((
            std::ops::Bound::Included((id.0, empty)),
            std::ops::Bound::Excluded((id.0 + 1, empty)),
        ))
        .unwrap()
    {
        let (key, value) = row.unwrap();
        let (_, doc_key) = key.value();
        let Some(d) = id_of_key(doc_key) else { continue };
        let record = crate::codec::decode_doc_record(value.value()).unwrap();
        out.insert(d, (record.stamp, !record.deleted));
    }
    out
}

fn index_names(e: &Engine, id: CollectionId) -> BTreeMap<String, Option<Stamp>> {
    match e.collection_by_id(id) {
        Ok(Some(meta)) => meta.indexes.iter().map(|i| (i.name.clone(), i.created)).collect(),
        _ => BTreeMap::new(),
    }
}

#[derive(Clone, Debug, Default)]
struct RepairOut {
    c: u8,
    defs: bool,
    /// `(id, stamp)` of every row the repair created.
    created: Vec<(i64, Stamp)>,
    /// Rows applied over a row held, with their new stamps.
    applied: Vec<(i64, Stamp)>,
    /// `(name, created)` of every index it built.
    built: Vec<(String, Option<Stamp>)>,
    counted_created: usize,
    left: usize,
    ambiguous: usize,
    own_horizon: Option<Hlc>,
    sender_horizon: Option<Hlc>,
    owed: bool,
    note: String,
    /// What the requester and the sender held just before a repair run inside a step
    /// (between two chunks of a sweep), which the step-end scan has not read yet.
    held_before: Option<(Vec<Event>, Vec<Event>)>,
}

/// The sender's page as the requester receives it: from a 0.49 sender, no horizon.
fn page_from(
    from: &Engine,
    from_old: bool,
    after: Option<crate::SnapshotCursor>,
    scope: Option<CollectionId>,
) -> crate::Result<crate::SnapshotPage> {
    let mut page = from.snapshot_page(after, scope)?;
    if from_old {
        page.tombstone_horizon = None;
    }
    Ok(page)
}

/// Whether the collection owes a late-drop sweep against its own or its database's
/// tombstone, as the repair reads it.
fn owes(e: &Engine, id: CollectionId) -> bool {
    let Ok(Some(meta)) = e.collection_by_id(id) else { return false };
    [id, kimmy_core::database_drop_id(DB)].into_iter().any(|t| {
        e.collection_dropped_at(t)
            .ok()
            .flatten()
            .is_some_and(|stamp| crate::late_drop::owes_a_sweep(&meta, stamp.hlc))
    })
}

/// A document repair of collection `c` on `to` from `from`, page by page, as the transport
/// runs one (`SnapshotProgress::of_collection_converging`, the sender's scoped pages).
/// `before` is what `to` held when the page's transaction opened, when a racing step
/// changed it after the call began.
fn converge_once(
    to: &Engine,
    from: &Engine,
    from_old: bool,
    c: u8,
    before: Option<&RefCell<Option<Rows>>>,
) -> crate::Result<RepairOut> {
    let id = cid(c);
    let held = rows(to, id);
    let mut out = RepairOut {
        c,
        own_horizon: to.tombstone_horizon()?,
        owed: owes(to, id),
        ..Default::default()
    };
    let mut progress =
        SnapshotProgress::of_collection_converging(id, kimmy_core::VersionVector::new());
    let mut pages = 0;
    while !progress.is_complete() {
        let page = page_from(from, from_old, progress.after().cloned(), progress.scope())?;
        out.sender_horizon = page.tombstone_horizon;
        let applied = to.apply_snapshot_page(from.node_id(), &mut progress, &page)?;
        out.counted_created += applied.created;
        out.left += applied.left;
        pages += 1;
        if applied.sender_dropped || applied.history || pages > 16 {
            let _ = write!(
                out.note,
                "ended: sender_dropped {} history {} pages {pages}",
                applied.sender_dropped, applied.history
            );
            break;
        }
    }
    let held = before.and_then(|b| b.borrow_mut().take()).unwrap_or(held);
    for (d, (stamp, live)) in rows(to, id) {
        match held.get(&d) {
            None if live => out.created.push((d, stamp)),
            Some((was, _)) if *was != stamp => out.applied.push((d, stamp)),
            _ => {}
        }
    }
    Ok(out)
}

/// A definition repair of collection `c` on `to` from `from`: the first scoped page,
/// applied under `Definitions`, as the transport runs one.
fn definitions_once(to: &Engine, from: &Engine, from_old: bool, c: u8) -> crate::Result<RepairOut> {
    let id = cid(c);
    let before = index_names(to, id);
    let mut out = RepairOut {
        c,
        defs: true,
        own_horizon: to.tombstone_horizon()?,
        owed: owes(to, id),
        ..Default::default()
    };
    let mut progress = SnapshotProgress::of_collection_definitions(id, 250_000);
    let page = page_from(from, from_old, None, Some(id))?;
    out.sender_horizon = page.tombstone_horizon;
    let applied = to.apply_snapshot_page(from.node_id(), &mut progress, &page)?;
    out.ambiguous = applied.definitions.ambiguous;
    out.counted_created = applied.definitions.built;
    for (name, created) in index_names(to, id) {
        if !before.contains_key(&name) {
            out.built.push((name, created));
        }
    }
    Ok(out)
}

/// The 0.49 line's retention pass over tombstones: the same rule, and no horizon.
fn old_release_collects(e: &Engine, cutoff: Hlc) -> crate::Result<usize> {
    let (docs, colls, indexes) = {
        let txn = e.db().begin_read()?;
        let mut docs = Vec::new();
        for row in txn.open_table(tables::DOCS)?.iter()? {
            let (key, value) = row?;
            let (c, k) = key.value();
            let record = crate::codec::decode_doc_record(value.value())?;
            if record.deleted && record.stamp.hlc < cutoff {
                docs.push((c, k.to_vec()));
            }
        }
        let mut colls = Vec::new();
        for row in txn.open_table(tables::COLLECTIONS_DROPPED)?.iter()? {
            let (key, value) = row?;
            let stamp = crate::codec::decode_oplog_key(value.value())?;
            if stamp.hlc < cutoff {
                colls.push(key.value());
            }
        }
        let mut indexes = Vec::new();
        for row in txn.open_table(tables::INDEXES_DROPPED)?.iter()? {
            let (key, value) = row?;
            let stamp = crate::codec::decode_oplog_key(value.value())?;
            if stamp.hlc < cutoff {
                indexes.push(key.value());
            }
        }
        (docs, colls, indexes)
    };
    // A collection tombstone with rows under it is kept (ADR-189, which the 0.49 line has).
    let colls: Vec<u64> = colls
        .into_iter()
        .filter(|id| matches!(e.collection_range_is_empty(CollectionId(*id)), Ok(true)))
        .collect();
    let removed = docs.len() + colls.len() + indexes.len();
    if removed == 0 {
        return Ok(0);
    }
    let txn = e.begin_write(crate::WriterHolder::Retention)?;
    {
        let mut table = txn.open_table(tables::DOCS)?;
        for (c, k) in &docs {
            crate::live_count::remove_record(&txn, &mut table, *c, k)?;
        }
        let mut dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
        for id in &colls {
            dropped.remove(*id)?;
        }
        let mut dropped = txn.open_table(tables::INDEXES_DROPPED)?;
        for key in &indexes {
            dropped.remove(*key)?;
        }
    }
    txn.commit()?;
    Ok(removed)
}

/// Every delete and drop `e` holds now: document tombstones, collection and database
/// tombstones, index tombstones; and its live rows.
fn held_events(e: &Engine) -> (Vec<Event>, Vec<(u8, i64, Stamp)>) {
    let txn = e.db().begin_read().unwrap();
    let mut found = Vec::new();
    let mut live = Vec::new();
    for row in txn.open_table(tables::DOCS).unwrap().iter().unwrap() {
        let (key, value) = row.unwrap();
        let (c, k) = key.value();
        let (Some(c), Some(d)) = (coll_of(c), id_of_key(k)) else { continue };
        let record = crate::codec::decode_doc_record(value.value()).unwrap();
        if record.deleted {
            found.push(Event::Del { c, d, t: record.stamp });
        } else {
            live.push((c, d, record.stamp));
        }
    }
    for row in txn.open_table(tables::COLLECTIONS_DROPPED).unwrap().iter().unwrap() {
        let (key, value) = row.unwrap();
        let t = crate::codec::decode_oplog_key(value.value()).unwrap();
        if key.value() == kimmy_core::database_drop_id(DB).0 {
            found.push(Event::DbDrop { t });
        } else if let Some(c) = coll_of(key.value()) {
            found.push(Event::Drop { c, t });
        }
    }
    for row in txn.open_table(tables::INDEXES_DROPPED).unwrap().iter().unwrap() {
        let (key, value) = row.unwrap();
        let t = crate::codec::decode_oplog_key(value.value()).unwrap();
        let (c, ix) = key.value();
        if let Some(c) = coll_of(c) {
            found.push(Event::IxDrop { c, ix, t });
        }
    }
    (found, live)
}

// -- The cluster --------------------------------------------------------------------------

struct Member {
    _dir: tempfile::TempDir,
    path: PathBuf,
    engine: Option<Rc<Engine>>,
    generation: u8,
    retention: u32,
    skew: i32,
    keep_oplog: bool,
    /// The 0.49.0 release is running the store.
    old: bool,
    backups: Vec<Vec<u8>>,
    /// Every delete and drop held, with the identity generation it was first seen under.
    ghost: BTreeMap<Event, u8>,
    /// When a held event stopped being held, the step that did it, and whether that step
    /// also replaced the life of the event's collection.
    gone: BTreeMap<Event, (usize, Step, bool)>,
    /// Each collection's `created` at the last scan.
    created: [Option<Hlc>; 2],
}

type Res<T> = std::result::Result<T, String>;

fn err(what: &str, e: impl std::fmt::Debug) -> String {
    format!("{what}: {e:?}")
}

struct Cluster {
    members: Vec<Member>,
    base: u64,
    k: u64,
    t: u32,
    step: usize,
    current: Option<Step>,
    same_name: bool,
    notes: Vec<String>,
    writes: BTreeSet<(u8, i64, Stamp)>,
    findings: Vec<Finding>,
    stats: Stats,
    adr085: BTreeSet<(usize, u8, i64, Stamp)>,
    overrun: bool,
}

impl Cluster {
    fn new(cfg: &Config, k: u64) -> Self {
        let members = (0..3)
            .map(|m| {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("kimmy.redb");
                Member {
                    _dir: dir,
                    path,
                    engine: None,
                    generation: 0,
                    retention: RETENTIONS[cfg.retention[m] as usize],
                    skew: SKEWS[cfg.skew[m] as usize],
                    keep_oplog: cfg.keep_oplog[m],
                    old: false,
                    backups: Vec::new(),
                    ghost: BTreeMap::new(),
                    gone: BTreeMap::new(),
                    created: [None; 2],
                }
            })
            .collect();
        let mut cluster = Cluster {
            members,
            base: crate::physical_now_ms(),
            k,
            t: 0,
            step: 0,
            current: None,
            same_name: cfg.same_name,
            notes: Vec::new(),
            writes: BTreeSet::new(),
            findings: Vec::new(),
            stats: Stats::default(),
            adr085: BTreeSet::new(),
            overrun: false,
        };
        for m in 0..3 {
            cluster.place_template(m);
            cluster.start(m, None).expect("a template opens");
        }
        // The schedule's times count from here, after the first starts.
        cluster.base = crate::physical_now_ms() + 1;
        cluster
    }

    fn build(&self) -> BuildVersions {
        let mut build = BuildVersions::ours();
        if self.same_name {
            build.written_by = PREVIOUS_RELEASE.into();
        }
        build
    }

    fn place_template(&mut self, m: usize) {
        let member = &self.members[m];
        let template = template(identity(m, member.generation));
        std::fs::copy(&template, &member.path).unwrap();
        std::fs::copy(
            crate::format::sidecar_path(&template),
            crate::format::sidecar_path(&member.path),
        )
        .unwrap();
    }

    fn e(&self, m: usize) -> Rc<Engine> {
        Rc::clone(self.members[m].engine.as_ref().expect("the member is open"))
    }

    fn at(&self, t: u32, skew: i32) -> u64 {
        (self.base as i64 + self.k as i64 * (i64::from(t) + i64::from(skew))) as u64
    }

    fn rel(&self, h: Hlc) -> String {
        if h == Hlc::ZERO {
            return "0".into();
        }
        let rel = h.wall_ms as i64 - self.base as i64;
        let rel = if rel.rem_euclid(self.k as i64) == 0 {
            format!("{}", rel / self.k as i64)
        } else {
            format!("{rel}/{}", self.k)
        };
        if h.counter == 0 { format!("t{rel}") } else { format!("t{rel}.{}", h.counter) }
    }

    fn st(&self, s: Stamp) -> String {
        let who = (0..3)
            .find(|m| self.members[*m].engine.as_ref().is_some_and(|e| e.node_id() == s.node))
            .map_or('?', |m| LETTERS[m]);
        format!("{}{}", self.rel(s.hlc), who)
    }

    fn note(&mut self, s: String) {
        self.notes.push(format!("  [{}] {s}", self.step));
    }

    /// Wait for the wall clock to reach this step's time: a start reads it.
    fn wall_reaches(&self, t: u32) {
        let target = self.at(t, 0);
        loop {
            let now = crate::physical_now_ms();
            if now > target {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis((target - now).min(50)));
        }
    }

    /// Move member `m`'s clock to this step's time before a local step.
    fn clock(&mut self, m: usize) {
        let e = self.e(m);
        let target = self.at(self.t, self.members[m].skew);
        let now = crate::physical_now_ms();
        if target.max(e.clock_last().wall_ms) <= now + 2 {
            self.overrun = true;
        }
        e.advance_clock_past(&Stamp::new(Hlc::new(target, 0), e.node_id()));
    }

    fn start(&mut self, m: usize, kill: Option<u8>) -> Res<()> {
        let path = self.members[m].path.clone();
        let build = self.build();
        if let Some(how) = kill {
            self.stats.kills += 1;
            if how == 0 {
                // The pre-open sidecar write lands and the process dies: nothing after it.
                let cleared = crate::format::check_before_open_with(&path, &build)
                    .map_err(|e| err("check_before_open", e))?;
                if let Some(sidecar) = cleared.sidecar_before_open() {
                    crate::format::write_sidecar(&path, &sidecar).map_err(|e| err("sidecar", e))?;
                }
                drop(cleared);
            } else {
                crate::hold_meter::test_hooks::ARM_AT_OPEN.with(|a| a.set(Some("sync_data")));
                let opened = Engine::open_as(&path, None, &build);
                crate::hold_meter::test_hooks::ARM_AT_OPEN.with(|a| a.set(None));
                match opened {
                    Err(_) => self.stats.kills_fsync_failed += 1,
                    Ok(engine) => {
                        self.note(format!(
                            "{}: the injected fsync failure did not stop the open",
                            LETTERS[m]
                        ));
                        let _ = engine.close();
                    }
                }
            }
        }
        let engine = Engine::open_as(&path, None, &build).map_err(|e| err("open", e))?;
        engine.reconcile_database_tombstones().map_err(|e| err("reconcile_database", e))?;
        engine.reconcile_late_drops().map_err(|e| err("reconcile_late_drops", e))?;
        engine.finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
        self.members[m].engine = Some(Rc::new(engine));
        Ok(())
    }

    fn stop(&mut self, m: usize) {
        let engine = self.members[m].engine.take().expect("open");
        let engine = Rc::try_unwrap(engine).unwrap_or_else(|_| panic!("an engine is still shared"));
        let _ = engine.close();
        // The 0.49.0 release leaves its own sidecar on the way out (its open wrote it).
        if std::mem::take(&mut self.members[m].old) {
            let path = &self.members[m].path;
            let mut sidecar = crate::format::read_sidecar(path).expect("a sidecar");
            sidecar.written_by = PREVIOUS_RELEASE.into();
            crate::format::write_sidecar(path, &sidecar).unwrap();
        }
    }

    fn remove_store(&mut self, m: usize) {
        let path = self.members[m].path.clone();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(crate::format::sidecar_path(&path));
    }

    fn horizons(&self) -> String {
        (0..3)
            .filter(|m| self.members[*m].engine.is_some())
            .map(|m| {
                let h = self.e(m).tombstone_horizon().ok().flatten();
                format!("{}:{}", LETTERS[m], h.map_or("-".into(), |h| self.rel(h)))
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    // -- The steps -----------------------------------------------------------------------

    fn run(&mut self, schedule: &Schedule) -> Res<()> {
        let times = schedule.times();
        for (i, step) in schedule.steps.iter().enumerate() {
            self.step = i;
            self.t = times[i];
            self.current = Some(*step);
            if step.opens() {
                self.wall_reaches(self.t);
            }
            self.apply(*step).map_err(|e| format!("step {i} ({step}): {e}"))?;
            for m in 0..3 {
                self.e(m).finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
            }
            for m in 0..3 {
                self.scan(m);
            }
            if std::env::var("YSM_TRACE").is_ok() {
                for m in 0..3 {
                    let s = self.state(m);
                    self.note(s);
                }
            }
            self.stats.steps += 1;
            if self.overrun {
                return Ok(());
            }
        }
        Ok(())
    }

    fn coll(&self, m: usize, c: u8) -> Option<crate::CollectionMeta> {
        self.e(m).get_collection(DB, NAMES[c as usize]).ok()
    }

    fn apply(&mut self, step: Step) -> Res<()> {
        match step {
            Step::Create { m, c } => {
                let m = m as usize;
                self.clock(m);
                for attempt in 0..2 {
                    match self.e(m).create_collection(DB, NAMES[c as usize]) {
                        Ok(meta) => {
                            let s = format!(
                                "{} created {} at {}",
                                l(m as u8),
                                n(c),
                                self.rel(meta.created)
                            );
                            self.note(s);
                        }
                        Err(StorageError::Core(kimmy_core::Error::CollectionExists { .. })) => {}
                        Err(StorageError::CollectionPurging { .. }) if attempt == 0 => {
                            self.e(m).finish_purges_now().map_err(|e| err("purge", e))?;
                            continue;
                        }
                        Err(e) => return Err(err("create_collection", e)),
                    }
                    break;
                }
            }
            Step::Write { m, c, d } => {
                let m = m as usize;
                let Some(coll) = self.coll(m, c) else { return Ok(()) };
                self.clock(m);
                let d = i64::from(d);
                let body = doc! { "_id": d, "k": d, "j": d, "v": self.step as i64, "by": LETTERS[m].to_string() };
                let out = self
                    .e(m)
                    .replace(&coll, &DocId::Int64(d), body, true)
                    .map_err(|e| err("replace", e))?;
                if let Some(stamp) = out.stamp {
                    self.writes.insert((c, d, stamp));
                    let s = format!("{} wrote {}/{d} at {}", l(m as u8), n(c), self.st(stamp));
                    self.note(s);
                }
            }
            Step::Delete { m, c, d } => {
                let m = m as usize;
                let Some(coll) = self.coll(m, c) else { return Ok(()) };
                self.clock(m);
                if let Some(stamp) = self
                    .e(m)
                    .delete_if(&coll, &DocId::Int64(i64::from(d)), None)
                    .map_err(|e| err("delete", e))?
                {
                    let s = format!("{} deleted {}/{d} at {}", l(m as u8), n(c), self.st(stamp));
                    self.note(s);
                }
            }
            Step::Index { m, c, ix } => {
                let m = m as usize;
                if self.coll(m, c).is_none() {
                    return Ok(());
                }
                self.clock(m);
                let (name, field) = index_slot(ix);
                match self.e(m).create_index(
                    DB,
                    NAMES[c as usize],
                    vec![IndexField::ascending(field)],
                    false,
                    Some(name.to_string()),
                ) {
                    Ok(meta) => {
                        let s = format!(
                            "{} index {}.{name} at {}",
                            l(m as u8),
                            n(c),
                            meta.created.map_or("-".into(), |s| self.st(s))
                        );
                        self.note(s);
                    }
                    Err(e) => self.note(format!("{} index {name} refused: {e}", l(m as u8))),
                }
            }
            Step::DropIndex { m, c, ix } => {
                let m = m as usize;
                if self.coll(m, c).is_none() {
                    return Ok(());
                }
                self.clock(m);
                let name = index_slot(ix).0;
                if let Ok(true) = self.e(m).drop_index(DB, NAMES[c as usize], name) {
                    self.note(format!("{} dropped index {}.{name}", l(m as u8), n(c)));
                }
            }
            Step::Drop { m, c } => {
                let m = m as usize;
                self.clock(m);
                let held =
                    self.e(m).drop_collection(DB, NAMES[c as usize]).map_err(|e| err("drop", e))?;
                if held {
                    let at = self.e(m).collection_dropped_at(cid(c)).ok().flatten();
                    let s =
                        format!("{} dropped {} at {:?}", l(m as u8), n(c), at.map(|s| self.st(s)));
                    self.note(s);
                }
            }
            Step::DropDb { m } => {
                let m = m as usize;
                self.clock(m);
                let held = self.e(m).drop_database(DB).map_err(|e| err("drop_database", e))?;
                let at = self.e(m).database_dropped_at(DB).ok().flatten();
                let s = format!(
                    "{} dropped the database (held {held}) at {:?}",
                    l(m as u8),
                    at.map(|s| self.st(s))
                );
                self.note(s);
            }
            Step::Filler { m, c } => {
                let m = m as usize;
                let Some(coll) = self.coll(m, c) else { return Ok(()) };
                self.clock(m);
                let docs = (FILLER_FROM..FILLER_FROM + FILLER)
                    .map(|d| doc! { "_id": d, "k": d, "j": d, "v": self.step as i64 })
                    .collect();
                let out = self
                    .e(m)
                    .insert_many_stamped(&coll, docs)
                    .map_err(|e| format!("insert_many: {e:?}"))?;
                for (id, stamp) in out {
                    if let DocId::Int64(d) = id {
                        self.writes.insert((c, d, stamp));
                    }
                }
                self.note(format!("{} filled {}", l(m as u8), n(c)));
            }
            Step::Gc { m } => self.gc(m as usize)?,
            Step::Restart { m, r } => {
                let m = m as usize;
                self.stop(m);
                if r != SAME {
                    self.members[m].retention = RETENTIONS[r as usize];
                }
                self.start(m, None)?;
                let h = self.e(m).tombstone_horizon().ok().flatten();
                let s = format!(
                    "{} restarted; horizon {}",
                    l(m as u8),
                    h.map_or("-".into(), |h| self.rel(h))
                );
                self.note(s);
            }
            Step::Kill { m, how } => {
                let m = m as usize;
                self.stop(m);
                self.start(m, Some(how))?;
                let h = self.e(m).tombstone_horizon().ok().flatten();
                let s = format!(
                    "{} killed at start, started; horizon {}",
                    l(m as u8),
                    h.map_or("-".into(), |h| self.rel(h))
                );
                self.note(s);
            }
            Step::Foreign { m } => {
                let m = m as usize;
                if !self.members[m].old {
                    self.members[m].old = true;
                    self.stats.foreign_runs += 1;
                    self.note(format!("{} is run by {PREVIOUS_RELEASE}", l(m as u8)));
                }
            }
            Step::Backup { m } => {
                let m = m as usize;
                let mut out = Vec::new();
                self.e(m).backup_to(&mut out, WalkScope::Request).map_err(|e| err("backup", e))?;
                if self.members[m].backups.len() < 3 {
                    self.members[m].backups.push(out);
                    let b = self.members[m].backups.len() - 1;
                    self.note(format!("{} backup #{b}", l(m as u8)));
                }
            }
            Step::Restore { m, b } => {
                let m = m as usize;
                let Some(backup) = self.members[m].backups.get(b as usize).cloned() else {
                    return Ok(());
                };
                self.stop(m);
                self.remove_store(m);
                crate::backup::restore_with(
                    &self.members[m].path,
                    &mut backup.as_slice(),
                    &self.build(),
                )
                .map_err(|e| err("restore", e))?;
                self.start(m, None)?;
                self.stats.restores += 1;
                let h = self.e(m).tombstone_horizon().ok().flatten();
                let s = format!(
                    "{} restored #{b}; horizon {}",
                    l(m as u8),
                    h.map_or("-".into(), |h| self.rel(h))
                );
                self.note(s);
            }
            Step::Wipe { m, from } => {
                let m = m as usize;
                if self.members[m].generation >= 9 {
                    return Ok(());
                }
                self.stop(m);
                self.remove_store(m);
                self.members[m].generation += 1;
                self.members[m].old = false;
                self.place_template(m);
                self.start(m, None)?;
                self.stats.wipes += 1;
                self.transfer(m, from as usize, None)?;
                let h = self.e(m).tombstone_horizon().ok().flatten();
                let s = format!(
                    "{} wiped, resynced from {}; horizon {}",
                    l(m as u8),
                    l(from),
                    h.map_or("-".into(), |h| self.rel(h))
                );
                self.note(s);
            }
            Step::Skew { m, s } => {
                self.members[m as usize].skew = SKEWS[s as usize];
            }
            Step::Pull { to, from } => self.pull(to as usize, from as usize, None)?,
            Step::Lose { to, from, d } => self.pull(to as usize, from as usize, Some(d))?,
            Step::Whole { to, from } => self.transfer(to as usize, from as usize, None)?,
            Step::Scoped { to, from, c } => {
                self.transfer(to as usize, from as usize, Some(cid(c)))?
            }
            Step::Converge { to, from, c } => {
                let (to, from) = (to as usize, from as usize);
                if self.members[to].old {
                    return Ok(());
                }
                let out =
                    converge_once(&self.e(to), &self.e(from), self.members[from].old, c, None)
                        .map_err(|e| err("converge", e))?;
                self.judge(to, from, out);
            }
            Step::ConvergeRacingGc { to, from, c } => {
                self.converge_racing_gc(to as usize, from as usize, c)?
            }
            Step::Definitions { to, from, c } => {
                let (to, from) = (to as usize, from as usize);
                if self.members[to].old {
                    return Ok(());
                }
                let out = definitions_once(&self.e(to), &self.e(from), self.members[from].old, c)
                    .map_err(|e| err("definitions", e))?;
                self.judge(to, from, out);
            }
            Step::PullSweepRace { to, from, rp, defs } => {
                self.pull_sweep_race(to as usize, from as usize, rp as usize, defs)?
            }
        }
        Ok(())
    }

    /// The cutoff of a retention pass on `m` now.
    fn cutoff(&self, m: usize) -> Option<u64> {
        let member = &self.members[m];
        if member.retention == NEVER {
            return None;
        }
        // The 0.49 run's passes are clamped to the wall clock of the start that follows:
        // a pass ahead of it is the clock stepped back that design 13.1 states and does not
        // close.
        let skew = if member.old { member.skew.min(0) } else { member.skew };
        Some(self.at(self.t, skew).saturating_sub(self.k * u64::from(member.retention)))
    }

    fn gc(&mut self, m: usize) -> Res<()> {
        let Some(cutoff) = self.cutoff(m) else { return Ok(()) };
        let e = self.e(m);
        let oplog = if self.members[m].keep_oplog { u64::MAX } else { 0 };
        let removed = if self.members[m].old {
            e.collect_garbage_at(cutoff, RetentionPolicy::new(oplog, u64::MAX))
                .map_err(|e| err("collect_garbage", e))?;
            old_release_collects(&e, Hlc::new(cutoff, 0)).map_err(|e| err("0.49 collect", e))?
        } else {
            e.collect_garbage_at(cutoff, RetentionPolicy::new(oplog, 0))
                .map_err(|e| err("collect_garbage", e))?
                .tombstones_removed
        };
        self.stats.gc_removed += removed;
        if removed > 0 {
            let s = format!(
                "{} collected {removed} tombstones below {}{}; horizon {}",
                l(m as u8),
                self.rel(Hlc::new(cutoff, 0)),
                if self.members[m].old { " (0.49.0: no horizon)" } else { "" },
                e.tombstone_horizon().ok().flatten().map_or("-".into(), |h| self.rel(h))
            );
            self.note(s);
        }
        Ok(())
    }

    /// One pull, as the transport makes it (the convergence model test's): the witnessed
    /// vector as `held`, the marked spans, the sender's verdict, the protocol's fallbacks.
    /// `lose` drops what the first window carries for a document (or `9`: every index
    /// creation): witnessed, not applied.
    fn pull(&mut self, to: usize, from: usize, mut lose: Option<u8>) -> Res<()> {
        let mut stopped_at: Option<Stamp> = None;
        for _ in 0..64 {
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
            let Some(start) = start else { return Ok(()) };
            match src.serve_verdict(&mine).map_err(|e| err("serve_verdict", e))? {
                ServeVerdict::Serve => {}
                verdict => {
                    self.note(format!(
                        "{}<-{}: {verdict:?}, a whole-database page",
                        LETTERS[to], LETTERS[from]
                    ));
                    return self.transfer(to, from, None);
                }
            }
            let window = src
                .entries_for_peer_marked(start, BATCH, Some(&mine), &marked, WalkScope::Background)
                .map_err(|e| err("entries_for_peer", e))?;
            let mut entries = window.entries;
            if let Some(what) = lose.take() {
                let before = entries.len();
                if what == 9 {
                    entries.retain(|e| e.kind != kimmy_core::OpKind::CreateIndex);
                } else {
                    entries.retain(|e| e.doc_id != Some(DocId::Int64(i64::from(what))));
                }
                if entries.len() < before {
                    self.note(format!(
                        "{}<-{}: lost {} entries",
                        LETTERS[to],
                        LETTERS[from],
                        before - entries.len()
                    ));
                }
            }
            let mut outcome = SyncOutcome::default();
            dst.apply_peer_window_into(
                &theirs,
                Some(&mine),
                &entries,
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
            if let Some(unknown) = outcome.unknown.clone() {
                if stopped_at == Some(unknown.stamp) {
                    return Ok(());
                }
                stopped_at = Some(unknown.stamp);
                self.transfer(to, from, Some(unknown.id))?;
                continue;
            }
            if window.exhausted {
                return Ok(());
            }
        }
        Err("a pull that never ended".into())
    }

    /// Snapshot pages, the whole database (coverage and horizon adoption on completion) or
    /// one collection (the count repair's `All` page). A 0.49 member neither sends nor
    /// reads the horizon.
    fn transfer(&mut self, to: usize, from: usize, scope: Option<CollectionId>) -> Res<()> {
        let mut progress =
            scope.map_or_else(SnapshotProgress::whole_database, SnapshotProgress::of_collection);
        let strip = self.members[from].old || self.members[to].old;
        let (src, dst) = (self.e(from), self.e(to));
        let mut waits = 0;
        let mut pages = 0;
        while !progress.is_complete() {
            let mut page = src
                .snapshot_page(progress.after().cloned(), progress.scope())
                .map_err(|e| err("snapshot_page", e))?;
            if strip {
                page.tombstone_horizon = None;
            }
            let applied = dst
                .apply_snapshot_page(src.node_id(), &mut progress, &page)
                .map_err(|e| err("apply_snapshot_page", e))?;
            if applied.purge_pending.is_some() {
                waits += 1;
                if waits > 4 {
                    return Err("a page that waits for a purge that never finishes".into());
                }
                dst.finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
            }
            pages += 1;
            if pages > 64 {
                return Err("a snapshot that never completed".into());
            }
        }
        Ok(())
    }

    fn converge_racing_gc(&mut self, to: usize, from: usize, c: u8) -> Res<()> {
        if self.members[to].old {
            return Ok(());
        }
        let cutoff = self.cutoff(to);
        let oplog = if self.members[to].keep_oplog { u64::MAX } else { 0 };
        let before: Rc<RefCell<Option<Rows>>> = Rc::new(RefCell::new(None));
        let fired = Rc::new(Cell::new(false));
        let cancelled = Rc::new(Cell::new(false));
        let removed = Rc::new(Cell::new(0usize));
        {
            let weak: Weak<Engine> = Rc::downgrade(self.members[to].engine.as_ref().unwrap());
            let (before, fired, cancelled, removed) =
                (Rc::clone(&before), Rc::clone(&fired), Rc::clone(&cancelled), Rc::clone(&removed));
            crate::sync::race_hooks::at(crate::sync::race_hooks::Race::ConvergePage, move || {
                if cancelled.get() {
                    return;
                }
                let Some(e) = weak.upgrade() else { return };
                fired.set(true);
                if let Some(cutoff) = cutoff {
                    let out = e.collect_garbage_at(cutoff, RetentionPolicy::new(oplog, 0)).unwrap();
                    removed.set(out.tombstones_removed);
                }
                *before.borrow_mut() = Some(rows(&e, cid(c)));
            });
        }
        let out =
            converge_once(&self.e(to), &self.e(from), self.members[from].old, c, Some(&*before));
        cancelled.set(true);
        // An unreached hook is consumed here, doing nothing.
        crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::ConvergePage);
        let out = out.map_err(|e| err("converge", e))?;
        if fired.get() {
            self.stats.racing_gc_fired += 1;
            self.stats.gc_removed += removed.get();
            if removed.get() > 0 {
                self.stats.racing_gc_collected += 1;
                self.note(format!(
                    "{}: a racing pass collected {} at the page",
                    LETTERS[to],
                    removed.get()
                ));
            }
        }
        self.judge(to, from, out);
        Ok(())
    }

    fn pull_sweep_race(&mut self, to: usize, from: usize, rp: usize, defs: bool) -> Res<()> {
        let results: Rc<RefCell<Vec<crate::Result<RepairOut>>>> = Rc::new(RefCell::new(Vec::new()));
        let cancelled = Rc::new(Cell::new(false));
        if !self.members[to].old && to != rp {
            let to_weak = Rc::downgrade(self.members[to].engine.as_ref().unwrap());
            let rp_weak = Rc::downgrade(self.members[rp].engine.as_ref().unwrap());
            let rp_old = self.members[rp].old;
            let (results, cancelled) = (Rc::clone(&results), Rc::clone(&cancelled));
            crate::late_drop::hooks::run_after_the_next_chunk(move || {
                if cancelled.get() {
                    return;
                }
                let (Some(to), Some(rp)) = (to_weak.upgrade(), rp_weak.upgrade()) else { return };
                let held = (held_events(&to).0, held_events(&rp).0);
                for c in 0..NAMES.len() as u8 {
                    let out = if defs {
                        definitions_once(&to, &rp, rp_old, c)
                    } else {
                        converge_once(&to, &rp, rp_old, c, None)
                    };
                    results.borrow_mut().push(out.map(|mut o| {
                        o.held_before = Some(held.clone());
                        o
                    }));
                }
            });
        }
        let pulled = self.pull(to, from, None);
        cancelled.set(true);
        pulled?;
        let results = std::mem::take(&mut *results.borrow_mut());
        if !results.is_empty() {
            self.stats.sweep_race_fired += 1;
            self.note(format!("{}: the repair ran between two chunks of a sweep", LETTERS[to]));
        }
        for out in results {
            let out = out.map_err(|e| err("repair between chunks", e))?;
            self.stats.sweep_owed_at_repair += usize::from(out.owed);
            self.judge(to, rp, out);
        }
        Ok(())
    }

    // -- The ghost and the oracle --------------------------------------------------------------

    /// Read member `m`'s tombstones into its history, and count its ordinary resurrections.
    fn scan(&mut self, m: usize) {
        let generation = self.members[m].generation;
        let e = self.e(m);
        let (found, live) = held_events(&e);
        let created: [Option<Hlc>; 2] = std::array::from_fn(|c| {
            e.collection_by_id(cid(c as u8)).ok().flatten().map(|m| m.created)
        });
        let held: BTreeSet<Event> = found.iter().copied().collect();
        let (step, current) = (self.step, self.current);
        let member = &mut self.members[m];
        for event in member.ghost.keys() {
            if !held.contains(event)
                && !member.gone.contains_key(event)
                && let Some(current) = current
            {
                let replaced = match *event {
                    Event::Del { c, .. } | Event::IxDrop { c, .. } | Event::Drop { c, .. } => {
                        member.created[c as usize] != created[c as usize]
                    }
                    Event::DbDrop { .. } => false,
                };
                member.gone.insert(*event, (step, current, replaced));
            }
        }
        member.created = created;
        for event in &held {
            member.gone.remove(event);
        }
        let ghost = &mut member.ghost;
        for event in found {
            ghost.entry(event).or_insert(generation);
        }
        for (c, d, s) in live {
            if ghost.keys().any(|ev| ev.kills_doc(c, d, s)) {
                self.adr085.insert((m, c, d, s));
            }
        }
    }

    /// What member `m` holds, for a trace.
    fn state(&self, m: usize) -> String {
        let e = self.e(m);
        let mut out = format!("    {}:", LETTERS[m]);
        for c in 0..NAMES.len() as u8 {
            let meta = e.collection_by_id(cid(c)).ok().flatten();
            let docs: Vec<String> = rows(&e, cid(c))
                .iter()
                .filter(|(d, _)| **d < FILLER_FROM)
                .map(|(d, (s, live))| {
                    format!("{d}{}@{}", if *live { "" } else { "x" }, self.st(*s))
                })
                .collect();
            let tomb = e.collection_dropped_at(cid(c)).ok().flatten();
            let _ = write!(
                out,
                " {}[{} floor {} tomb {} docs {} idx {:?}]",
                n(c),
                meta.as_ref().map_or("-".into(), |m| self.rel(m.created)),
                meta.as_ref().and_then(|m| m.incarnation_floor).map_or("-".into(), |h| self.rel(h)),
                tomb.map_or("-".into(), |s| self.st(s)),
                docs.join(","),
                meta.map(|m| m.indexes.iter().map(|i| i.name.clone()).collect::<Vec<_>>())
                    .unwrap_or_default()
            );
        }
        let db = e.database_dropped_at(DB).ok().flatten();
        let h = e.tombstone_horizon().ok().flatten();
        let _ = write!(
            out,
            " db-tomb {} horizon {}",
            db.map_or("-".into(), |s| self.st(s)),
            h.map_or("-".into(), |h| self.rel(h))
        );
        out
    }

    /// Whether `m` holds, now, the evidence of `event`.
    fn holds(&self, m: usize, event: &Event) -> bool {
        let e = self.e(m);
        match *event {
            Event::Del { c, d, t } => {
                rows(&e, cid(c)).get(&d).is_some_and(|(s, live)| !live && *s >= t)
            }
            Event::Drop { c, t } => {
                e.collection_dropped_at(cid(c)).ok().flatten().is_some_and(|s| s >= t)
            }
            Event::DbDrop { t } => e.database_dropped_at(DB).ok().flatten().is_some_and(|s| s >= t),
            Event::IxDrop { c, ix, t } => {
                e.index_dropped_at(cid(c), ix).ok().flatten().is_some_and(|s| s >= t)
            }
        }
    }

    fn describe(&self, m: usize, event: &Event, generation: u8) -> String {
        let what = match *event {
            Event::Del { c, d, .. } => format!("delete of {}/{d}", n(c)),
            Event::Drop { c, .. } => format!("drop of {}", n(c)),
            Event::DbDrop { .. } => "database drop".into(),
            Event::IxDrop { c, ix, .. } => format!("index drop {}#{ix}", n(c)),
        };
        let wiped =
            if generation < self.members[m].generation { " (a wiped identity's)" } else { "" };
        let gone = match self.members[m].gone.get(event) {
            Some((step, by, replaced)) => format!(
                ", gone at step {step} ({by}){}",
                if *replaced { ", which replaced the collection's life" } else { "" }
            ),
            None => ", still held".into(),
        };
        format!("{}'s {what} at {}{wiped}{gone}", LETTERS[m], self.st(event.stamp()))
    }

    /// The key of a resurrection: which side's history kills it and what removed it.
    fn key_of(&self, cat: Cat, sides: &[(usize, &Event)], to: usize) -> String {
        let mut parts: Vec<String> = sides
            .iter()
            .map(|(m, event)| {
                let side = if *m == to { "own" } else { "sender" };
                let kind = match event {
                    Event::Del { .. } => "delete",
                    Event::Drop { .. } => "drop",
                    Event::DbDrop { .. } => "dbdrop",
                    Event::IxDrop { .. } => "ixdrop",
                };
                let by = match self.members[*m].gone.get(*event) {
                    Some((_, step, replaced)) => format!(
                        "{}{}",
                        format!("{step:?}").split([' ', '{']).next().unwrap_or(""),
                        if *replaced { "+life" } else { "" }
                    ),
                    None => "held".into(),
                };
                format!("{side}:{kind}:{by}")
            })
            .collect();
        parts.sort();
        parts.dedup();
        format!("{cat:?}[{}]", parts.join(","))
    }

    /// Judge what a repair on `to` from `from` created against the union of both members'
    /// histories, and measure the third member's.
    fn judge(&mut self, to: usize, from: usize, out: RepairOut) {
        let third = 3 - to - from;
        if let Some((mine, theirs)) = &out.held_before {
            for (m, events) in [(to, mine), (from, theirs)] {
                let generation = self.members[m].generation;
                for event in events {
                    self.members[m].ghost.entry(*event).or_insert(generation);
                }
            }
        }
        let what = if out.defs { "DEFINITIONS" } else { "CONVERGE" };
        if out.defs {
            self.stats.definitions += 1;
            self.stats.ambiguous += out.ambiguous;
        } else {
            self.stats.converges += 1;
            self.stats.left += out.left;
        }
        let horizons = format!(
            "own {} sender {}",
            out.own_horizon.map_or("-".into(), |h| self.rel(h)),
            out.sender_horizon.map_or("-".into(), |h| self.rel(h))
        );
        if !out.note.is_empty() {
            let s =
                format!("{}<-{} {what}({}): {}", LETTERS[to], LETTERS[from], n(out.c), out.note);
            self.note(s);
        }
        let count = if out.defs { out.built.len() } else { out.created.len() };
        if count > 0 && out.sender_horizon.is_none() {
            self.findings.push(Finding::plain(
                Cat::CreatedWithoutHorizon,
                format!(
                    "step {}: {}<-{} {what}({}) wrote {count} from a page without a horizon",
                    self.step,
                    LETTERS[to],
                    LETTERS[from],
                    n(out.c)
                ),
            ));
        }
        if !out.defs && count > 0 && out.owed {
            self.findings.push(Finding::plain(
                Cat::CreatedWhileOwed,
                format!(
                    "step {}: {}<-{} {what}({}) created {count} while the life owes a sweep",
                    self.step,
                    LETTERS[to],
                    LETTERS[from],
                    n(out.c)
                ),
            ));
        }
        if count != out.counted_created {
            self.findings.push(Finding::plain(
                Cat::CreatedCount,
                format!(
                    "step {}: {}<-{} {what}({}) counted {} and wrote {count}",
                    self.step,
                    LETTERS[to],
                    LETTERS[from],
                    n(out.c),
                    out.counted_created
                ),
            ));
        }
        // Documents.
        for (d, s) in &out.created {
            self.stats.created += 1;
            let note = format!(
                "{}<-{} {what}({}) created {d} at {} ({horizons}{})",
                LETTERS[to],
                LETTERS[from],
                n(out.c),
                self.st(*s),
                if out.owed { ", the life owes a sweep" } else { "" }
            );
            self.note(note.clone());
            if !self.writes.contains(&(out.c, *d, *s)) {
                self.findings.push(Finding::plain(
                    Cat::CreatedUnknownWrite,
                    format!("step {}: {note}", self.step),
                ));
            }
            let mut hits = Vec::new();
            let mut sides = Vec::new();
            let mut current = false;
            for m in [to, from] {
                for (event, generation) in &self.members[m].ghost {
                    if event.kills_doc(out.c, *d, *s) {
                        current |= *generation == self.members[m].generation;
                        hits.push(self.describe(m, event, *generation));
                        sides.push((m, event));
                    }
                }
            }
            if !hits.is_empty() && !current {
                self.stats.pre_wipe += 1;
                let s = format!("  (only a wiped identity's history: {})", hits.join(", "));
                self.note(s);
            } else if !hits.is_empty() {
                let cat = Cat::CreatedResurrection;
                let key = self.key_of(cat, &sides, to);
                self.findings.push(Finding {
                    cat,
                    key,
                    detail: format!("step {}: {note}; dead by {}", self.step, hits.join(", ")),
                });
            }
            self.third(third, *s, |ev| ev.kills_doc(out.c, *d, *s));
        }
        for (d, s) in &out.applied {
            let dead = [to, from]
                .iter()
                .any(|m| self.members[*m].ghost.keys().any(|ev| ev.kills_doc(out.c, *d, *s)));
            self.stats.spread_over_held += usize::from(dead);
        }
        // Indexes.
        for (name, created) in &out.built {
            self.stats.built += 1;
            let note = format!(
                "{}<-{} {what}({}) built {name} created {} ({horizons})",
                LETTERS[to],
                LETTERS[from],
                n(out.c),
                created.map_or("-".into(), |s| self.st(s))
            );
            self.note(note.clone());
            let Some(created) = *created else { continue };
            let ix = kimmy_core::IndexMeta::derive_id(name);
            let mut hits = Vec::new();
            let mut sides = Vec::new();
            let mut current = false;
            for m in [to, from] {
                for (event, generation) in &self.members[m].ghost {
                    if event.kills_index(out.c, ix, created) {
                        current |= *generation == self.members[m].generation;
                        hits.push(self.describe(m, event, *generation));
                        sides.push((m, event));
                    }
                }
            }
            if !hits.is_empty() && !current {
                self.stats.pre_wipe += 1;
                let s = format!("  (only a wiped identity's history: {})", hits.join(", "));
                self.note(s);
            } else if !hits.is_empty() {
                let cat = Cat::BuiltResurrection;
                let key = self.key_of(cat, &sides, to);
                self.findings.push(Finding {
                    cat,
                    key,
                    detail: format!("step {}: {note}; dropped by {}", self.step, hits.join(", ")),
                });
            }
            self.third(third, created, |ev| ev.kills_index(out.c, ix, created));
        }
    }

    /// The third member's window (design 13.3): a created row whose stamp the third
    /// member's history says is dead. Lost when the third member no longer holds the
    /// evidence (it collected or forgot it); otherwise its delete still wins on delivery.
    fn third(&mut self, m: usize, s: Stamp, kills: impl Fn(&Event) -> bool) {
        let hits: Vec<Event> =
            self.members[m].ghost.keys().filter(|ev| kills(ev)).copied().collect();
        if hits.is_empty() {
            return;
        }
        self.stats.third_member += 1;
        if !hits.iter().any(|ev| self.holds(m, ev)) {
            self.stats.third_member_lost += 1;
        }
        let window = hits
            .iter()
            .map(|ev| ev.stamp().hlc.wall_ms.saturating_sub(s.hlc.wall_ms))
            .max()
            .unwrap_or(0)
            / self.k;
        self.stats.third_window_max_ms = self.stats.third_window_max_ms.max(window);
    }
}

// -- Running a schedule ------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct Outcome {
    pub findings: Vec<Finding>,
    pub stats: Stats,
    pub report: String,
    pub k: u64,
}

impl Outcome {
    fn signature(&self) -> BTreeSet<String> {
        self.findings.iter().map(|f| f.key.clone()).collect()
    }
}

pub(crate) fn check(schedule: &Schedule) -> Outcome {
    let mut k = 1;
    let mut retries = 0;
    loop {
        let mut cluster = Cluster::new(&schedule.cfg, k);
        let result = cluster.run(schedule);
        if cluster.overrun && k < 64 {
            k *= 2;
            retries += 1;
            continue;
        }
        let mut findings = std::mem::take(&mut cluster.findings);
        if let Err(e) = result {
            findings.push(Finding::plain(Cat::Error, e));
        }
        let mut stats = std::mem::take(&mut cluster.stats);
        stats.adr085 = cluster.adr085.len();
        stats.schedules = 1;
        stats.retries = retries;
        let mut report = String::new();
        if !findings.is_empty() || std::env::var("YSM_TRACE").is_ok() {
            let _ = writeln!(report, "  notes:");
            for n in &cluster.notes {
                let _ = writeln!(report, "  {n}");
            }
            let _ = writeln!(report, "  horizons at the end: {}", cluster.horizons());
        }
        return Outcome { findings, stats, report, k };
    }
}

fn render(schedule: &Schedule, outcome: &Outcome) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "{}", schedule.show());
    let _ = writeln!(out, "  as rust: {}", schedule.as_rust());
    let _ = writeln!(out, "  time scale k = {}", outcome.k);
    for f in &outcome.findings {
        let _ = writeln!(out, "  {}: {}", f.key, f.detail);
    }
    out.push_str(&outcome.report);
    out
}

/// Remove steps, then simplify the configuration, while the failure keeps its keys.
pub(crate) fn minimise(schedule: &Schedule, want: &BTreeSet<String>) -> Schedule {
    let fails = |s: &Schedule| {
        let got = check(s).signature();
        want.iter().all(|c| got.contains(c))
    };
    let mut current = schedule.clone();
    loop {
        let mut shrunk = false;
        let mut i = current.steps.len();
        while i > 0 {
            i -= 1;
            let mut candidate = current.clone();
            candidate.steps.remove(i);
            if fails(&candidate) {
                current = candidate;
                shrunk = true;
            }
        }
        for m in 0..3 {
            let simpler: [fn(&mut Config, usize); 2] =
                [|c, m| c.skew[m] = 0, |c, m| c.keep_oplog[m] = true];
            for simpler in simpler {
                let mut candidate = current.clone();
                simpler(&mut candidate.cfg, m);
                if candidate != current && fails(&candidate) {
                    current = candidate;
                    shrunk = true;
                }
            }
        }
        if current.cfg.same_name {
            let mut candidate = current.clone();
            candidate.cfg.same_name = false;
            if fails(&candidate) {
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

/// SplitMix64: reproducible from a seed.
pub(crate) struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
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

    fn m(&mut self) -> u8 {
        self.below(3) as u8
    }

    fn pair(&mut self) -> (u8, u8) {
        let to = self.m();
        (to, (to + 1 + self.below(2) as u8) % 3)
    }
}

/// A seeded schedule of `len` random steps after a creation the others pull.
pub(crate) fn generate(seed: u64, len: usize) -> Schedule {
    let mut rng = Rng::new(seed);
    let cfg = Config {
        retention: std::array::from_fn(|_| rng.below(RETENTIONS.len() as u64) as u8),
        skew: std::array::from_fn(|_| if rng.below(14) == 0 { 4 } else { rng.below(4) as u8 }),
        keep_oplog: std::array::from_fn(|_| rng.below(2) == 0),
        same_name: rng.below(4) == 0,
    };
    let first = rng.m();
    let mut steps = vec![
        Step::Create { m: first, c: ORDERS },
        Step::Pull { to: (first + 1) % 3, from: first },
        Step::Pull { to: (first + 2) % 3, from: first },
    ];
    let filler_at = if rng.below(6) == 0 { Some(rng.below(len as u64) as usize) } else { None };
    let coll = |rng: &mut Rng| if rng.below(5) == 0 { 1 } else { ORDERS };
    let doc = |rng: &mut Rng| 1 + rng.below(3) as u8;
    for i in 0..len {
        if filler_at == Some(i) {
            steps.push(Step::Filler { m: rng.m(), c: ORDERS });
            continue;
        }
        let roll = rng.below(1000);
        let step = match roll {
            0..=159 => Step::Write { m: rng.m(), c: coll(&mut rng), d: doc(&mut rng) },
            160..=249 => Step::Delete { m: rng.m(), c: coll(&mut rng), d: doc(&mut rng) },
            250..=274 => Step::Create { m: rng.m(), c: coll(&mut rng) },
            275..=299 => Step::Drop { m: rng.m(), c: coll(&mut rng) },
            300..=309 => Step::DropDb { m: rng.m() },
            310..=339 => Step::Index { m: rng.m(), c: coll(&mut rng), ix: rng.below(2) as u8 },
            340..=359 => Step::DropIndex { m: rng.m(), c: coll(&mut rng), ix: rng.below(2) as u8 },
            360..=449 => Step::Gc { m: rng.m() },
            450..=474 => Step::Restart {
                m: rng.m(),
                r: if rng.below(2) == 0 { SAME } else { rng.below(RETENTIONS.len() as u64) as u8 },
            },
            475..=486 => Step::Kill { m: rng.m(), how: rng.below(2) as u8 },
            487..=504 => Step::Foreign { m: rng.m() },
            505..=524 => Step::Backup { m: rng.m() },
            525..=539 => Step::Restore { m: rng.m(), b: rng.below(2) as u8 },
            540..=547 => {
                let (m, from) = rng.pair();
                Step::Wipe { m, from }
            }
            548..=559 => Step::Skew { m: rng.m(), s: rng.below(SKEWS.len() as u64) as u8 },
            560..=679 => {
                let (to, from) = rng.pair();
                Step::Pull { to, from }
            }
            680..=739 => {
                let (to, from) = rng.pair();
                let d = if rng.below(5) == 0 { 9 } else { doc(&mut rng) };
                Step::Lose { to, from, d }
            }
            740..=769 => {
                let (to, from) = rng.pair();
                Step::Whole { to, from }
            }
            770..=784 => {
                let (to, from) = rng.pair();
                Step::Scoped { to, from, c: coll(&mut rng) }
            }
            785..=884 => {
                let (to, from) = rng.pair();
                Step::Converge { to, from, c: coll(&mut rng) }
            }
            885..=919 => {
                let (to, from) = rng.pair();
                Step::ConvergeRacingGc { to, from, c: coll(&mut rng) }
            }
            920..=979 => {
                let (to, from) = rng.pair();
                Step::Definitions { to, from, c: coll(&mut rng) }
            }
            _ => {
                let (to, from) = rng.pair();
                let rp = 3 - to - from;
                Step::PullSweepRace { to, from, rp, defs: rng.below(2) == 0 }
            }
        };
        steps.push(step);
    }
    Schedule { cfg, steps }
}

// -- Exhaustive families -----------------------------------------------------------------------

/// A fixed prefix and a reduced alphabet, every sequence of `depth` of it enumerated.
pub(crate) struct Family {
    pub name: &'static str,
    pub cfg: Config,
    pub prefix: Vec<Step>,
    pub alphabet: Vec<Step>,
    pub depth: usize,
}

impl Family {
    fn size(&self) -> usize {
        self.alphabet.len().pow(self.depth as u32)
    }

    fn schedule(&self, mut index: usize) -> Schedule {
        let mut steps = self.prefix.clone();
        for _ in 0..self.depth {
            steps.push(self.alphabet[index % self.alphabet.len()]);
            index /= self.alphabet.len();
        }
        Schedule { cfg: self.cfg.clone(), steps }
    }
}

use Step::*;

const A: u8 = 0;
const B: u8 = 1;
const C: u8 = 2;
const O: u8 = 0;
const K: u8 = 0;

pub(crate) fn families() -> Vec<Family> {
    // Retention slot 0 (2 ms) collects at the next step; 5 never collects.
    let short = Config { retention: [0, 0, 5], ..Config::default() };
    vec![
        // Documents between one pair: deletes, collection on either side, the race at the
        // page, a lost insert, retention changed at a restart.
        Family {
            name: "documents",
            cfg: short.clone(),
            prefix: vec![
                Create { m: A, c: O },
                Write { m: A, c: O, d: 1 },
                Pull { to: B, from: A },
                Pull { to: C, from: A },
            ],
            alphabet: vec![
                Delete { m: A, c: O, d: 1 },
                Delete { m: B, c: O, d: 1 },
                Write { m: A, c: O, d: 2 },
                Gc { m: A },
                Gc { m: B },
                Restart { m: B, r: 4 },
                Lose { to: B, from: A, d: 2 },
                Pull { to: A, from: B },
                Converge { to: A, from: B, c: O },
                Converge { to: B, from: A, c: O },
                ConvergeRacingGc { to: A, from: B, c: O },
            ],
            depth: 4,
        },
        // What forgets a delete: the 0.49.0 release's passes, a killed start, a restore, a
        // wipe.
        Family {
            name: "forgetting",
            cfg: short.clone(),
            prefix: vec![
                Create { m: A, c: O },
                Pull { to: B, from: A },
                Backup { m: B },
                Write { m: A, c: O, d: 1 },
                Pull { to: B, from: A },
                Pull { to: C, from: A },
            ],
            alphabet: vec![
                Delete { m: B, c: O, d: 1 },
                Gc { m: B },
                Foreign { m: B },
                Restart { m: B, r: SAME },
                Kill { m: B, how: 0 },
                Kill { m: B, how: 1 },
                Restore { m: B, b: 0 },
                Backup { m: B },
                Wipe { m: B, from: C },
                Converge { to: B, from: A, c: O },
                Pull { to: A, from: B },
            ],
            depth: 4,
        },
        // The same, with this build calling itself 0.49.0 (review revA M1).
        Family {
            name: "forgetting-same-name",
            cfg: Config { same_name: true, ..short.clone() },
            prefix: vec![
                Create { m: A, c: O },
                Write { m: A, c: O, d: 1 },
                Pull { to: B, from: A },
            ],
            alphabet: vec![
                Delete { m: B, c: O, d: 1 },
                Gc { m: B },
                Foreign { m: B },
                Restart { m: B, r: SAME },
                Kill { m: B, how: 0 },
                Converge { to: B, from: A, c: O },
            ],
            depth: 5,
        },
        // Collection, database and index drops, recreation, and the definition repair.
        Family {
            name: "drops-and-indexes",
            cfg: short.clone(),
            prefix: vec![
                Create { m: A, c: O },
                Index { m: A, c: O, ix: K },
                Write { m: A, c: O, d: 1 },
                Pull { to: B, from: A },
            ],
            alphabet: vec![
                DropIndex { m: A, c: O, ix: K },
                Drop { m: A, c: O },
                DropDb { m: A },
                Create { m: A, c: O },
                Gc { m: A },
                Converge { to: A, from: B, c: O },
                Definitions { to: A, from: B, c: O },
                Pull { to: B, from: A },
                Index { m: A, c: O, ix: K },
            ],
            depth: 4,
        },
        // Three members: the sender clause (a sender that collected and had the row pushed
        // back) and the whole-database adoption.
        Family {
            name: "three-members",
            cfg: Config { retention: [5, 0, 5], ..Config::default() },
            prefix: vec![
                Create { m: A, c: O },
                Pull { to: B, from: A },
                Pull { to: C, from: A },
                Backup { m: B },
                Write { m: A, c: O, d: 1 },
                Pull { to: B, from: A },
            ],
            alphabet: vec![
                Delete { m: B, c: O, d: 1 },
                Gc { m: B },
                Whole { to: B, from: A },
                Whole { to: B, from: C },
                Restore { m: B, b: 0 },
                Converge { to: C, from: B, c: O },
                Converge { to: B, from: A, c: O },
                Converge { to: B, from: C, c: O },
                Pull { to: C, from: B },
            ],
            depth: 4,
        },
        // The delete already made: what forgets it, under this build's real name, at depth 4.
        Family {
            name: "forgetting-deleted",
            cfg: short.clone(),
            prefix: vec![
                Create { m: A, c: O },
                Pull { to: B, from: A },
                Backup { m: B },
                Write { m: A, c: O, d: 1 },
                Pull { to: B, from: A },
                Pull { to: C, from: A },
                Delete { m: B, c: O, d: 1 },
            ],
            alphabet: vec![
                Gc { m: B },
                Foreign { m: B },
                Restart { m: B, r: SAME },
                Kill { m: B, how: 0 },
                Kill { m: B, how: 1 },
                Restore { m: B, b: 0 },
                Backup { m: B },
                Restore { m: B, b: 1 },
                Wipe { m: B, from: C },
                Converge { to: B, from: A, c: O },
                Whole { to: B, from: C },
            ],
            depth: 4,
        },
        // The index sender clause: a sender that dropped an index, collected the drop and had
        // the index pushed back, and a requester that lost the creation.
        Family {
            name: "index-sender",
            cfg: Config { retention: [5, 0, 5], ..Config::default() },
            prefix: vec![
                Create { m: A, c: O },
                Index { m: A, c: O, ix: K },
                Pull { to: B, from: A },
                Lose { to: C, from: A, d: 9 },
            ],
            alphabet: vec![
                DropIndex { m: B, c: O, ix: K },
                Gc { m: B },
                Whole { to: B, from: A },
                Scoped { to: B, from: A, c: O },
                Definitions { to: C, from: B, c: O },
                Definitions { to: B, from: A, c: O },
                Definitions { to: C, from: A, c: O },
                Pull { to: C, from: B },
            ],
            depth: 4,
        },
        // A drop that reaches a newer life late, swept in two chunks with a repair between.
        late_drop_family("late-database-drop", DropDb { m: A }),
        late_drop_family("late-collection-drop", Drop { m: A, c: O }),
        late_drop_unheld_family(),
    ]
}

/// The third member's window (design 13.3), measured: C deletes a row the pair still holds
/// or lost, and collects the tombstone; the pair's horizons move only when they collect.
/// Nothing here is judged against the pair (C's history is not the pair's): the counts are
/// the measurement.
pub(crate) fn third_member_families() -> Vec<Family> {
    let make = |name: &'static str, retention: [u8; 3]| Family {
        name,
        cfg: Config { retention, ..Config::default() },
        prefix: vec![
            Create { m: A, c: O },
            Pull { to: B, from: A },
            Pull { to: C, from: A },
            Write { m: A, c: O, d: 1 },
            Pull { to: C, from: A },
            Lose { to: B, from: A, d: 1 },
        ],
        alphabet: vec![
            Delete { m: C, c: O, d: 1 },
            Gc { m: C },
            Gc { m: A },
            Gc { m: B },
            Delete { m: A, c: O, d: 2 },
            Converge { to: B, from: A, c: O },
            Pull { to: A, from: C },
        ],
        depth: 4,
    };
    vec![make("third-member-unequal", [5, 5, 0]), make("third-member-equal", [0, 0, 0])]
}

/// A database drop by a member that never held the collection, so it carries no collection
/// tombstone of it: only the database's tombstone says the life before is over.
fn late_drop_unheld_family() -> Family {
    Family {
        name: "late-database-drop-unheld",
        cfg: Config { retention: [5, 5, 5], ..Config::default() },
        prefix: vec![
            Create { m: C, c: O },
            Index { m: C, c: O, ix: K },
            Write { m: C, c: O, d: 1 },
            Create { m: A, c: 1 },
            DropDb { m: A },
            Create { m: B, c: O },
            Filler { m: B, c: O },
        ],
        alphabet: vec![
            Pull { to: B, from: C },
            Lose { to: B, from: C, d: 1 },
            Lose { to: B, from: C, d: 9 },
            Pull { to: B, from: A },
            PullSweepRace { to: B, from: A, rp: C, defs: false },
            PullSweepRace { to: B, from: A, rp: C, defs: true },
            Converge { to: B, from: C, c: O },
            Definitions { to: B, from: C, c: O },
            Write { m: C, c: O, d: 2 },
        ],
        depth: 4,
    }
}

fn late_drop_family(name: &'static str, drop: Step) -> Family {
    Family {
        name,
        cfg: Config { retention: [5, 5, 5], ..Config::default() },
        prefix: vec![
            Create { m: A, c: O },
            Pull { to: C, from: A },
            Index { m: C, c: O, ix: K },
            Write { m: C, c: O, d: 1 },
            drop,
            // B makes its own life after the drop, never having heard of it.
            Create { m: B, c: O },
            Filler { m: B, c: O },
        ],
        alphabet: vec![
            Pull { to: B, from: C },
            Lose { to: B, from: C, d: 9 },
            Pull { to: B, from: A },
            PullSweepRace { to: B, from: A, rp: C, defs: false },
            PullSweepRace { to: B, from: A, rp: C, defs: true },
            Converge { to: B, from: C, c: O },
            Definitions { to: B, from: C, c: O },
            Write { m: C, c: O, d: 2 },
        ],
        depth: 4,
    }
}

// -- The drivers -------------------------------------------------------------------------------

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Run `schedules` on `threads` threads; return the totals and every failing schedule.
fn run_all(
    make: &(dyn Fn(usize) -> Schedule + Sync),
    count: usize,
) -> (Stats, Vec<(usize, Schedule, Outcome)>) {
    let skip: BTreeSet<usize> = std::env::var("YSM_SKIP")
        .unwrap_or_default()
        .split(',')
        .filter_map(|v| v.trim().parse().ok())
        .collect();
    let threads = env_usize("YSM_THREADS", 14);
    let stop_at_first = std::env::var("YSM_STOP_AT_FIRST").is_ok();
    let next = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let failures = Mutex::new(Vec::new());
    let total = Mutex::new(Stats::default());
    std::thread::scope(|scope| {
        for _ in 0..threads {
            let worker = std::thread::Builder::new().stack_size(32 << 20);
            worker
                .spawn_scoped(scope, || {
                    let mut stats = Stats::default();
                    loop {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= count {
                            break;
                        }
                        if skip.contains(&i) {
                            continue;
                        }
                        let schedule = make(i);
                        let outcome = check(&schedule);
                        stats.add(&outcome.stats);
                        if !outcome.findings.is_empty() {
                            if stop_at_first {
                                stop.store(true, Ordering::Relaxed);
                            }
                            failures.lock().unwrap().push((i, schedule, outcome));
                        }
                    }
                    total.lock().unwrap().add(&stats);
                })
                .expect("a worker thread");
        }
    });
    (total.into_inner().unwrap(), failures.into_inner().unwrap())
}

/// Report the failures, one minimised schedule per distinct signature.
fn report(
    name: &str,
    stats: &Stats,
    failures: Vec<(usize, Schedule, Outcome)>,
    elapsed: std::time::Duration,
) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{name}: {} schedules, {} steps, {:.1} s; {} failing\n  stats: {stats:?}",
        stats.schedules,
        stats.steps,
        elapsed.as_secs_f64(),
        failures.len()
    );
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    let minimise_cap = env_usize("YSM_MINIMISE", 6);
    let mut failures = failures;
    failures.sort_by_key(|(_, s, _)| s.steps.len());
    let mut minimised = 0;
    let indexes: BTreeSet<usize> = failures.iter().map(|(i, _, _)| *i).collect();
    for (index, schedule, outcome) in &failures {
        for key in outcome.signature() {
            let first = !seen.contains_key(&key);
            *seen.entry(key.clone()).or_default() += 1;
            if !first {
                continue;
            }
            let _ =
                writeln!(out, "--- failing ({key}), index {index}:\n{}", render(schedule, outcome));
            if minimised < minimise_cap {
                minimised += 1;
                let small = minimise(schedule, &BTreeSet::from([key.clone()]));
                let again = check(&small);
                let _ = writeln!(out, "--- minimised ({key}):\n{}", render(&small, &again));
            }
        }
    }
    let _ = writeln!(out, "--- keys and how many failing schedules carry each: {seen:?}");
    let _ = writeln!(out, "--- failing indexes: {indexes:?}");
    out
}

fn write_report(name: &str, text: &str) {
    if let Ok(dir) = std::env::var("YSM_REPORT_DIR") {
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::write(Path::new(&dir).join(format!("{name}.txt")), text);
    }
    eprintln!("{text}");
}

/// The seeded generator over long schedules (`YSM_SEEDS`, `YSM_LEN`, `YSM_SEED0`).
#[test]
#[ignore = "the long seeded run; see the young-stamp model test report"]
fn young_stamp_seeded() {
    let seeds = env_usize("YSM_SEEDS", 600);
    let len = env_usize("YSM_LEN", 70);
    let seed0 = env_usize("YSM_SEED0", 0) as u64;
    let started = std::time::Instant::now();
    let (stats, failures) = run_all(&|i| generate(seed0 + i as u64, len), seeds);
    let failed = !failures.is_empty();
    let text = report("seeded", &stats, failures, started.elapsed());
    write_report("seeded", &text);
    assert!(!failed, "the seeded run found failing schedules");
}

/// Every sequence of each family's reduced alphabet at its depth (`YSM_FAMILY` picks one).
#[test]
#[ignore = "the exhaustive run; see the young-stamp model test report"]
fn young_stamp_exhaustive() {
    let only = std::env::var("YSM_FAMILY").ok();
    let mut failed = false;
    for family in families() {
        if only.as_deref().is_some_and(|o| !family.name.contains(o)) {
            continue;
        }
        let started = std::time::Instant::now();
        let (stats, failures) = run_all(&|i| family.schedule(i), family.size());
        failed |= !failures.is_empty();
        let text = report(family.name, &stats, failures, started.elapsed());
        write_report(family.name, &text);
        if failed && std::env::var("YSM_STOP_AT_FIRST").is_ok() {
            break;
        }
    }
    assert!(!failed, "the exhaustive run found failing schedules");
}

/// The third member's window, measured (not judged): see [`third_member_families`].
#[test]
#[ignore = "the third member's window, measured; see the young-stamp model test report"]
fn young_stamp_third_member_window() {
    for family in third_member_families() {
        let started = std::time::Instant::now();
        let (stats, failures) = run_all(&|i| family.schedule(i), family.size());
        let text = report(family.name, &stats, failures, started.elapsed());
        write_report(family.name, &text);
    }
}

/// A short seeded run and every family at depth two, for the ordinary suite.
#[test]
fn young_stamp_smoke() {
    let started = std::time::Instant::now();
    let (mut stats, mut failures) = run_all(&|i| generate(0x5eed_0000 + i as u64, 40), 16);
    for family in families() {
        let shallow = Family { depth: 2, ..family };
        let (s, f) = run_all(&|i| shallow.schedule(i), shallow.size());
        stats.add(&s);
        failures.extend(f);
    }
    let failed = !failures.is_empty();
    let text = report("smoke", &stats, failures, started.elapsed());
    eprintln!("{text}");
    assert!(!failed, "the young-stamp smoke run found failing schedules:\n{text}");
}

/// Replay one schedule: the seeded schedule `YSM_REPLAY_SEED` (with `YSM_LEN`), or the named
/// row `YSM_ROW`. `YSM_TRACE` prints every member's state after every step.
#[test]
#[ignore = "a single replay"]
fn young_stamp_replay() {
    let schedule = match std::env::var("YSM_ROW") {
        Ok(name) => rows_named().into_iter().find(|(n, _)| *n == name).expect("a named row").1,
        Err(_) => generate(env_usize("YSM_REPLAY_SEED", 0) as u64, env_usize("YSM_LEN", 70)),
    };
    let outcome = check(&schedule);
    eprintln!("{}", render(&schedule, &outcome));
    if !outcome.findings.is_empty() {
        let small = minimise(&schedule, &outcome.signature());
        let again = check(&small);
        eprintln!("--- minimised:\n{}", render(&small, &again));
    }
}

// -- Named rows: the minimised findings, each failing on this tree ------------------------------

pub(crate) fn rows_named() -> Vec<(&'static str, Schedule)> {
    vec![
        // YS1: B drops `orders` and makes a new life; C writes 1 after the drop, not having
        // heard of it; A pulls C's write and deletes it, also after the drop. When the drop
        // reaches A its life holds nothing live after the drop (only the tombstone), so it
        // is buried, and the burial's purge removes the tombstone. Nothing moves A's horizon:
        // C's row is young on A and the repair writes back a document A deleted.
        ("ys1", ys1(Converge { to: A, from: C, c: O })),
        // YS1's control: the count repair's scoped page brings the same row back (not judged:
        // the oracle looks at what the divergence repair creates only).
        ("ys1-control-scoped", ys1(Scoped { to: A, from: C, c: O })),
        // YS2: a restore resets the horizon to its own wall-clock time, but the deletes it forgot
        // were stamped by the HLC, which a fast peer (C, 100 s ahead) has pushed past every
        // wall clock. A's correct clock restores below its own delete.
        (
            "ys2",
            Schedule {
                cfg: Config { retention: [5, 5, 5], skew: [0, 0, 4], ..Config::default() },
                steps: vec![
                    Create { m: A, c: O },
                    Pull { to: B, from: A },
                    Pull { to: C, from: A },
                    Backup { m: A },
                    Write { m: C, c: O, d: 2 },
                    Pull { to: A, from: C },
                    Write { m: A, c: O, d: 1 },
                    Pull { to: B, from: A },
                    Delete { m: A, c: O, d: 1 },
                    Restore { m: A, b: 0 },
                    Converge { to: A, from: B, c: O },
                ],
            },
        ),
        // YS2's control: a plain pull brings the same row back by entries (A's restored
        // vector is the backup's), so the repair loses nothing the restore had not.
        (
            "ys2-control-pull",
            Schedule {
                cfg: Config { retention: [5, 5, 5], skew: [0, 0, 4], ..Config::default() },
                steps: vec![
                    Create { m: A, c: O },
                    Pull { to: B, from: A },
                    Pull { to: C, from: A },
                    Backup { m: A },
                    Write { m: C, c: O, d: 2 },
                    Pull { to: A, from: C },
                    Write { m: A, c: O, d: 1 },
                    Pull { to: B, from: A },
                    Delete { m: A, c: O, d: 1 },
                    Restore { m: A, b: 0 },
                    Pull { to: A, from: B },
                ],
            },
        ),
        // A database drop reaching B's newer life, a document repair between two sweep chunks.
        ("late-db-race", {
            let family = late_drop_family("late-database-drop", DropDb { m: A });
            let mut steps = family.prefix;
            steps.extend([
                Write { m: C, c: O, d: 2 },
                Pull { to: B, from: C },
                PullSweepRace { to: B, from: A, rp: C, defs: false },
            ]);
            Schedule { cfg: family.cfg, steps }
        }),
        ("late-db-unheld-race", {
            let family = late_drop_unheld_family();
            let mut steps = family.prefix;
            steps.extend([PullSweepRace { to: B, from: A, rp: C, defs: false }]);
            Schedule { cfg: family.cfg, steps }
        }),
        (
            "seeded-1",
            Schedule {
                cfg: Config {
                    retention: [2, 4, 5],
                    skew: [0, 2, 0],
                    keep_oplog: [true, true, true],
                    same_name: false,
                },
                steps: vec![
                    Step::Create { m: 2, c: 0 },
                    Step::Pull { to: 0, from: 2 },
                    Step::Pull { to: 1, from: 2 },
                    Step::Drop { m: 1, c: 0 },
                    Step::Write { m: 0, c: 0, d: 1 },
                    Step::Create { m: 1, c: 0 },
                    Step::Write { m: 2, c: 0, d: 1 },
                    Step::PullSweepRace { to: 2, from: 1, rp: 0, defs: false },
                    Step::Delete { m: 0, c: 0, d: 1 },
                    Step::Pull { to: 0, from: 2 },
                    Step::ConvergeRacingGc { to: 0, from: 2, c: 0 },
                ],
            },
        ),
    ]
}

fn ys1(last: Step) -> Schedule {
    Schedule {
        cfg: Config { retention: [5, 5, 5], ..Config::default() },
        steps: vec![
            Create { m: C, c: O },
            Pull { to: A, from: C },
            Pull { to: B, from: C },
            Drop { m: B, c: O },
            Create { m: B, c: O },
            Write { m: C, c: O, d: 1 },
            Pull { to: A, from: C },
            Delete { m: A, c: O, d: 1 },
            Pull { to: A, from: B },
            last,
        ],
    }
}

/// A finding's row: the repair must create nothing a history says is dead.
fn finding(name: &str) {
    let (_, schedule) = rows_named().into_iter().find(|(n, _)| *n == name).expect("a named row");
    let outcome = check(&schedule);
    assert!(outcome.findings.is_empty(), "finding {name}:\n{}", render(&schedule, &outcome));
}

#[test]
#[ignore = "finding YS1, fails on this tree: see the young-stamp model test report"]
fn finding_ys1_a_burial_of_a_re_dated_life_forgets_a_delete_stamped_after_the_drop() {
    finding("ys1");
}

#[test]
#[ignore = "finding YS2, fails on this tree: see the young-stamp model test report"]
fn finding_ys2_a_restore_resets_below_its_own_deletes_when_the_hlc_leads_the_wall_clock() {
    finding("ys2");
}
