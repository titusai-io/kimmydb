//! The resolve row is the pure function (ADR-272; Q6.6, the resolve row, unit R3).
//!
//! The settle pass keeps one row per swapped name, and a resolution reads it instead of the name's events. The row is a cache with a
//! rule: it is deleted by every transaction that makes a collection arrive or leave, written by the pass where the pass runs, and
//! checked by every reader (the newest event of the name is the one it was decided against, and the physical it names is alive), with
//! the computation from the events as the fallback. This test is what proves the rule complete.
//!
//! A seeded generator drives a member through random sequences of the operations that touch what a name serves, and after EVERY
//! operation, for every name it touched, it asserts:
//!
//! 1. `resolve` (the row path) answers exactly what `resolve_computed` (the events and the collections alive) answers;
//! 2. a name with no event has no row, and a current row says what the computation says;
//! 3. the fallback counter moved by exactly one when the row was missing or stale for a name that has events, and by none otherwise;
//! 4. after an operation that runs the settle pass (a binding or a drop delivered by a window, a local drop or a recreation, a restore,
//!    a snapshot join, the pass itself) the row of a name that has a stage row is current: a pass site that forgot to write it is the
//!    finding, and the fallback would hide it from (1) alone. A name with an event and no stage row (`ghost`, made by hand through the
//!    door, a state no member should reach) cannot be named by a page or a restore, so it is held to (1) to (3) only.
//!
//! The case and operation counts are fixed (`SEQUENCES`, `STEPS`; `RESOLVE_ROW_SEQUENCES` overrides the first), and a failure names the
//! seed and prints the steps so that it replays.

use kimmy_core::{
    CollectionBind, CollectionId, Hlc, MergePolicy, OpKind, OplogEntry, RegisterEvent, Stamp,
    VersionVector,
};
use redb::ReadableDatabase;

use crate::register::{VisibleKind, VisibleRow};
use crate::{Engine, PeerWindow, SnapshotProgress, SyncOutcome, WalkScope, physical_now_ms};

const DB: &str = "app";
/// Sequences of operations per run, and operations in each.
const SEQUENCES: u64 = 100;
const STEPS: usize = 30;

/// The kinds of operation the generator drives; each must take effect at least once in a run.
const KINDS: [&str; 12] = [
    "new stage",
    "bind",
    "drop entry",
    "local drop",
    "recreate",
    "alive after bind",
    "stored with no pass",
    "forget",
    "restore",
    "page-make",
    "the pass alone",
    "an event with no stage row",
];

/// xorshift64*: a seeded source with no dependency, so a failing seed replays on any machine.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

/// A window of `entries` delivered to `b`: `None` when the engine refused it, which an operation of the generator may provoke.
fn window(b: &Engine, entries: &[OplogEntry]) -> Option<SyncOutcome> {
    let mut theirs = VersionVector::default();
    for entry in entries {
        theirs.observe(entry.stamp);
    }
    let mine = b.witnessed_vector().ok()?;
    let mut outcome = SyncOutcome::default();
    b.apply_peer_window_into(
        &theirs,
        Some(&mine),
        entries,
        PeerWindow {
            scanned_to: entries.last().map_or(Hlc::ZERO, |e| e.stamp.hlc),
            exhausted: true,
            passed_through: None,
            asked_partial: false,
        },
        &mut outcome,
    )
    .ok()?;
    Some(outcome)
}

fn creation_of(a: &Engine, physical: &str) -> OplogEntry {
    everything(a)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, physical)
        })
        .expect("the author logged the stage's creation")
}

fn bind_of(a: &Engine, physical: &str, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), a.node_id(), a.epoch()),
        kind: OpKind::BindCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionBind {
                db: DB.into(),
                name: "orders".into(),
                physical: physical.into(),
            })
            .unwrap(),
        ),
        meta: None,
    }
}

fn drop_of_orders(a: &Engine, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), a.node_id(), a.epoch()),
        kind: OpKind::DropCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(bson::serialize_to_vec(&kimmy_core::CollectionRef::new(DB, "orders")).unwrap()),
        meta: None,
    }
}

/// A whole-database pull of `sender` into `joiner`; whether it completed.
fn pull(joiner: &Engine, sender: &Engine) -> bool {
    let mut progress = SnapshotProgress::whole_database();
    for _ in 0..50 {
        if progress.is_complete() {
            return true;
        }
        let Ok(page) = sender.snapshot_page(progress.after().cloned(), progress.scope()) else {
            return false;
        };
        if joiner.apply_snapshot_page(sender.node_id(), &mut progress, &page).is_err() {
            return false;
        }
    }
    false
}

/// What a resolution answers, as a value: the physical, the collection's id and the stamp that made it visible, or the error's text.
type Answer = Result<(String, u64, Option<Stamp>), String>;

fn answer(resolved: crate::Result<crate::Resolved>) -> Answer {
    resolved
        .map(|resolved| (resolved.physical, resolved.meta.id.0, resolved.visible))
        .map_err(|e| e.to_string())
}

struct StageRecord {
    name: String,
    creation: OplogEntry,
}

/// The member under test, the author of its stages and entries, and what has been done.
struct World {
    author: Engine,
    _author_dir: tempfile::TempDir,
    member: Engine,
    _member_dir: tempfile::TempDir,
    stages: Vec<StageRecord>,
    wall: u64,
    steps: Vec<String>,
    seed: u64,
    /// How many times each kind of operation TOOK EFFECT (changed the member's state), so that the run can prove it exercised every
    /// transition and not only attempted it.
    effective: std::collections::BTreeMap<&'static str, usize>,
}

impl World {
    fn new(seed: u64) -> Self {
        let (author, _author_dir) = engine();
        let (member, _member_dir) = engine();
        member.create_collection(DB, "plain").unwrap();
        // far enough above the real clock that no stamp made here is made after the author's, and below every stage's deadline
        let wall = physical_now_ms() + 5_000;
        let (stages, steps) = (Vec::new(), Vec::new());
        let effective = std::collections::BTreeMap::new();
        Self { author, _author_dir, member, _member_dir, stages, wall, steps, seed, effective }
    }

    fn took(&mut self, kind: &'static str) {
        *self.effective.entry(kind).or_default() += 1;
    }

    /// Whether the member holds the register's stage row of at least one stage of `orders`.
    fn has_a_stage_row(&self) -> bool {
        let txn = self.member.db().begin_read().unwrap();
        self.stages.iter().any(|stage| {
            let id = CollectionId::derive(DB, &stage.name);
            crate::register::stage_row_named_read(&txn, id).unwrap().is_some()
        })
    }

    /// The newest event stored for `orders`, which an operation that stored one changes.
    fn newest(&self) -> Option<Stamp> {
        let txn = self.member.db().begin_read().unwrap();
        crate::register::newest_stored_event_read(&txn, DB, "orders").unwrap()
    }

    fn next_wall(&mut self) -> u64 {
        self.wall += 1;
        self.wall
    }

    fn pick(&self, rng: &mut Rng) -> Option<usize> {
        (!self.stages.is_empty()).then(|| rng.below(self.stages.len()))
    }

    /// One operation; whether it ran the settle pass for `orders` (so that the row must be current after it).
    fn step(&mut self, rng: &mut Rng) -> (String, bool) {
        match rng.below(13) {
            0 | 1 => {
                let name = format!("orders.__stg.s{}", self.stages.len());
                let deadline = Hlc::new(physical_now_ms() + 600_000, 0);
                if self.author.create_stage(DB, &name, MergePolicy::Document, deadline).is_ok() {
                    let creation = creation_of(&self.author, &name);
                    let _ = self.member.apply_batch(std::slice::from_ref(&creation));
                    self.stages.push(StageRecord { name: name.clone(), creation });
                    if self.member.get_collection(DB, &name).is_ok() {
                        self.took("new stage");
                    }
                }
                (format!("new stage {name}"), false)
            }
            2..=4 => {
                let Some(k) = self.pick(rng) else { return ("bind (no stage)".into(), false) };
                let wall = self.next_wall();
                let bind = bind_of(&self.author, &self.stages[k].name, wall);
                let before = self.newest();
                let ran = window(&self.member, std::slice::from_ref(&bind))
                    .is_some_and(|outcome| !outcome.stopped());
                if self.newest() != before {
                    self.took("bind");
                }
                (format!("bind {} at {wall}", self.stages[k].name), ran)
            }
            5 => {
                // a logical drop is an event only for a name that was swapped: it needs a stage of the name
                if self.stages.is_empty() {
                    return ("logical drop entry (no stage: a plain drop)".into(), false);
                }
                let wall = self.next_wall();
                let drop = drop_of_orders(&self.author, wall);
                let before = self.newest();
                let ran = window(&self.member, std::slice::from_ref(&drop))
                    .is_some_and(|outcome| !outcome.stopped());
                if self.newest() != before {
                    self.took("drop entry");
                }
                (format!("logical drop entry at {wall}"), ran)
            }
            6 => {
                let ran = matches!(self.member.drop_collection(DB, "orders"), Ok(true));
                if ran {
                    self.took("local drop");
                }
                ("local drop".into(), ran)
            }
            7 => {
                let ran = self.member.create_collection(DB, "orders").is_ok();
                if ran {
                    self.took("recreate");
                }
                ("recreate".into(), ran)
            }
            8 => {
                // a stage made alive AFTER its binding may be in effect: its collection removed by hand, its creation applied again
                let Some(k) = self.pick(rng) else {
                    return ("alive after bind (no stage)".into(), false);
                };
                let name = self.stages[k].name.clone();
                let had_events = self.newest().is_some();
                let txn = self.member.begin_write(crate::WriterHolder::Ddl).unwrap();
                let removed = {
                    let mut collections = txn.open_table(crate::tables::COLLECTIONS).unwrap();
                    collections.remove((DB, name.as_str())).unwrap().is_some()
                };
                txn.commit().unwrap();
                let _ = self.member.apply_batch(std::slice::from_ref(&self.stages[k].creation));
                // it took effect when the collection was there, was gone, and is there again, under a name that has events
                if removed && had_events && self.member.get_collection(DB, &name).is_ok() {
                    self.took("alive after bind");
                }
                (format!("alive after bind {name}"), false)
            }
            9 => {
                // an event stored through the door with no pass: the row is stale and must say so. A real name that has an event has a
                // stage row, so this runs only once the name has a stage; the other case is `11`, on its own name
                if self.stages.is_empty() {
                    return ("event stored with no pass (no stage)".into(), false);
                }
                let wall = self.next_wall();
                let (node, epoch) = (self.author.node_id(), self.author.epoch());
                let stamp = Stamp::new(Hlc::new(wall, 0), node, epoch);
                let txn = self.member.begin_write(crate::WriterHolder::Ddl).unwrap();
                let event = RegisterEvent::LogicalDrop;
                let put = crate::register::put(&txn, DB, "orders", stamp, event);
                let stored = matches!(put, Ok(kimmy_core::register_state::Put::Stored));
                txn.commit().unwrap();
                if stored {
                    self.took("stored with no pass");
                }
                (format!("event stored with no pass at {wall}"), false)
            }
            10 => match self.pick(rng) {
                Some(k) => {
                    let name = self.stages[k].name.clone();
                    if self.member.forget_collection_leaving_rows(DB, &name).is_ok() {
                        self.took("forget");
                    }
                    (format!("forget {name}"), false)
                }
                None => ("forget (no stage)".into(), false),
            },
            11 => {
                // The state no member should be in: an event for a name that has no stage row (the door called by hand). It is kept as an
                // explicit operation so that, if the claim that no production path stores one is ever wrong, the answers are still proven
                // right: the row path equals the computation, the fallback is counted on every resolve, and no row is demanded.
                let wall = self.next_wall();
                let (node, epoch) = (self.author.node_id(), self.author.epoch());
                let stamp = Stamp::new(Hlc::new(wall, 0), node, epoch);
                let txn = self.member.begin_write(crate::WriterHolder::Ddl).unwrap();
                let event = RegisterEvent::LogicalDrop;
                let put = crate::register::put(&txn, DB, "ghost", stamp, event);
                let stored = matches!(put, Ok(kimmy_core::register_state::Put::Stored));
                txn.commit().unwrap();
                if stored {
                    self.took("an event with no stage row");
                }
                (format!("an event for a name with no stage row at {wall}"), false)
            }
            _ => match rng.below(3) {
                0 => {
                    let mut bytes = Vec::new();
                    if self.member.backup_to(&mut bytes, WalkScope::Request).is_err() {
                        return ("restore (backup refused)".into(), false);
                    }
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().join("kimmy.redb");
                    if crate::backup::restore(&path, &mut &bytes[..]).is_ok() {
                        self.member = Engine::open(&path).unwrap();
                        self._member_dir = dir;
                        self.took("restore");
                        ("restore".into(), true)
                    } else {
                        ("restore (refused)".into(), false)
                    }
                }
                1 => {
                    let (joiner, dir) = engine();
                    if pull(&joiner, &self.member) {
                        self.member = joiner;
                        self._member_dir = dir;
                        self.took("page-make");
                        ("page-make".into(), true)
                    } else {
                        ("page-make (failed)".into(), false)
                    }
                }
                _ => {
                    let txn = self.member.begin_write(crate::WriterHolder::Ddl).unwrap();
                    let settled = crate::Engine::settle_register_in_txn(&txn, DB, "orders", None);
                    let ok = settled.is_ok();
                    if ok {
                        txn.commit().unwrap();
                        self.took("the pass alone");
                    } else {
                        let _ = txn.abort();
                    }
                    ("the pass alone".into(), ok)
                }
            },
        }
    }

    /// The four assertions, after the operation `op`, for every name the generator touches.
    fn check(&self, n: usize, op: &str, pass_ran: bool) {
        let say = |what: &str| {
            format!("{what}\n  seed {} step {n} ({op})\n  steps: {:#?}", self.seed, self.steps)
        };
        for name in ["orders", "plain", "ghost"] {
            let m = &self.member;
            let (newest, row, computed) = {
                let txn = m.db().begin_read().unwrap();
                (
                    crate::register::newest_stored_event_read(&txn, DB, name).unwrap(),
                    crate::register::visible_row_read(&txn, DB, name).unwrap(),
                    answer(Engine::resolve_computed(&txn, DB, name)),
                )
            };
            let before = m.register_resolve_fallbacks();
            let got = answer(m.resolve(DB, name));
            let fell_back = m.register_resolve_fallbacks() - before;
            let what = format!("{name}: the row path answers what the computation does");
            assert_eq!(got, computed, "{}", say(&what));
            if newest.is_none() {
                let what = format!("{name}: a name with no event has a row");
                assert!(row.is_none(), "{}", say(&what));
            }
            let current = |row: &VisibleRow| row.newest_event == newest;
            let alive = |row: &VisibleRow| match row.kind {
                VisibleKind::Dropped => true,
                // an identity row serves the identity, or says the name is absent: it does not depend on the identity being alive
                VisibleKind::Identity => true,
                VisibleKind::Bound | VisibleKind::Created => {
                    m.get_collection(DB, &row.physical).is_ok()
                }
            };
            let is_current = row.as_ref().is_some_and(current);
            let usable = newest.is_none() || (is_current && row.as_ref().is_some_and(&alive));
            let what = format!("{name}: a fallback exactly when the row is missing or stale");
            assert_eq!(fell_back, u64::from(!usable), "{}", say(&what));
            // only a name that has a stage row can be named by a page or a restore, so only then does a pass owe it a row
            if pass_ran && name == "orders" && newest.is_some() && self.has_a_stage_row() {
                assert!(
                    usable,
                    "{}",
                    say("a pass site left the row of a name that has a stage row missing or stale")
                );
            }
            let Some(row) = row.as_ref().filter(|_| is_current && newest.is_some()) else {
                continue;
            };
            if !alive(row) {
                // a current row whose physical is gone is the reader's fallback, which the computation answers
                continue;
            }
            match (&row.kind, &computed) {
                (VisibleKind::Dropped, Err(_)) => {}
                (VisibleKind::Identity, Ok((physical, _, None))) => {
                    assert_eq!(physical, name, "{}", say("an identity row names the identity"));
                }
                (VisibleKind::Identity, Err(_)) => {}
                (VisibleKind::Bound | VisibleKind::Created, Ok((physical, _, visible))) => {
                    assert_eq!(
                        (physical.as_str(), *visible),
                        (row.physical.as_str(), row.event),
                        "{}",
                        say("a current row names what the computation names")
                    );
                }
                (kind, answered) => {
                    let what = format!("a current row ({kind:?}) disagrees with {answered:?}");
                    panic!("{}", say(&what));
                }
            }
        }
    }
}

#[test]
fn the_row_is_the_pure_function_after_every_operation_of_every_sequence() {
    let sequences = std::env::var("RESOLVE_ROW_SEQUENCES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(SEQUENCES);
    let mut touched_a_row = 0usize;
    let mut effective: std::collections::BTreeMap<&'static str, usize> = Default::default();
    for index in 0..sequences {
        let seed = 0x5EED_0000 + index;
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let mut world = World::new(seed);
        for n in 0..STEPS {
            let (op, pass_ran) = world.step(&mut rng);
            world.steps.push(op.clone());
            world.check(n, &op, pass_ran);
            let txn = world.member.db().begin_read().unwrap();
            if crate::register::visible_row_read(&txn, DB, "orders").unwrap().is_some() {
                touched_a_row += 1;
            }
        }
        for (kind, n) in &world.effective {
            *effective.entry(*kind).or_default() += n;
        }
    }
    assert!(
        touched_a_row > sequences as usize,
        "premise: the generator reaches states that have a row, or the property pins nothing ({touched_a_row})"
    );
    // every transition must have really happened: an operation that is always refused leaves the property green and the transition untested
    for kind in KINDS {
        let n = effective.get(kind).copied().unwrap_or(0);
        assert!(n > 0, "premise: the {kind} step took effect {n} times in {sequences} sequences");
    }
}

#[test]
fn a_name_refreshed_many_times_resolves_without_reading_its_events() {
    const SWAPS: usize = 200;
    let (author, _da) = engine();
    let (member, _dm) = engine();
    let mut wall = physical_now_ms() + 5_000;
    for k in 0..SWAPS {
        let name = format!("orders.__stg.s{k}");
        let deadline = Hlc::new(physical_now_ms() + 600_000, 0);
        author.create_stage(DB, &name, MergePolicy::Document, deadline).unwrap();
        member.apply_batch(std::slice::from_ref(&creation_of(&author, &name))).unwrap();
        wall += 1;
        let bind = bind_of(&author, &name, wall);
        let outcome = window(&member, std::slice::from_ref(&bind)).expect("the window applies");
        assert!(!outcome.stopped(), "premise: the binding of swap {k} is taken");
    }
    let last = format!("orders.__stg.s{}", SWAPS - 1);
    let before = crate::register::rows_read_for_tests();
    let served = member.resolve(DB, "orders").map(|resolved| resolved.physical);
    assert_eq!(served.map_err(|e| e.to_string()), Ok(last), "premise: the last swap serves");
    assert_eq!(
        crate::register::rows_read_for_tests(),
        before,
        "a name with {SWAPS} swaps resolves reading none of them"
    );
    assert_eq!(member.register_resolve_fallbacks(), 0, "and it did not fall back");
    {
        let txn = member.db().begin_read().unwrap();
        crate::register::timeline_read(&txn, DB, "orders").unwrap();
    }
    assert!(
        crate::register::rows_read_for_tests() >= before + SWAPS,
        "premise: the full computation reads every swap's events, so the 0 above is a measurement"
    );
}
