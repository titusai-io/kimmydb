//! The rows of unit P that waited on unit N's API, written against N's tip, `7b6d2ba4` (origin/q6-0620; N is pushed). Written by the tester at the manager's order, Thu Nov 26 2026.
//!
//! SUPERSEDES the four `#[cfg(any())]` blocks that waited on N:
//!   - `needs_the_names_table_tie` (pending-unitP-plain-life-tests.rs): the tie row                  -> `tie_*` below
//!   - `needs_the_options_row` (same file): rows 3, 4 and 5                                          -> `row3_*`, `row4_*`, `row5_*`
//!   - `needs_the_names_table` (pending-pendingwrite-rows-tests.rs): w1c and w3                      -> `w1c_*`, `w3_*`
//!
//! The file tests the options rows and the names rows that N keeps: the tie rule for two creations at one Hlc (`tie_*`), a plain remake dated above the floor (`row3_*`),
//! the life of the rows (`row4_*`, `w3_*`), the answer of the fetch (`w1c_*`) and a creation redelivered after its row committed (`row5_*`).
//!
//! BASE: it does NOT compile at 6e54fe17 (where the pending P files sit): it calls `register::{BaseOptions, Carried, carried_rows}`, `CARRY_OPTIONS` and the options and names tables, which N added. It compiles only after the rebase onto the new tip.
//! STATUS: NOT COMPILED, NEVER RUN, NOT QUEUED. Every API call below was read against `git show 7b6d2ba4:...` (name AND signature: the table at the foot of this header). Nothing here is a claim about what the product does:
//! the expected statuses are the tester's READS of the code at 7b6d2ba4 and must be read against the first run.
//!
//! WHAT N GIVES (read): `register::BaseOptions` (pub(crate): db, name, at, merge_policy, join_retention_ms, loss_limits, stage; `of_creation`, `of_meta`), `register::note_base_options(&WriteTransaction, &BaseOptions) -> Result<bool>` (rise-only: the later creation by
//! `(at, policy text)`, a tie to the greater JSON encoding of the whole row), `register::carried_rows(&ReadTransaction, Carried::{Names, Options}) -> Result<Vec<(Vec<u8>, Vec<u8>)>>` (rows as stored), the tables `REGISTER_NAMES` and `COLLECTION_BASE_OPTIONS`,
//! `Engine::note_creation_seen` (sync.rs:2646: the creation is SEEN, in its own committed transaction, BEFORE the history judgement and the adoption), `SnapshotPage::{carried, dropped, dropped_database}`.
//! WHAT N DOES NOT GIVE: a collector of either table (gc.rs never touches them), a names row for a plain collection (only stages, recreations and register events have one), any hook between the row's commit and the adoption.
//!
//! FOR THE FIRST COMPILE WHEN P STARTS (the manager, Nov 26; two unverified points): (1) the tie row's stage variant: whether the stage deadlines (now+60 s, now+90 s) are inside the clamp from the creation's stamp; (2) w3: whether the register keeps a stage's
//! names row longer than the ruled collection rule says.
//!
//! EXPECTED STATUS AT 7b6d2ba4 (reads, not runs):
//!   tie_* (R, K, stage)                       GUARD, green: N's `options_rise` already orders by the greater encoding and the entries route feeds it (`note_creation_seen`). Red only if P breaks it.
//!   row3_*                                    RED, on P's remake alone (the collection stays gone after the touch). Every premise is asserted first.
//!   row4_*                                    RED, on the last step (the row is never collected); the keep steps are green.
//!   w3_* (a stage's names and options rows)   RED, on the last step likewise.
//!   w1c_*                                     GUARD, green: N's tests :1026 and :1117 pin the same on the scoped page. This one pins it on the page the fetch answers with (transport.rs:1503 answers `engine.snapshot_page(after, collection)`).
//!   row5_* (the state half)                   GUARD, probably green: the entries route adopts by comparison (sync.rs:2722-2730) whatever the row holds.
//!   `needs_a_race_point_*` (the crash half)   cfg(any()): needs a Race point between sync.rs:2646 and :2730 (the coder's call when P is built).
//!
//! CALLS READ AT 7b6d2ba4 (name, signature):
//!   Engine::apply_batch(&self, &[OplogEntry]) -> Result<SyncOutcome> (sync.rs:1169)        Engine::create_collection(&self, db, name) -> Result<CollectionMeta>
//!   Engine::create_stage(&self, db, physical, MergePolicy, deadline: Hlc) -> Result<CollectionMeta> (engine.rs:5526)
//!   Engine::drop_collection(&self, db, name) -> Result<bool> (6908)                         Engine::drop_database(&self, name) -> Result<bool> (5283)
//!   Engine::collection_dropped_at(&self, CollectionId) -> Result<Option<Stamp>>            Engine::database_dropped_at(&self, db) -> Result<Option<Stamp>> (database_tombstone.rs:100)
//!   Engine::collect_garbage_at(&self, now_ms: u64, RetentionPolicy) -> Result<GcOutcome> (gc.rs:108; a tombstone goes when `stamp.hlc < Hlc(now_ms - tombstone_secs*1000)`, and only with no rows under it)
//!   Engine::finish_purges_now(&self) -> Result<usize> (purge.rs, test-hooks)                Engine::snapshot_page(&self, Option<cursor>, Option<CollectionId>) -> Result<SnapshotPage>
//!   Engine::begin_write(WriterHolder) -> Result<WriteTxn> (pub(crate))                      Engine::db(&self) -> &redb::Database (pub(crate))
//!   crate::engine::ddl_entry(Stamp, OpKind, CollectionId, &T: Serialize) -> Result<OplogEntry> (engine.rs:7612, pub(crate))
//!   kimmy_core::{CollectionCreate{db,name,merge_policy,validator,join_retention_ms,loss_limits,stage}, CollectionRef::new(db, name), LossLimits{cap,cut_bytes}, StageInfo{deadline}, database_drop_id(db), DATABASE_DROP_NAME}
//!   gc::{RetentionPolicy::new(oplog_secs, tombstone_secs), GcOutcome} re-exported at the crate root (lib.rs:346).

use bson::doc;
use kimmy_core::{
    CollectionCreate, CollectionId, CollectionRef, Hlc, LossLimits, MergePolicy, OpKind,
    OplogEntry, StageInfo, Stamp, VersionVector,
};
use redb::ReadableDatabase;

use crate::register::{BaseOptions, Carried};
use crate::{
    Engine, PeerWindow, RetentionPolicy, SyncOutcome, WalkScope, WriterHolder, physical_now_ms,
};

const DB: &str = "app";
/// A stage's physical name (`kimmy_core::is_stage_name`: it carries `.__stg.`).
const STAGE: &str = "orders.__stg.n1";

// ---- fixtures: copied from the neighbouring tests (`names_and_options_tests`, `plain_life_tests`, `pending_write_rows_tests`) -------------------------------------------------

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn window(b: &Engine, entries: &[OplogEntry]) -> SyncOutcome {
    let mut theirs = VersionVector::default();
    for entry in entries {
        theirs.observe(entry.stamp);
    }
    let mine = b.witnessed_vector().unwrap();
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
    .unwrap();
    outcome
}

fn id_of(name: &str) -> CollectionId {
    CollectionId::derive(DB, name)
}

fn id_key(name: &str) -> Vec<u8> {
    id_of(name).0.to_be_bytes().to_vec()
}

/// The rows of a carried table as the engine stores them (`names_and_options_tests::rows`).
fn rows(e: &Engine, which: Carried) -> Vec<(Vec<u8>, Vec<u8>)> {
    let txn = e.db().begin_read().unwrap();
    crate::register::carried_rows(&txn, which).unwrap()
}

/// The options row of `name` as stored (bytes), if the table holds one.
fn options_bytes(e: &Engine, name: &str) -> Option<Vec<u8>> {
    rows(e, Carried::Options).into_iter().find(|(k, _)| *k == id_key(name)).map(|(_, v)| v)
}

fn options_of(e: &Engine, name: &str) -> Option<BaseOptions> {
    options_bytes(e, name).map(|v| serde_json::from_slice(&v).expect("a row that decodes"))
}

fn has_names_row(e: &Engine, name: &str) -> bool {
    rows(e, Carried::Names).iter().any(|(k, _)| *k == id_key(name))
}

/// The entry of a creation of `name`, as `by` would have minted it at `wall_ms` (`names_and_options_tests::creation`, with the body given).
fn creation_entry(by: &Engine, wall_ms: u64, body: &CollectionCreate) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        kind: OpKind::CreateCollection,
        collection: id_of(&body.name),
        doc_id: None,
        body: Some(bson::serialize_to_vec(body).expect("the creation encodes")),
        meta: None,
    }
}

/// A `field` creation of `orders` with the join retention `r` and the loss limits `k`.
fn field_body(r: u64, k: LossLimits) -> CollectionCreate {
    CollectionCreate {
        db: DB.into(),
        name: "orders".into(),
        merge_policy: MergePolicy::Field,
        validator: None,
        join_retention_ms: Some(r),
        loss_limits: Some(k),
        stage: None,
    }
}

/// A drop of `orders` by `by`, stamped `wall_ms` (`plain_life_tests::drop_of`, `names_and_options_tests::drop_entry`).
fn drop_of(by: &Engine, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        kind: OpKind::DropCollection,
        collection: id_of("orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionRef::new(DB, "orders")).expect("the drop encodes"),
        ),
        meta: None,
    }
}

/// The drop of the whole database by `by`, stamped `wall_ms` (`swap_routes_tests::drop_database_entry`).
fn drop_database_entry(by: &Engine, wall_ms: u64) -> OplogEntry {
    crate::engine::ddl_entry(
        Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        OpKind::DropCollection,
        kimmy_core::database_drop_id(DB),
        &CollectionRef::new(DB, kimmy_core::DATABASE_DROP_NAME),
    )
    .unwrap()
}

/// An entry of `x`'s oplog re-stamped by hand (`plain_life_tests::stamped`).
fn stamped(mut entry: OplogEntry, wall_ms: u64, by: &Engine) -> OplogEntry {
    entry.stamp = Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch());
    entry
}

/// A write of `x` into its own plain life of `orders`, re-stamped to `wall_ms`: the touch above both floors.
fn touch_at(wall_ms: u64) -> (OplogEntry, Engine, tempfile::TempDir) {
    let (x, dx) = engine();
    let xo = x.create_collection(DB, "orders").expect("x's life");
    x.insert(&xo, doc! { "_id": 7, "v": "touch" }).expect("x writes");
    let insert = everything(&x)
        .into_iter()
        .rev()
        .find(|e| e.kind == OpKind::Insert && e.collection == id_of("orders"))
        .expect("x's insert");
    (stamped(insert, wall_ms, &x), x, dx)
}

// ---- the tie row, by the entries route --------------------------------------------------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Differs {
    /// the join retention `R`
    R,
    /// the loss limits `K`
    K,
    /// the stage's deadline (a stage creation: the honest body that carries a stage)
    Stage,
}

/// Source: the manager's tie rule (Nov 26 12:22 AM and 12:46 AM: "the greater canonical encoding of the WHOLE options row wins ... the encoding is JSON; deliberate") and N's `options_rise`. Two creations of one name with the SAME Hlc and the same policy text
/// and a different R, K or stage, by two members (two real members cannot stamp one Hlc, so both entries are minted at one wall time by two engines). The row is the same bytes on every member after both arrivals in EITHER order, and it is the greater
/// of the two encodings of `BaseOptions::of_creation`: the encoding the route itself writes (JSON, so numbers compare as TEXT: nobody may make it numeric).
/// The stage variant uses a stage's name and two deadlines (the honest body that carries `stage`); a plain name with a stage body is not refused on the entries route either (`reserved_name_without_its_marks` refuses only the other direction), but no honest member
/// sends one, so it is not built.
/// EXPECTED: GUARD, green at 7b6d2ba4 (the entries route calls `note_creation_seen` before it judges anything). Goes red if P's change to the standing life's refusal ("two Rs", sync.rs) touches the row, or a rewrite of the options row's write
/// compares by arrival.
/// Mutants: arrival order wins (the first held); the smaller encoding wins; only R compared (K and stage ignored); the numeric comparison of R; the encoding of the row without the stage.
/// UNVERIFIED by a run: the stage creation's deadlines are clamped from the creation's stamp (swap.rs `StageMeta`): `now + 60 s` and `now + 90 s` are assumed inside the clamp; the row is written before that judgement, so a clamp would show as an error line, not as a wrong row.
#[test]
fn tie_two_creations_at_one_hlc_with_another_r_k_or_stage_leave_the_greater_encoding_of_the_whole_row_in_either_order()
 {
    let wall = physical_now_ms().saturating_sub(5_000);
    let (a, _da) = engine();
    let (b, _db) = engine();
    let now = physical_now_ms();
    let stage_body = |deadline_ms: u64| CollectionCreate {
        db: DB.into(),
        name: STAGE.into(),
        merge_policy: MergePolicy::Document,
        validator: None,
        join_retention_ms: None,
        loss_limits: None,
        stage: Some(StageInfo { deadline: Hlc::new(deadline_ms, 0) }),
    };
    let k4 = LossLimits { cap: 4, cut_bytes: 64 };
    let k8 = LossLimits { cap: 8, cut_bytes: 128 };
    let variants: Vec<(Differs, CollectionCreate, CollectionCreate)> = vec![
        (Differs::R, field_body(3_600_000, k4), field_body(172_800_000, k4)),
        (Differs::K, field_body(3_600_000, k4), field_body(3_600_000, k8)),
        (Differs::Stage, stage_body(now + 60_000), stage_body(now + 90_000)),
    ];
    let mut red = Vec::new();
    for (variant, body_a, body_b) in &variants {
        let (first, second) = (creation_entry(&a, wall, body_a), creation_entry(&b, wall, body_b));
        assert_eq!(
            first.stamp.hlc, second.stamp.hlc,
            "{variant:?} premise: equal policy_key (one Hlc, one policy text)"
        );
        assert_ne!(first.stamp, second.stamp, "{variant:?} premise: two creations, by two members");
        let name = body_a.name.as_str();
        let encode = |body: &CollectionCreate| {
            serde_json::to_vec(&BaseOptions::of_creation(
                DB,
                &body.name,
                Hlc::new(wall, 0),
                body.merge_policy,
                body.join_retention_ms,
                body.loss_limits,
                body.stage,
            ))
            .expect("the row encodes")
        };
        let (ea, eb) = (encode(body_a), encode(body_b));
        assert_ne!(ea, eb, "{variant:?} premise: the two rows differ");
        let greater = std::cmp::max(ea.clone(), eb.clone());

        let mut ends = Vec::new();
        for order in [[&first, &second], [&second, &first]] {
            let (z, _dz) = engine();
            for entry in order {
                if let Err(e) = z.apply_batch(std::slice::from_ref(entry)) {
                    red.push(format!("{variant:?}: the creation did not apply: {e}"));
                }
            }
            ends.push(options_bytes(&z, name));
        }
        if ends[0] != ends[1] {
            red.push(format!("{variant:?}: the two orders end with different rows"));
        }
        for (i, end) in ends.iter().enumerate() {
            match end {
                None => red.push(format!("{variant:?}: order {i}: no options row")),
                Some(bytes) if *bytes != greater => red.push(format!(
                    "{variant:?}: order {i}: the row is not the greater encoding of the whole row"
                )),
                Some(_) => {}
            }
        }
    }
    assert!(red.is_empty(), "{}", red.join("\n"));
}

// ---- ROW 3: a plain remake whose row's `at` is below both tombstones ---------------------------------------------------------------------------------------------------------

/// ROW 3. Source: build-order addendum 12:56 AM, "RULED (manager), binding on P: the remake is justified by the fact above the floor, never by the row's creation. It takes ONLY the options, the database and the name from the row. The life it makes is dated the floor (the drop's
/// successor, as a re-dated life is), and the row's `at` goes only into the policy stamp." Test owed: "a plain remake whose row's `at` is below both its own and its database's tombstone succeeds".
/// Steps (t0 = the life's creation, the row's `at`): `orders` is created locally (its options row has `at` = t0); a drop of it at t0+20 and a drop of the database at t0+25 are taken (both tombstones above the row's `at`); a write at t0+40, above both floors, arrives (the touch).
/// EXPECTED RED TODAY: the remake does not exist, so the touch is history and the collection stays gone (`get_collection` fails at "the remake stands"). Every premise before it is asserted, so a red at a premise is a different red.
/// WANTED after P: the collection stands; its `created` is above the later floor (t0+25); its `merge_policy_at` is the row's `at`; the touch is applied.
/// Mutants: the remake refused because the row's `at` is below the tombstone ("history"); the life dated at the row's `at` (below the floor); `at` used as the life's creation.
#[test]
fn row3_a_plain_remake_whose_rows_at_is_below_both_tombstones_succeeds_dated_above_the_floor() {
    let (m, _dm) = engine();
    let first = m.create_collection(DB, "orders").expect("the life");
    let t0 = first.created.wall_ms;
    let row_at = options_of(&m, "orders").expect("premise: the creation wrote its options row").at;

    let (touch, x, _dx) = touch_at(t0 + 40);
    let (own_drop, database_drop) = (drop_of(&x, t0 + 20), drop_database_entry(&x, t0 + 25));
    window(&m, std::slice::from_ref(&own_drop));
    window(&m, std::slice::from_ref(&database_drop));
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the drops buried the life");
    assert_eq!(
        m.collection_dropped_at(id_of("orders")).unwrap(),
        Some(own_drop.stamp),
        "premise: the collection's own tombstone"
    );
    assert_eq!(
        m.database_dropped_at(DB).unwrap(),
        Some(database_drop.stamp),
        "premise: the database's tombstone"
    );
    assert!(
        row_at < own_drop.stamp.hlc && row_at < database_drop.stamp.hlc,
        "premise: the row's `at` is below both tombstones"
    );
    assert!(touch.stamp.hlc > database_drop.stamp.hlc, "premise: the touch is above both floors");

    window(&m, std::slice::from_ref(&touch));

    let c =
        m.get_collection(DB, "orders").expect("the remake stands (RED TODAY: there is no remake)");
    assert!(c.created > database_drop.stamp.hlc, "the life is dated above the later floor");
    assert_eq!(
        c.merge_policy_at,
        Some(row_at),
        "and the row's `at` went only into the policy stamp"
    );
    assert_eq!(m.count(&c, WalkScope::Background).unwrap(), 1, "the touch is applied");
}

// ---- ROW 4: the rule that keeps a row ---------------------------------------------------------------------------------------------------------------------------------------

/// ROW 4. Source: the same addendum, "Collecting rows (w3): a row is kept while its own id's tombstone or its database's tombstone EXISTS. It is never decided by comparing `at` with a cutoff or a stamp." Test owed: "a row whose `at` is older than the tombstone retention is kept while its
/// tombstone is." Steps: `orders` is created (its options row has `at` = t0); a drop of it at t0+10 s and a drop of the database at t0+20 s are taken, and the purges are finished (a tombstone with rows under it is never collected). Three passes of `collect_garbage_at` (tombstone
/// retention 1 s; the oplog's is a year): the first with a cutoff BELOW both tombstones but ABOVE the row's `at` (the row is older than the cutoff and both tombstones exist: KEPT); the second with a cutoff between the two (the collection's own tombstone is collected, the database's stays:
/// KEPT); the third above both (both gone: COLLECTED). The tombstone stamps are hand-built ten seconds apart because the retention is in whole seconds.
/// EXPECTED RED TODAY: nothing collects the table, so the first two steps are green and the last fails ("collected once both are gone"). Premises on the tombstones are asserted, so a red there is the pass not collecting what it should, not the row.
/// Mutants: collected by comparing `at` with the cutoff (the first step goes red); kept for ever (the last step goes red); kept while only its own tombstone exists (the second step goes red).
/// Reads: `tombstone < cutoff` collects (gc.rs:`scan_dropped_collections`), `cutoff = now_ms - tombstone_secs * 1000` (gc.rs:`cutoff`); `finish_purges_now` first because a tombstone with rows under it is kept.
#[test]
fn row4_a_row_older_than_the_retention_is_kept_while_either_tombstone_exists_and_collected_when_both_are_gone()
 {
    let (m, _dm) = engine();
    let life = m.create_collection(DB, "orders").expect("the life");
    let t0 = life.created.wall_ms;
    assert!(options_of(&m, "orders").is_some(), "premise: the creation wrote its options row");
    let (x, _dx) = engine();
    let (own_drop, database_drop) =
        (drop_of(&x, t0 + 10_000), drop_database_entry(&x, t0 + 20_000));
    window(&m, std::slice::from_ref(&own_drop));
    window(&m, std::slice::from_ref(&database_drop));
    m.finish_purges_now().expect("the purges finish");
    assert_eq!(
        m.collection_dropped_at(id_of("orders")).unwrap(),
        Some(own_drop.stamp),
        "premise: the collection's own tombstone"
    );
    assert_eq!(
        m.database_dropped_at(DB).unwrap(),
        Some(database_drop.stamp),
        "premise: the database's tombstone"
    );

    let policy = RetentionPolicy::new(31_536_000, 1);
    let pass = |now_ms: u64| {
        m.collect_garbage_at(now_ms, policy).expect("the retention pass runs");
    };

    // 1: a cutoff above the row's `at` and below both tombstones
    pass(own_drop.stamp.hlc.wall_ms + 1_000 - 500);
    assert!(
        Hlc::new(t0, 0) < Hlc::new(own_drop.stamp.hlc.wall_ms - 500, 0),
        "premise: the row's `at` is older than the cutoff"
    );
    assert!(
        m.collection_dropped_at(id_of("orders")).unwrap().is_some(),
        "premise: the pass collected neither tombstone"
    );
    assert!(
        options_of(&m, "orders").is_some(),
        "kept while both tombstones exist, though its `at` is older than the cutoff"
    );

    // 2: a cutoff between the two tombstones
    pass(own_drop.stamp.hlc.wall_ms + 1_000 + 5_000);
    assert_eq!(
        m.collection_dropped_at(id_of("orders")).unwrap(),
        None,
        "premise: the collection's own tombstone is collected"
    );
    assert!(
        m.database_dropped_at(DB).unwrap().is_some(),
        "premise: the database's tombstone is kept"
    );
    assert!(options_of(&m, "orders").is_some(), "kept while the database's tombstone exists");

    // 3: a cutoff above both
    pass(database_drop.stamp.hlc.wall_ms + 1_000 + 5_000);
    assert_eq!(
        m.database_dropped_at(DB).unwrap(),
        None,
        "premise: the database's tombstone is collected"
    );
    assert!(
        options_of(&m, "orders").is_none(),
        "collected once both tombstones are gone (RED TODAY: nothing collects the table)"
    );
}

// ---- w3: the rows of a stage ------------------------------------------------------------------------------------------------------------------------------------------------

/// w3, rebound by the manager, and REBOUND AGAIN at P12 (ruled 6:20 PM, hardproblems: "NAMES rows are never collected in P"): a STAGE has a names row, and a names row outlives every
/// event and stage row of its name (the restore refuses a store that holds a stage row with no names row), and the options row of an id that has a names row is kept with it (the register's
/// own rows refer to the pair). So after the database's tombstone and the stage's own are both collected, BOTH rows of the stage are KEPT, and the member still passes the restore check
/// (a backup of it restores). The collected case is row 4's (a plain collection: no names row). It was a "collected when none covers it" test before P12; the flip is a fix-commit flip, as
/// with Q8.
/// Mutants (P12): the options row of an id with a names row collected (the options assert goes red); the names row collected (the names assert goes red).
#[test]
fn w3_a_stages_names_and_options_rows_are_kept_while_a_tombstone_covers_it_and_collected_when_none_does()
 {
    let (m, _dm) = engine();
    let now = physical_now_ms();
    let stage = m
        .create_stage(DB, STAGE, MergePolicy::Document, Hlc::new(now + 60_000, 0))
        .expect("the stage is created");
    assert!(has_names_row(&m, STAGE), "premise: a stage has a names row");
    assert!(options_of(&m, STAGE).is_some(), "premise: a stage has an options row");
    let (x, _dx) = engine();
    let drop = drop_database_entry(&x, stage.created.wall_ms + 10_000);
    window(&m, std::slice::from_ref(&drop));
    m.finish_purges_now().expect("the purges finish");
    assert_eq!(
        m.database_dropped_at(DB).unwrap(),
        Some(drop.stamp),
        "premise: the database's tombstone"
    );
    assert!(m.get_collection(DB, STAGE).is_err(), "premise: the drop buried the stage");

    let policy = RetentionPolicy::new(31_536_000, 1);
    m.collect_garbage_at(drop.stamp.hlc.wall_ms + 1_000 - 500, policy)
        .expect("the first pass runs");
    assert!(
        m.database_dropped_at(DB).unwrap().is_some(),
        "premise: the first pass collected nothing"
    );
    assert!(has_names_row(&m, STAGE), "the names row is kept while the tombstone stands");
    assert!(options_of(&m, STAGE).is_some(), "and the options row");

    m.collect_garbage_at(drop.stamp.hlc.wall_ms + 1_000 + 5_000, policy)
        .expect("the second pass runs");
    assert_eq!(
        m.database_dropped_at(DB).unwrap(),
        None,
        "premise: the database's tombstone is collected"
    );
    assert_eq!(
        m.collection_dropped_at(id_of(STAGE)).unwrap(),
        None,
        "premise: the stage's own tombstone is collected"
    );
    assert!(
        has_names_row(&m, STAGE),
        "the names row is KEPT: a names row outlives its stage row, and nothing collects one"
    );
    assert!(options_of(&m, STAGE).is_some(), "and the options row of an id that has a names row");
    // the consequence: the member passes the restore check (a stage row with no names row would be refused)
    let mut bytes = Vec::new();
    m.backup_to(&mut bytes, WalkScope::Request).expect("the backup is written");
    let dir = tempfile::tempdir().expect("a temporary directory");
    crate::backup::restore(&dir.path().join("kimmy.redb"), &mut &bytes[..])
        .expect("the backup restores");
}

// ---- P12: an options row is kept while its collection stands and while a tombstone of its own covers it ----------------------------------------------------------------------

/// UNIT P, P12: an options row is collected only when nothing holds it, and a collection that STANDS holds it, and the collection's OWN tombstone holds it (row 4 shows the database's, and the
/// two tombstones' separation). A plain collection stands through a pass far in the future of every stamp (its row is kept: the restore requires an options row of every standing collection),
/// is then dropped by a drop of its own (no database drop), and a pass whose cutoff is below that tombstone keeps the row (the tombstone is held), and a pass above it collects it. Mutants (P12): collected
/// while the collection stands; collected while only its own tombstone stands.
#[test]
fn an_options_row_is_kept_while_its_collection_stands_and_while_its_own_tombstone_stands() {
    let (m, _dm) = engine();
    let life = m.create_collection(DB, "orders").expect("the life");
    let t0 = life.created.wall_ms;
    let policy = RetentionPolicy::new(31_536_000, 1);
    m.collect_garbage_at(t0 + 10 * 365 * 24 * 3600 * 1000, policy).expect("the pass runs");
    assert!(options_of(&m, "orders").is_some(), "kept while the collection stands");

    let (x, _dx) = engine();
    let own_drop = drop_of(&x, t0 + 10_000);
    window(&m, std::slice::from_ref(&own_drop));
    m.finish_purges_now().expect("the purges finish");
    assert_eq!(
        m.collection_dropped_at(id_of("orders")).unwrap(),
        Some(own_drop.stamp),
        "premise: the collection's own tombstone, and no database drop"
    );
    assert!(m.database_dropped_at(DB).unwrap().is_none(), "premise: no tombstone of the database");
    m.collect_garbage_at(own_drop.stamp.hlc.wall_ms + 1_000 - 500, policy).expect("the pass runs");
    assert!(
        m.collection_dropped_at(id_of("orders")).unwrap().is_some(),
        "premise: the pass collected nothing"
    );
    assert!(options_of(&m, "orders").is_some(), "kept while its own tombstone stands");
    m.collect_garbage_at(own_drop.stamp.hlc.wall_ms + 1_000 + 5_000, policy)
        .expect("the pass runs");
    assert_eq!(m.collection_dropped_at(id_of("orders")).unwrap(), None, "premise: collected");
    assert!(options_of(&m, "orders").is_none(), "collected with the last tombstone");
}

// ---- P11: a names row outlives every event and stage row of its name ------------------------------------------------------------------------------------------------------

/// UNIT P, P11 (tests first; the 6:08 AM BINDING note on w3): "a names row must outlive every event and stage row of its name, and an options row every standing collection, or the
/// restore's clauses 1 and 2 refuse a valid backup". A member holds a plain `orders` and takes a stage of it and its binding: the register now holds a stage row and events of the name, the identity
/// is buried at the binding's stamp, and the stage stands. A retention pass far in the future of every stamp collects the identity's tombstone (nothing covers it any more). The names rows of the
/// name and of the stage are KEPT (a stage row and events refer to them), the stage's options row is kept (it stands), and a backup of the member restores: it is refused ("a stage row with no names
/// row", "a register event of a name with no names row") if the collector took the names row. GREEN today (nothing collects the table); red if P12's collector removes a names row a stage row or an event still names.
/// Mutants (P12): a names row collected when no tombstone covers its id; an options row collected while its collection stands.
#[test]
fn a_names_row_outlives_the_stage_row_and_the_events_of_its_name_and_the_member_still_restores() {
    let now = physical_now_ms();
    let (a, _da) = engine();
    a.create_stage(DB, STAGE, MergePolicy::Document, Hlc::new(now + 120_000, 0))
        .expect("the stage");
    let creation = everything(&a)
        .into_iter()
        .find(|e| e.kind == OpKind::CreateCollection && e.collection == id_of(STAGE))
        .expect("the stage's creation");
    let bind = OplogEntry {
        stamp: Stamp::new(Hlc::new(now + 20_000, 0), a.node_id(), a.epoch()),
        kind: OpKind::BindCollection,
        collection: id_of("orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&kimmy_core::CollectionBind {
                db: DB.into(),
                name: "orders".into(),
                physical: STAGE.into(),
            })
            .expect("the binding encodes"),
        ),
        meta: None,
    };
    let (m, _dm) = engine();
    m.create_collection(DB, "orders").expect("the member's own identity");
    window(&m, std::slice::from_ref(&creation));
    window(&m, std::slice::from_ref(&bind));
    assert_eq!(
        m.resolve(DB, "orders").expect("the name resolves").physical,
        STAGE,
        "premise: bound"
    );
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the identity is buried");
    m.finish_purges_now().expect("the purges finish");
    assert!(has_names_row(&m, "orders"), "premise: the name has a names row");
    assert!(has_names_row(&m, STAGE), "premise: the stage has a names row");

    let far = now + 10 * 365 * 24 * 3600 * 1000;
    m.collect_garbage_at(far, RetentionPolicy::new(31_536_000, 1)).expect("the pass runs");
    assert!(has_names_row(&m, "orders"), "the identity's names row outlives its events");
    assert!(has_names_row(&m, STAGE), "and the stage's names row outlives the stage's row");
    assert!(options_of(&m, STAGE).is_some(), "and the options row of a stage that stands");

    // the consequence: a backup of the member restores (its register and its names rows agree)
    let mut bytes = Vec::new();
    m.backup_to(&mut bytes, WalkScope::Request).expect("the backup is written");
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("kimmy.redb");
    crate::backup::restore(&path, &mut &bytes[..]).expect("the backup restores");
}

// ---- w1c: the fetch's answer ------------------------------------------------------------------------------------------------------------------------------------------------

/// A sender that holds `orders` with two writes and another collection of the database (so the database's drop has something to reach).
fn sender() -> (Engine, tempfile::TempDir) {
    let (a, d) = engine();
    let orders = a.create_collection(DB, "orders").expect("the collection");
    a.insert(&orders, doc! { "_id": 1, "v": "first" }).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    a.insert(&orders, doc! { "_id": 2, "v": "second" }).unwrap();
    a.create_collection(DB, "keep").expect("the database's other collection");
    (a, d)
}

/// w1c, the fetch's answer carries the collection's options row and BOTH floors, from a sender that holds the collection standing and from one that holds it gone. The kimmy-cluster fetch answers with `engine.snapshot_page(after, collection)` (transport.rs:1503), the scoped page.
/// Adapted from the cfg(any()) block (its `page.names` and `page.floors` do not exist): the page carries `carried` rows (table, key, value as the sender stores them: a plain collection has an OPTIONS row and no names row), `dropped` (the collection's own tombstone) and
/// `dropped_database` (the database's, when the sender holds it).
/// EXPECTED: GUARD, green at 7b6d2ba4 (N's `a_scoped_page_carries_the_options_row...` :1026 and `a_scoped_page_of_a_collection_gone_before_a_database_drop_names_both_drops` :1117 pin the same). It goes red if P changes what a scoped answer carries.
/// Mutants: the answer without the options row; with only the collection's own tombstone; the database's floor named for a collection that stands.
#[test]
fn w1c_the_fetch_answer_carries_the_options_row_and_both_floors() {
    let id = id_of("orders");
    for gone in [false, true] {
        let (a, _da) = sender();
        let (own, database) = if gone {
            assert!(a.drop_collection(DB, "orders").unwrap());
            std::thread::sleep(std::time::Duration::from_millis(5));
            let own = a
                .collection_dropped_at(id)
                .unwrap()
                .expect("premise: the collection's own tombstone");
            assert!(a.drop_database(DB).unwrap());
            let database =
                a.database_dropped_at(DB).unwrap().expect("premise: the database's tombstone");
            assert!(database > own, "premise: the database was dropped after the collection");
            (Some(own), Some(database))
        } else {
            (None, None)
        };
        let held = options_bytes(&a, "orders").expect("premise: the sender holds the options row");

        let page = a.snapshot_page(None, Some(id)).expect("the answer");

        let carried: Vec<(u8, Vec<u8>, Vec<u8>)> = page
            .carried
            .iter()
            .map(|row| (row.table, row.key.to_vec(), row.value.to_vec()))
            .collect();
        assert!(
            carried.contains(&(crate::CARRY_OPTIONS, id_key("orders"), held)),
            "gone={gone}: the answer carries the options row of the collection, as the sender stores it: {carried:?}"
        );
        assert_eq!(
            page.dropped, own,
            "gone={gone}: the collection's own tombstone, when it is gone"
        );
        assert_eq!(
            page.dropped_database, database,
            "gone={gone}: the database's, when the sender holds it and the collection is gone"
        );
    }
}

/// w1c, the THIRD LEG (P-plan.md section 8's case): a gone collection with NO tombstone of its own, under a database drop alone. The collection's own tombstone was collected (row4's step 2: a cutoff between the two drops) and the database's is kept, so the sender
/// holds the options row, no own tombstone and the database's floor. The scoped answer carries the row as the sender stores it, no `dropped` (there is none to carry), and `dropped_database` (the sender's coverage names the entry, since it took it by a window).
/// EXPECTED: GUARD, green at 30f3fc04 (read: `snapshot_page` reads the rows once for every scoped call, and names the database's drop for any collection that is gone here, whatever its own tombstone). It changes at NO P commit; it must stay green through P12, whose collector keeps
/// the row while the database's tombstone is kept (w3). Mutants: the options row collected with the collection's own tombstone (the premise on the row fails); `dropped_database` named only when the collection has a tombstone of its own; the row carried only when there is one.
/// If it is RED at the first run, that is a finding for P (a path of the scoped answer that does not carry the row), not a test bug.
#[test]
fn w1c_a_gone_collection_with_no_tombstone_of_its_own_carries_its_row_and_names_the_database_drop()
{
    let id = id_of("orders");
    let (m, _dm) = engine();
    let life = m.create_collection(DB, "orders").expect("the life");
    let t0 = life.created.wall_ms;
    let (x, _dx) = engine();
    let (own_drop, database_drop) =
        (drop_of(&x, t0 + 10_000), drop_database_entry(&x, t0 + 20_000));
    window(&m, std::slice::from_ref(&own_drop));
    window(&m, std::slice::from_ref(&database_drop));
    m.finish_purges_now().expect("the purges finish");
    // the collector, with a cutoff between the two drops: the collection's own tombstone goes, the database's stays
    m.collect_garbage_at(
        own_drop.stamp.hlc.wall_ms + 1_000 + 5_000,
        RetentionPolicy::new(31_536_000, 1),
    )
    .expect("the retention pass runs");
    assert_eq!(
        m.collection_dropped_at(id).unwrap(),
        None,
        "premise: no tombstone of the collection's own"
    );
    assert_eq!(
        m.database_dropped_at(DB).unwrap(),
        Some(database_drop.stamp),
        "premise: the database's tombstone is kept"
    );
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the collection is gone");
    let held =
        options_bytes(&m, "orders").expect("premise: the sender still holds the options row");

    let page = m.snapshot_page(None, Some(id)).expect("the answer");
    let carried: Vec<(u8, Vec<u8>, Vec<u8>)> =
        page.carried.iter().map(|row| (row.table, row.key.to_vec(), row.value.to_vec())).collect();
    assert!(
        carried.contains(&(crate::CARRY_OPTIONS, id_key("orders"), held)),
        "the answer carries the options row, as the sender stores it, though no tombstone of the collection's own is held: {carried:?}"
    );
    assert_eq!(page.dropped, None, "and no tombstone of its own, for there is none");
    assert_eq!(
        page.dropped_database,
        Some(database_drop.stamp),
        "and it names the database's drop: the floor a fetching member judges a write above it by"
    );
}

// ---- ROW 5: adoption by comparison, never when the row rises ----------------------------------------------------------------------------------------------------------------

/// ROW 5, the state half. Source: build-order addendum 12:52 AM, "RULED, binding on P: a standing life adopts by COMPARING its definition with the row every time a creation is seen. It must NEVER adopt 'when the row rises'. The row commits first, and a redelivered creation
/// after a crash raises nothing, so adoption tied to the rise would be lost for good."
/// The post-crash state is BUILT BY HAND (the crash itself is the cfg(any()) half below): a member takes creation A (a `field` collection, R = 1 h) and stands with A's options; the row is then raised to creation B's (a later Hlc, R = 48 h) in a transaction of its own, as
/// `note_creation_seen` commits it before the adoption (the row holds B, the definition still holds A); the SAME creation B is then delivered. WANTED: the delivery applies, the standing life ends with B's options (adopted by comparing the definition with B, not because the row
/// rose), the row's bytes are unchanged by it, and no refusal is counted.
/// EXPECTED: GUARD, probably green at 7b6d2ba4 (the entries route adopts when `held.join_retention_ms != target.join_retention_ms` and the creation is the later one: sync.rs:2722-2730, whatever the row holds). It goes red if P ties the adoption to the row's rise.
/// Mutant: adoption only when the row rises (the redelivery leaves the definition at A for good).
#[test]
fn row5_a_creation_redelivered_after_its_row_committed_and_its_adoption_did_not_adopts_by_comparison_and_raises_nothing()
 {
    let base = physical_now_ms().saturating_sub(5_000);
    let k = LossLimits { cap: 4, cut_bytes: 64 };
    let (author, _da) = engine();
    let (other, _do) = engine();
    let a = creation_entry(&author, base, &field_body(3_600_000, k));
    let b = creation_entry(&other, base + 1_000, &field_body(172_800_000, k));
    let (m, _dm) = engine();
    m.apply_batch(std::slice::from_ref(&a)).expect("A applies");
    assert_eq!(
        m.get_collection(DB, "orders").expect("A's life stands").join_retention_ms,
        Some(3_600_000),
        "premise: the definition holds A's R"
    );

    // the row commits for B; the adoption never happened
    let b_options = BaseOptions::of_creation(
        DB,
        "orders",
        b.stamp.hlc,
        MergePolicy::Field,
        Some(172_800_000),
        Some(k),
        None,
    );
    {
        let txn = m.begin_write(WriterHolder::Ddl).expect("a writer");
        assert!(
            crate::register::note_base_options(&txn, &b_options).expect("the row"),
            "premise: B's options rise above A's"
        );
        txn.commit().expect("the row commits");
    }
    let row_before = options_bytes(&m, "orders").expect("the row holds B");
    assert_eq!(
        options_of(&m, "orders").expect("the row").at,
        b.stamp.hlc,
        "premise: the row holds B's"
    );
    assert_eq!(
        m.get_collection(DB, "orders").unwrap().join_retention_ms,
        Some(3_600_000),
        "premise: the definition still holds A's R"
    );

    let outcome = m.apply_batch(std::slice::from_ref(&b)).expect("the redelivery applies");

    assert_eq!(
        m.get_collection(DB, "orders").unwrap().join_retention_ms,
        Some(172_800_000),
        "the standing life ends with B's options, adopted by comparison"
    );
    assert_eq!(
        options_bytes(&m, "orders").expect("the row"),
        row_before,
        "the redelivery raised nothing: the row's bytes are unchanged"
    );
    assert_eq!(outcome.ddl_refused, 0, "and no refusal is counted");
}

/// ROW 5, the crash half: the failure BETWEEN the row's commit and the adoption, on the first delivery. The `Race::OptionsRowCommitted` point (sync.rs, the entries route, right after
/// `note_creation_seen` has committed the row in a transaction of its own) is where the test arms a failed write: the row holds B and the definition A.
/// WANTED: the first delivery fails after the row committed; the second delivery (the same entry) succeeds, adopts B by comparison, and raises nothing (the same assertions as the state half).
/// `row5b` is the same stop on a member that holds NO collection yet: a lower creation delivered afterwards makes the collection from its own options and the definition then follows the row.
mod row5_crash_half {
    use super::*;

    /// An engine on a directory of its own, and the path to open it again by: a failed write at the stop point poisons the handle (redb asks to close and re-open the
    /// database), so a crash is followed by a REOPEN on the same directory, as a restart would do.
    fn on_a_directory() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        (dir, path)
    }

    #[test]
    fn row5_crash_half_a_stop_after_the_options_row_commits_is_healed_by_the_redelivery() {
        let base = physical_now_ms().saturating_sub(5_000);
        let k = LossLimits { cap: 4, cut_bytes: 64 };
        let (author, _da) = engine();
        let (other, _do) = engine();
        let a = creation_entry(&author, base, &field_body(3_600_000, k));
        let b = creation_entry(&other, base + 1_000, &field_body(172_800_000, k));
        let (_dir, path) = on_a_directory();
        let m = std::sync::Arc::new(Engine::open(&path).unwrap());
        m.apply_batch(std::slice::from_ref(&a)).expect("A applies");
        let stopped = std::sync::Arc::clone(&m);
        crate::sync::race_hooks::at(
            crate::sync::race_hooks::Race::OptionsRowCommitted,
            move || {
                assert!(stopped.arm_test_storage_failure("write"), "premise: the stop is armed");
            },
        );
        assert!(
            m.apply_batch(std::slice::from_ref(&b)).is_err(),
            "the first delivery stops after the row committed"
        );
        // the restart
        drop(m);
        let m = Engine::open(&path).unwrap();
        let row_before = options_bytes(&m, "orders").expect("the row holds B");
        assert_eq!(
            options_of(&m, "orders").expect("the row").at,
            b.stamp.hlc,
            "premise: the row committed for B before the stop"
        );
        assert_eq!(
            m.get_collection(DB, "orders").unwrap().join_retention_ms,
            Some(3_600_000),
            "premise: the definition still holds A's R"
        );
        let outcome = m.apply_batch(std::slice::from_ref(&b)).expect("the redelivery applies");
        assert_eq!(
            m.get_collection(DB, "orders").unwrap().join_retention_ms,
            Some(172_800_000),
            "adopted by comparison"
        );
        assert_eq!(
            options_bytes(&m, "orders").expect("the row"),
            row_before,
            "the redelivery raised nothing"
        );
        assert_eq!(outcome.ddl_refused, 0, "and counted no refusal");
    }

    /// The stop between the row's commit and the creation, on a member that holds no collection: B (the later creation) commits its row and stops; A (the lower creation)
    /// is delivered after the restart, makes the collection from its own options, and the definition follows the row, so it ends with B's, as it would had B been applied.
    #[test]
    fn row5b_a_lower_creation_met_after_a_later_ones_row_committed_and_stopped_ends_with_the_rows_options()
     {
        let base = physical_now_ms().saturating_sub(5_000);
        let k = LossLimits { cap: 4, cut_bytes: 64 };
        let (author, _da) = engine();
        let (other, _do) = engine();
        let a = creation_entry(&author, base, &field_body(3_600_000, k));
        let b = creation_entry(&other, base + 1_000, &field_body(172_800_000, k));
        let (_dir, path) = on_a_directory();
        let m = std::sync::Arc::new(Engine::open(&path).unwrap());
        let stopped = std::sync::Arc::clone(&m);
        crate::sync::race_hooks::at(
            crate::sync::race_hooks::Race::OptionsRowCommitted,
            move || {
                assert!(stopped.arm_test_storage_failure("write"), "premise: the stop is armed");
            },
        );
        assert!(
            m.apply_batch(std::slice::from_ref(&b)).is_err(),
            "the delivery of B stops after its row committed"
        );
        // the restart
        drop(m);
        let m = Engine::open(&path).unwrap();
        assert!(
            m.get_collection(DB, "orders").is_err(),
            "premise: nothing was made by the stopped delivery"
        );
        assert_eq!(
            options_of(&m, "orders").expect("the row holds B").at,
            b.stamp.hlc,
            "premise: B's row committed"
        );
        let row_before = options_bytes(&m, "orders").expect("the row");
        m.apply_batch(std::slice::from_ref(&a)).expect("A applies");
        assert_eq!(
            m.get_collection(DB, "orders").expect("A made the collection").join_retention_ms,
            Some(172_800_000),
            "the definition follows the row: B's R, not the A that made it"
        );
        assert_eq!(
            options_bytes(&m, "orders").expect("the row"),
            row_before,
            "and A raised nothing"
        );
    }
}
