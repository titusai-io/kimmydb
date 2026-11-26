//! Unit P's rows from hardproblems' P review (build order, 12:25 PM CST addendum; the tester, Thu Nov 26 2026, against 30f3fc04). NOT compiled, NOT run, NOT queued. Install as
//! crates/kimmy-storage/src/plain_life_page_rows_tests.rs with `#[cfg(test)] mod plain_life_page_rows_tests;`.
//!
//! - pb1, pb2, pb3 (A2): the DROP-BETWEEN shape through a PAGE and through the CARRY's merge (the carry's names and options rows are merged when the carry's walk ends, BEFORE the pull's first document is applied; hardproblems' 1:10 PM ruling, not at the grant). The sender holds c2's life and has seen c4 (its options row is at c4); the receiver holds only
//!   d3 (c2 < d3 < c4). The page's state (created = c2) is refused by d3, but the row with `at` = c4 is above the floor, so the collection stands and the receiver makes it. pb1: the whole-database pages as they
//!   are. pb2: the same pages with their DEFINITIONS taken out, so only the carry brings the row (the carry's merge, before the pull's first document, must make it). pb3, a guard: a floor ABOVE the row's `at` makes nothing.
//!   RED today (pb1, pb2: the state is refused and nothing remakes it; the page route has no remake). GREEN at P7 (A2: ONE function, "gone, never swapped, row at above the later floor: make it"; DATED by hardproblems' 3:33 PM split: a ROW above the floor makes the creation's life (created = the row's `at`, NO Redated marker), a FACT above the floor with the row's `at` at or below it makes the drop's remains (created = the floor's successor, the marker SET),
//!   called at both route tops and at the carry's merge; the coder says if the page and carry callers land in another commit). pb3 is green today and must stay green: its mutant is "a remake whenever a row exists".
//! - pc1, pc2 (A1, B3): the remake fires for EXACTLY the kinds the keep test counts, so gone-versus-standing does not depend on order. pc1: CreateIndex@5, DropIndex@6, drop@4, in the three causal orders. pc2: the
//!   DropIndex ALONE above the floor (the index was made below it). pc3: a ConfigureVectors above the floor of a gone collection remakes it (it is not history), in both orders. Each: every order ends in the SAME state, the
//!   fold's: the collection stands, dated above d4 (its own tombstone stays d4), with the facts above the floor applied. RED today (at least one order loses the collection or the fact), GREEN at P7 (B3: a document, a delete,
//!   CreateIndex, DropIndex, ConfigureValidator and ConfigureVectors remake; the keep test counts the same kinds). Mutants (P7): the remake without DropIndex (pc2); without ConfigureVectors (pc3); the keep test counting
//!   a kind the remake does not (pc1, pc2, pc3: the orders disagree).
//! - pd1 (B5): `is_history` for a STANDING collection reads the DATABASE's drop too: a member holds a collection that stands, then holds a database tombstone D that no sweep has run under yet (the window the sweep closes
//!   after the grant, the coder's 12:45 AM trace); a write at or below D is history, one above it is applied. RED today (`is_history` reads the collection's own tombstone and its incarnation floor, neither of which holds D),
//!   GREEN at P7 (B5: "reads both floors, memoised by database", 0.5 h and this test). Mutants: the database's floor not read; read only when the collection holds no own tombstone.
//!   pd1 models the window with `record_database_tombstone` (pub(crate): the tombstone and nothing else); a member in the real window is one whose page carried D and whose sweep has not run.
//!
//! - pe1, pe2 (A3, B1): a remake that CANNOT run WAITS. pe1, the page route: the receiver holds the floor d3 and the unpurged rows of the collection it dropped; the page's state is refused by d3 but the row's `at` (c4) is above it, so the page waits WHOLE,
//!   as `page_waits_on_a_purge` does: `purge_pending` names the id, nothing is applied, `progress` does not move, the purge is asked for; after the purge the same page applies and the collection is made. pe2, the entries route: a write above the floor that meets
//!   `CollectionPurging` is left UNTAKEN (`PurgePending`: not appended, the window stops), never judged history; after the purge the transport's re-offer applies it and the collection stands with that write alone. RED today (the page's pre-check skips a state that the
//!   tombstone makes history, so nothing waits; the gone arm judges the write history by the own tombstone), GREEN at P7 (A3: the remake's two route callers; the coder says if the page wait lands with P10).
//!   There is NO "row still staged" wait to write (hardproblems' 1:10 PM ruling): the carry's names and options rows are merged when the carry's walk ends, before the pull's first document, so a row is held by the time any document is judged; pb2's outcome stands.
//! - pf1, pf2, pf3 (Q7 `relay_loss_record`, docs.rs:1998-2092; hardproblems' 1:10 PM ruling): a LossRecord for a gone `field` collection is PARKED, not history. pf1: a LossRecord alone above the floor makes no collection and does not stop the window (green today, and at P7).
//!   pf2: above the floor it is APPENDED (served onward) and PARKED by collection id and document with the row's R (its expiry is the loser's wall plus R), and no collection is made: RED today (the gone arm judges it history: not parked), GREEN at P7. pf3, the order row: drop@4, write@5,
//!   LossRecord@6 on a `field` collection, in the orders (5, 6) and (6, 5): the same collection, the same loss record on the document, and the entry in both oplogs. RED today (both orders lose the write and the record), GREEN at P7. Mutant (P7): the record taken as history before the write
//!   remakes the collection (the order (6, 5) loses the record).
//! - ph1 (Redated marker ruling, hardproblems' 2:37 PM): a GUARD that a stage created below a database drop leaves the two orders of delivery ALIKE (the register's logical drops, the own tombstone, the standing of `orders` and of the stage). The count is not asserted: a stage creation that
//!   arrives after D is refused under D, so it is 0 as built. Its sibling is the survivor file's s1 marker row.
//! - ph2 (the case the Redated ruling is really about): `orders` held, a stage created at sc, a database drop D above sc, a write W above D from a member that had not heard D, and a binding B of the stage above D and inside its deadline. One member takes them as "W, D, sc, B" (the life is
//!   re-dated) and as "D, W, sc, B" (the collection is remade): both end alike in the register's events for `orders`, the own tombstone, the marker `is_redated()`, and whether `orders` and the stage stand. Expected GREEN today, count 1 or 0 in both (not asserted). Installed with P7b; mutant:
//!   the remake sets no Redated marker.
//!
//! - pg1 (hardproblems' 3:33 PM, against P7b 13882962): the one stop path left after P7b flips the old "later-life stop" tests to remakes, `defines_a_later_life` (sync.rs:1832, the `UnknownCollection` arm ~2410): a CreateIndex stamped ABOVE a collection's
//!   tombstone reaches a member that holds the drop and NO options row for the id. The window STOPS (the transport plans the fetch), nothing is made before the fetch; after the fetch, whose page carries the row and the author's definition above the floor (so the page remakes the collection itself), the collection is held as P7b rules and the window proceeds.
//!
//! First-compile risks: `Hlc::successor` (pub, hlc.rs:65), `Engine::record_database_tombstone`, `index_dropped_at(CollectionId, u32)`, `IndexMeta::derive_id`, `VectorConfig`'s fields (as unit S's tests build it), and the page's `collections`
//! being assignable (pub) for pb2.

use bson::doc;
use kimmy_core::merge_state::{
    LossKey, LossKind, LossValue, MStamp, MergeState, Path, Value, WriterId,
};
use kimmy_core::{
    CollectionBind, CollectionCreate, CollectionId, CollectionRef, DocId, Hlc, IndexField,
    IndexMeta, LossLimits, MergePolicy, OpKind, OplogEntry, Stamp, VersionVector,
};
use redb::ReadableDatabase;

use crate::register::{BaseOptions, Carried};
use crate::{Engine, PeerWindow, SnapshotProgress, SyncOutcome, WalkScope, physical_now_ms};

const DB: &str = "app";

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

fn pause() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn id_of(name: &str) -> CollectionId {
    CollectionId::derive(DB, name)
}

fn id_key(name: &str) -> Vec<u8> {
    id_of(name).0.to_be_bytes().to_vec()
}

/// The options row of `orders`, as stored, if the table holds one.
fn options_of(e: &Engine) -> Option<BaseOptions> {
    let txn = e.db().begin_read().unwrap();
    let key = id_key("orders");
    let (_, value) = crate::register::carried_rows(&txn, Carried::Options)
        .unwrap()
        .into_iter()
        .find(|(k, _)| *k == key)?;
    Some(serde_json::from_slice(&value).unwrap())
}

/// What a member holds of `orders`: whether it stands and how many documents.
fn holds(m: &Engine) -> (bool, u64) {
    match m.get_collection(DB, "orders") {
        Ok(c) => (true, m.count(&c, WalkScope::Background).unwrap()),
        Err(_) => (false, 0),
    }
}

// ---- pb1, pb2, pb3: the drop-between shape through a page and through the carry's merge ----------------------------------------------------------------

/// A creation of `orders` by `by`, stamped `wall_ms`, under `policy` (a `field` creation carries a valid R and K).
fn creation(by: &Engine, wall_ms: u64, policy: MergePolicy) -> OplogEntry {
    let field = policy == MergePolicy::Field;
    let body = CollectionCreate {
        db: DB.into(),
        name: "orders".into(),
        merge_policy: policy,
        validator: None,
        join_retention_ms: field.then_some(60_000),
        loss_limits: field.then_some(LossLimits::DEFAULT),
        stage: None,
    };
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        kind: OpKind::CreateCollection,
        collection: id_of("orders"),
        doc_id: None,
        body: Some(bson::serialize_to_vec(&body).expect("the creation encodes")),
        meta: None,
    }
}

/// The drop of `orders` by `by`, stamped `wall_ms`.
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

/// The stamps of the shape: c2 < d3 < c4, and a floor d5 above all three, in the past by a minute so that no stamp is near the clock bound.
struct Stamps {
    c2: OplogEntry,
    d3: OplogEntry,
    c4: OplogEntry,
    d5: OplogEntry,
}

fn stamps() -> Stamps {
    let base = physical_now_ms() - 60_000;
    let (x, _dx) = engine();
    let (y, _dy) = engine();
    let (z, _dz) = engine();
    let (w, _dw) = engine();
    // c2 is `document`, c4 is `field` (so the remake has to read the ROW's options), the drops are by other members
    Stamps {
        c2: creation(&x, base + 1_000, MergePolicy::Document),
        d3: drop_of(&z, base + 3_000),
        c4: creation(&y, base + 5_000, MergePolicy::Field),
        d5: drop_of(&w, base + 7_000),
    }
}

/// The sender: it holds c2's life, and has SEEN c4 (a creation of the name over a standing life: the options row rises to c4, and the definition follows it).
fn the_sender(s: &Stamps) -> (Engine, tempfile::TempDir) {
    let (m, d) = engine();
    window(&m, std::slice::from_ref(&s.c2));
    window(&m, std::slice::from_ref(&s.c4));
    let held = m.get_collection(DB, "orders").expect("premise: the sender holds a life");
    assert_eq!(held.created, s.c2.stamp.hlc, "premise: the life it holds is c2's");
    let row = options_of(&m).expect("premise: the sender holds an options row");
    assert_eq!(row.at, s.c4.stamp.hlc, "premise: and its row is c4's, above d3");
    (m, d)
}

/// The receiver: it holds only the drop, as the collection's own tombstone, and no row.
fn the_receiver(drop: &OplogEntry) -> (Engine, tempfile::TempDir) {
    let (m, d) = engine();
    window(&m, std::slice::from_ref(drop));
    assert_eq!(
        m.collection_dropped_at(id_of("orders")).unwrap(),
        Some(drop.stamp),
        "premise: the receiver holds the drop as the collection's own tombstone"
    );
    assert!(m.get_collection(DB, "orders").is_err(), "premise: and no collection");
    assert!(options_of(&m).is_none(), "premise: and no options row");
    (m, d)
}

/// A whole-database pull of `from` into `into`; with `without_definitions` every page's definitions are taken out, so that only the carry's rows reach the receiver.
fn pull(from: &Engine, into: &Engine, without_definitions: bool) {
    let mut progress = SnapshotProgress::whole_database();
    for _ in 0..50 {
        if progress.is_complete() {
            return;
        }
        let mut page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        if without_definitions {
            page.collections.clear();
        }
        into.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
    }
    panic!("the pull did not complete in 50 pages");
}

/// What the receiver must hold after the pull: the collection stands, the CREATION's life (hardproblems' 3:33 PM dating split: made because the ROW's `at` is above the floor, so created = the row's `at`, c4, and NO Redated marker), under the ROW's options (`field`, from c4), the row's `at` in its policy stamp, and no document.
fn the_collection_c4_makes(m: &Engine, s: &Stamps, how: &str) {
    let held = m.get_collection(DB, "orders");
    assert!(held.is_ok(), "{how}: the row with `at` above the floor d3 makes the collection");
    let held = held.unwrap();
    assert_eq!(
        held.created, s.c4.stamp.hlc,
        "{how}: the creation's life: created is the row's `at` (c4), above the floor"
    );
    assert!(
        !held.is_redated(),
        "{how}: and no Redated marker (a remake made by a ROW above the floor is the creation's own life)"
    );
    assert_eq!(
        held.merge_policy,
        MergePolicy::Field,
        "{how}: under the row's options, not the page's state (c2's, `document`)"
    );
    assert_eq!(
        held.merge_policy_at,
        Some(s.c4.stamp.hlc),
        "{how}: the row's `at` is the policy stamp"
    );
    assert_eq!(
        m.count(&held, WalkScope::Background).unwrap(),
        0,
        "{how}: and it holds no document"
    );
    assert_eq!(
        m.collection_dropped_at(id_of("orders")).unwrap(),
        Some(s.d3.stamp),
        "{how}: its own tombstone is still d3: the life follows the drop it follows"
    );
}

/// pb1: the whole-database pages. RED today, GREEN at P7 (A2: the remake at the page route's top).
#[test]
fn pb1_a_row_above_the_floor_makes_the_collection_when_the_pages_state_is_refused() {
    let s = stamps();
    let (sender, _ds) = the_sender(&s);
    let (m, _dm) = the_receiver(&s.d3);
    pull(&sender, &m, false);
    the_collection_c4_makes(&m, &s, "by the whole pages");
}

/// pb2: the same with the definitions taken out of the pages: only the CARRY brings the row, and the carry's merge (when the carry's walk ends, before the pull's first document is applied) must make the collection. RED today, GREEN at P7 (A2: the remake at the carry's merge).
#[test]
fn pb2_a_row_above_the_floor_makes_the_collection_through_the_carrys_merge_before_the_first_document()
 {
    let s = stamps();
    let (sender, _ds) = the_sender(&s);
    let (m, _dm) = the_receiver(&s.d3);
    pull(&sender, &m, true);
    the_collection_c4_makes(&m, &s, "by the carry alone");
}

/// pb3, a GUARD (green today, must stay green): the receiver's floor is d5, ABOVE the row's `at` (c4): the row is no creation above the floor, and nothing is made, by the pages and by the carry alone. Mutant (P7): a remake whenever a row exists.
#[test]
fn pb3_a_row_below_the_floor_makes_nothing_by_the_pages_or_by_the_carry() {
    for without_definitions in [false, true] {
        let s = stamps();
        let (sender, _ds) = the_sender(&s);
        let (m, _dm) = the_receiver(&s.d5);
        pull(&sender, &m, without_definitions);
        assert!(
            m.get_collection(DB, "orders").is_err(),
            "definitions out={without_definitions}: a row at c4, below the floor d5, makes no collection"
        );
    }
}

// ---- pc1, pc2, pc3: the remake fires for exactly the kinds the keep test counts --------------------------------------------------------------------------

/// What every order must end in, the fold's: `orders` stands, its own tombstone is d4, and the facts above d4 are applied.
#[derive(Debug, PartialEq)]
struct Ends {
    stands: bool,
    own_tombstone: Option<Stamp>,
    index_x: bool,
    index_x_dropped: bool,
    vectors: bool,
}

fn ends(m: &Engine) -> Ends {
    let meta = m.get_collection(DB, "orders").ok();
    Ends {
        stands: meta.is_some(),
        own_tombstone: m.collection_dropped_at(id_of("orders")).unwrap(),
        index_x: meta.as_ref().is_some_and(|c| c.index("x").is_some()),
        index_x_dropped: m
            .index_dropped_at(id_of("orders"), IndexMeta::derive_id("x"))
            .unwrap()
            .is_some(),
        vectors: meta.as_ref().is_some_and(|c| c.vector.is_some()),
    }
}

/// The entries of one kind for `orders` that `m` minted itself, oldest first.
fn own(m: &Engine, kind: OpKind) -> Vec<OplogEntry> {
    everything(m)
        .into_iter()
        .filter(|e| {
            e.kind == kind && e.collection == id_of("orders") && e.stamp.node == m.node_id()
        })
        .collect()
}

/// Author `x` makes `orders` (c1). A second member then takes it and DROPS it (`the_drop_by_another_member`), and `x`, which has not heard of the drop, goes on.
fn the_life() -> (Engine, tempfile::TempDir, OplogEntry) {
    let (x, dx) = engine();
    x.create_collection(DB, "orders").unwrap();
    let c1 = own(&x, OpKind::CreateCollection).pop().expect("the creation");
    (x, dx, c1)
}

/// The drop d4 by a member that took c1 only, stamped after `x`'s last operation so far.
fn the_drop_by_another_member(c1: &OplogEntry) -> OplogEntry {
    let (z, _dz) = engine();
    window(&z, std::slice::from_ref(c1));
    pause();
    assert!(z.drop_collection(DB, "orders").unwrap());
    own(&z, OpKind::DropCollection).pop().expect("the drop")
}

/// Every order in which a member that holds `prefix` takes `facts` is tried in a fresh member; each ends as `want` says (the fold). Returns the red lines.
fn every_order_ends_alike(
    prefix: &[OplogEntry],
    orders: &[Vec<&OplogEntry>],
    want: &Ends,
) -> Vec<String> {
    let mut red = Vec::new();
    for order in orders {
        let (m, _dm) = engine();
        for entry in prefix {
            window(&m, std::slice::from_ref(entry));
        }
        assert!(
            m.get_collection(DB, "orders").is_ok(),
            "premise: the member holds the collection before the facts"
        );
        for entry in order {
            window(&m, std::slice::from_ref(*entry));
        }
        let got = ends(&m);
        if &got != want {
            let names: Vec<String> =
                order.iter().map(|e| format!("{:?}@{:?}", e.kind, e.stamp.hlc.wall_ms)).collect();
            red.push(format!("taking {names:?}: ends {got:?}, the fold says {want:?}"));
        }
    }
    red
}

/// pc1: CreateIndex@5, DropIndex@6 (both by `x`, which has not heard of the drop) and the drop@4 (by `z`): in the orders (5, 6, 4), (5, 4, 6) and (4, 5, 6) the member ends the same, the fold's: the collection stands (it has facts above
/// d4), its own tombstone is d4, the index "x" is gone and its tombstone is held. RED today (the order 4, 5, 6 meets the CreateIndex at a gone collection, and the others disagree with it), GREEN at P7 (B3: CreateIndex and DropIndex remake).
#[test]
fn pc1_an_index_created_and_dropped_above_a_drop_ends_the_same_in_every_order() {
    let (x, _dx, c1) = the_life();
    let d4 = the_drop_by_another_member(&c1);
    pause();
    x.create_index(DB, "orders", vec![IndexField::ascending("v")], false, Some("x".into()))
        .unwrap();
    pause();
    assert!(x.drop_index(DB, "orders", "x").unwrap());
    let i5 = own(&x, OpKind::CreateIndex).pop().expect("the index creation");
    let i6 = own(&x, OpKind::DropIndex).pop().expect("the index drop");
    assert!(
        d4.stamp.hlc < i5.stamp.hlc && i5.stamp.hlc < i6.stamp.hlc,
        "premise: drop@4 < CreateIndex@5 < DropIndex@6"
    );
    let want = Ends {
        stands: true,
        own_tombstone: Some(d4.stamp),
        index_x: false,
        index_x_dropped: true,
        vectors: false,
    };
    let red = every_order_ends_alike(
        std::slice::from_ref(&c1),
        &[vec![&i5, &i6, &d4], vec![&i5, &d4, &i6], vec![&d4, &i5, &i6]],
        &want,
    );
    assert!(
        red.is_empty(),
        "pc1: {} of 3 orders end in another state:\n{}",
        red.len(),
        red.join("\n")
    );
}

/// pc2 (A1): the DropIndex ALONE above the floor: the index was made below d4 and is held by the member; the drop of it is stamped above d4. In both orders the member ends the same: the collection stands, own tombstone d4, the index gone and its
/// tombstone held. The keep test counts an index tombstone above the floor; the remake must fire for the same kind, or gone-versus-standing depends on order. RED today, GREEN at P7 (B3: DropIndex remakes). Mutant: the remake without DropIndex.
#[test]
fn pc2_a_drop_of_an_index_alone_above_a_drop_ends_the_same_in_both_orders() {
    let (x, _dx, c1) = the_life();
    x.create_index(DB, "orders", vec![IndexField::ascending("v")], false, Some("x".into()))
        .unwrap();
    let i2 = own(&x, OpKind::CreateIndex).pop().expect("the index creation");
    let d4 = {
        let (z, _dz) = engine();
        window(&z, &[c1.clone(), i2.clone()]);
        pause();
        assert!(z.drop_collection(DB, "orders").unwrap());
        own(&z, OpKind::DropCollection).pop().expect("the drop")
    };
    pause();
    assert!(x.drop_index(DB, "orders", "x").unwrap());
    let i6 = own(&x, OpKind::DropIndex).pop().expect("the index drop");
    assert!(
        i2.stamp.hlc < d4.stamp.hlc && d4.stamp.hlc < i6.stamp.hlc,
        "premise: CreateIndex@2 < drop@4 < DropIndex@6"
    );
    let want = Ends {
        stands: true,
        own_tombstone: Some(d4.stamp),
        index_x: false,
        index_x_dropped: true,
        vectors: false,
    };
    let red =
        every_order_ends_alike(&[c1.clone(), i2.clone()], &[vec![&d4, &i6], vec![&i6, &d4]], &want);
    assert!(
        red.is_empty(),
        "pc2: {} of 2 orders end in another state:\n{}",
        red.len(),
        red.join("\n")
    );
}

fn vector_config() -> kimmy_core::VectorConfig {
    kimmy_core::VectorConfig {
        fields: vec!["text".into()],
        provider: kimmy_core::ProviderConfig::Byo {},
        dim: 4,
        metric: Default::default(),
        document_prefix: None,
        query_prefix: None,
        chunk: Default::default(),
    }
}

/// pc3 (B3): a ConfigureVectors above the floor of a gone collection REMAKES it and is not history, and the orders agree: the collection stands with the configuration, its own tombstone is d4. The configuration makes its shadow
/// (the entry's own effect); P remakes no shadow of its own (ruling B2). RED today (the configuration meets a gone collection), GREEN at P7. Mutant: the remake without ConfigureVectors.
#[test]
fn pc3_a_vector_configuration_above_a_drop_remakes_the_collection_in_both_orders() {
    let (x, _dx, c1) = the_life();
    let d4 = the_drop_by_another_member(&c1);
    pause();
    x.configure_vectors(DB, "orders", vector_config()).expect("the configuration is made");
    let v5 = own(&x, OpKind::ConfigureVectors).pop().expect("the configuration entry");
    assert!(d4.stamp.hlc < v5.stamp.hlc, "premise: drop@4 < ConfigureVectors@5");
    let want = Ends {
        stands: true,
        own_tombstone: Some(d4.stamp),
        index_x: false,
        index_x_dropped: false,
        vectors: true,
    };
    let red =
        every_order_ends_alike(std::slice::from_ref(&c1), &[vec![&d4, &v5], vec![&v5, &d4]], &want);
    assert!(
        red.is_empty(),
        "pc3: {} of 2 orders end in another state:\n{}",
        red.len(),
        red.join("\n")
    );
}

// ---- pd1: is_history for a STANDING collection reads the database's drop -------------------------------------------------------------------------------

/// pd1 (B5): a member holds `orders` standing, and then holds a database tombstone D that no sweep has run under (`record_database_tombstone`: the tombstone and nothing else, the window the sweep closes after the grant). A write
/// stamped BELOW D is history (not applied); a write above D is applied. RED today (`is_history` reads the collection's own tombstone and its incarnation floor: the member holds neither, so the write below D is applied), GREEN at P7 (B5).
/// Mutants: the database's floor not read; read only when the collection holds no own tombstone (the guard half, the write above D, stays green in both).
#[test]
fn pd1_a_write_below_the_databases_drop_is_history_for_a_standing_collection() {
    let (a, _da) = engine();
    let orders = a.create_collection(DB, "orders").unwrap();
    a.insert(&orders, doc! { "_id": 1, "v": "below the drop" }).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(25));
    a.insert(&orders, doc! { "_id": 2, "v": "above the drop" }).unwrap();
    let writes: Vec<OplogEntry> = everything(&a)
        .into_iter()
        .filter(|e| {
            e.doc_id.is_some() && e.collection == id_of("orders") && e.stamp.node == a.node_id()
        })
        .collect();
    assert_eq!(writes.len(), 2, "premise: two writes");
    // D between them
    let d = Stamp::new(Hlc::new(writes[0].stamp.hlc.wall_ms + 10, 0), a.node_id(), a.epoch());
    assert!(
        writes[0].stamp.hlc < d.hlc && d.hlc < writes[1].stamp.hlc,
        "premise: write1 < D < write2"
    );
    let creation = own(&a, OpKind::CreateCollection).pop().expect("the creation");

    let (m, _dm) = engine();
    window(&m, std::slice::from_ref(&creation));
    assert_eq!(holds(&m), (true, 0), "premise: the member holds the collection standing");
    m.record_database_tombstone(DB, d)
        .expect("the database's tombstone is recorded, and nothing else");
    assert!(
        m.collection_dropped_at(id_of("orders")).unwrap().is_none(),
        "premise: the collection holds no tombstone of its own"
    );
    assert_eq!(
        m.database_dropped_at(DB).unwrap(),
        Some(d),
        "premise: the database's tombstone is held"
    );

    window(&m, std::slice::from_ref(&writes[0]));
    assert_eq!(
        holds(&m),
        (true, 0),
        "pd1: a write stamped below the database's drop is history: it is not applied"
    );
    window(&m, std::slice::from_ref(&writes[1]));
    assert_eq!(holds(&m), (true, 1), "and a write above it is applied");
}

// ---- pe1, pe2: a remake that cannot run waits (A3) ------------------------------------------------------------------------------------------------------

/// Real members, real stamps: `s` makes `orders` (c2) and writes document 1; `m` takes both (so it holds the collection and a row of data under its id); `z` takes them and DROPS the collection (d3); `y`, which made the name on its own after d3, makes it again (c4, `field`); `s` takes
/// c4 (a creation of the name over its standing life: its row rises to c4). `m` then takes d3: the collection is buried and its rows await the purger (NOT run). Returns (s, m, c2, d3, c4).
fn the_world_where_a_purge_is_pending()
-> (Engine, Engine, OplogEntry, OplogEntry, OplogEntry, [tempfile::TempDir; 3]) {
    let (s, ds) = engine();
    let first = s.create_collection(DB, "orders").unwrap();
    s.insert(&first, doc! { "_id": 1, "v": "old" }).unwrap();
    let c2 = own(&s, OpKind::CreateCollection).pop().expect("c2");
    let (m, dm) = engine();
    window(&m, &everything(&s));
    assert_eq!(holds(&m), (true, 1), "premise: the receiver holds the collection and its document");
    let (z, _dz) = engine();
    window(&z, &everything(&s));
    pause();
    assert!(z.drop_collection(DB, "orders").unwrap());
    let d3 = own(&z, OpKind::DropCollection).pop().expect("d3");
    pause();
    let (y, dy) = engine();
    y.create_collection_with_policy(DB, "orders", MergePolicy::Field, None).unwrap();
    let c4 = own(&y, OpKind::CreateCollection).pop().expect("c4");
    assert!(c2.stamp.hlc < d3.stamp.hlc && d3.stamp.hlc < c4.stamp.hlc, "premise: c2 < d3 < c4");
    window(&s, std::slice::from_ref(&c4));
    assert_eq!(
        options_of(&s).expect("premise: the sender holds a row").at,
        c4.stamp.hlc,
        "premise: its row is c4's"
    );
    window(&m, std::slice::from_ref(&d3));
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the drop buried the collection");
    assert!(m.rows_under(id_of("orders")).unwrap() > 0, "premise: its rows are not purged");
    (s, m, c2, d3, c4, [ds, dm, dy])
}

/// pe1, the PAGE route. RED today: the page's pre-check (`page_waits_on_a_purge`) skips a state the tombstone makes history, so nothing waits, the page is applied, and the row above the floor makes nothing. GREEN at P7 (A3).
/// Mutants (P7): the page's remake runs without checking the pending purge (the old rows come back into the new life: the second half); the wait counted only for a state that is not history.
#[test]
fn pe1_a_page_whose_remake_cannot_run_waits_whole_and_applies_after_the_purge() {
    let (s, m, _c2, _d3, c4, _dirs) = the_world_where_a_purge_is_pending();
    let id = id_of("orders");
    let page = s.snapshot_page(None, None).unwrap();
    let mut progress = SnapshotProgress::whole_database();
    let waited = m.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
    assert_eq!(
        waited.purge_pending,
        Some(id),
        "pe1: a page whose row above the floor needs a remake waits on the pending purge: {waited:?}"
    );
    assert_eq!(progress.pages(), 0, "pe1: and progress did not move");
    assert!(m.get_collection(DB, "orders").is_err(), "pe1: and nothing on the page was applied");
    assert_eq!(m.queued_purges().first(), Some(&id), "pe1: and the purge was asked for");

    m.finish_purges_now().unwrap();
    let applied = m.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
    assert_eq!(applied.purge_pending, None, "pe1: after the purge the page applies: {applied:?}");
    assert!(progress.is_complete(), "pe1: and the pull completes");
    let held = m.get_collection(DB, "orders").expect("pe1: the collection is made from the row");
    assert_eq!(
        held.created, c4.stamp.hlc,
        "pe1: the creation's life: created is the row's `at` (c4)"
    );
    assert!(
        !held.is_redated(),
        "pe1: and no Redated marker (made because the ROW is above the floor)"
    );
    assert_eq!(held.merge_policy, MergePolicy::Field, "pe1: under the row's options");
    assert_eq!(held.merge_policy_at, Some(c4.stamp.hlc), "pe1: the row's `at` is the policy stamp");
    assert_eq!(
        m.count(&held, WalkScope::Background).unwrap(),
        0,
        "pe1: and none of the old rows came back"
    );
}

/// pe2, the ENTRIES route: a write above the floor that meets a pending purge is left UNTAKEN, never judged history. `a` made the collection and wrote document 1; `z` took them and dropped it (d); `a`, which has not heard of the drop, writes document 2 (W, above d). The member `m`
/// holds the collection (and document 1), takes d (buried, rows pending), then W. RED today (the gone arm finds the own tombstone and returns History: `purge_pending` is 0, the write is lost, nothing is re-offered), GREEN at P7 (A3: a remake that meets `CollectionPurging` leaves the entry untaken:
/// `PurgePending`, not appended; the purge completes; the re-offer applies it). Mutants (P7): the entry taken as history while a purge pends; the entry applied without a collection.
#[test]
fn pe2_a_write_above_the_floor_that_meets_a_pending_purge_is_left_untaken_and_applies_when_offered_again()
 {
    let (a, _da) = engine();
    let orders = a.create_collection(DB, "orders").unwrap();
    a.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    let (z, _dz) = engine();
    window(&z, &everything(&a));
    let (m, _dm) = engine();
    window(&m, &everything(&a));
    assert_eq!(holds(&m), (true, 1), "premise: the member holds the collection and its document");
    pause();
    assert!(z.drop_collection(DB, "orders").unwrap());
    let d = own(&z, OpKind::DropCollection).pop().expect("the drop");
    pause();
    a.insert(&orders, doc! { "_id": 2, "v": "after the drop" }).unwrap();
    let w: Vec<OplogEntry> = everything(&a)
        .into_iter()
        .filter(|e| e.doc_id == Some(DocId::Int64(2)) && e.collection == id_of("orders"))
        .collect();
    assert_eq!(w.len(), 1, "premise: one write of document 2");
    assert!(d.stamp.hlc < w[0].stamp.hlc, "premise: the write is stamped above the drop");
    window(&m, std::slice::from_ref(&d));
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the drop buried the collection");
    assert!(m.rows_under(id_of("orders")).unwrap() > 0, "premise: its rows are not purged");

    let first = window(&m, &w);
    assert!(
        first.purge_pending > 0,
        "pe2: a write above the floor that meets a pending purge is left untaken (PurgePending), not judged history: {first:?}"
    );
    assert!(!everything(&m).iter().any(|e| e.stamp == w[0].stamp), "pe2: and it is not appended");
    assert!(
        m.get_collection(DB, "orders").is_err(),
        "pe2: and no collection is made over rows that are not purged"
    );

    m.finish_purges_now().unwrap();
    let again = window(&m, &w);
    assert_eq!(again.purge_pending, 0, "pe2: after the purge the re-offer is taken: {again:?}");
    let held = m.get_collection(DB, "orders").expect("pe2: the write remakes the collection");
    assert_eq!(
        held.created,
        d.stamp.hlc.successor(),
        "pe2: dated just above the floor (made by a FACT above the floor, the row's `at` below it: the drop's remains)"
    );
    assert!(held.is_redated(), "pe2: with the Redated marker set");
    assert_eq!(m.count(&held, WalkScope::Background).unwrap(), 1, "pe2: with the write alone");
    assert!(m.get(&held, &DocId::Int64(2)).unwrap().is_some(), "pe2: document 2 is there");
    assert!(
        m.get(&held, &DocId::Int64(1)).unwrap().is_none(),
        "pe2: and document 1, below the floor, did not come back"
    );
}

// ---- pf1, pf2: a relayed loss record does not remake a gone collection -----------------------------------------------------------------------------------

/// A relayed loss record for document 1 of `orders`, by `from`, stamped `wall + 1` (the loser's stamp is `wall`), as `loss_limits_tests::loss_entry` builds it.
fn loss_entry(from: &Engine, wall: u64) -> (LossKey, OplogEntry) {
    loss_entry_for(from, 1, wall)
}

/// The same for document `doc`.
fn loss_entry_for(from: &Engine, doc: i64, wall: u64) -> (LossKey, OplogEntry) {
    let key = LossKey::field(
        LossKind::Clash,
        Path::flat("x").unwrap(),
        MStamp {
            hlc: Hlc::new(wall, 0),
            writer: WriterId { node: from.node_id(), epoch: from.epoch() },
        },
    );
    let entry = OplogEntry {
        stamp: Stamp::new(Hlc::new(wall + 1, 0), from.node_id(), from.epoch()),
        kind: OpKind::LossRecord,
        collection: id_of("orders"),
        doc_id: Some(DocId::Int64(doc)),
        body: Some(MergeState::encode_loss_record(&key, &LossValue::Value(Value::Int64(7)))),
        meta: None,
    };
    (key, entry)
}

/// A member that holds a gone `field` collection (its own tombstone d, no rows left to purge) and the loss record, stamped above d, that is taken by it.
fn a_gone_field_collection_and_a_loss_record_above_its_floor()
-> (Engine, tempfile::TempDir, OplogEntry, OplogEntry) {
    let (x, _dx) = engine();
    let orders = x.create_collection_with_policy(DB, "orders", MergePolicy::Field, None).unwrap();
    x.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    let (z, _dz) = engine();
    window(&z, &everything(&x));
    let (m, dm) = engine();
    window(&m, &everything(&x));
    pause();
    assert!(z.drop_collection(DB, "orders").unwrap());
    let d = own(&z, OpKind::DropCollection).pop().expect("the drop");
    window(&m, std::slice::from_ref(&d));
    m.finish_purges_now().unwrap();
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the collection is gone");
    assert_eq!(
        m.collection_dropped_at(id_of("orders")).unwrap(),
        Some(d.stamp),
        "premise: with its own tombstone"
    );
    let (_key, loss) = loss_entry(&x, d.stamp.hlc.wall_ms + 1_000);
    assert!(d.stamp.hlc < loss.stamp.hlc, "premise: the loss record is stamped above the drop");
    (m, dm, d, loss)
}

/// pf1: a relayed loss record above the floor of a gone collection does NOT remake it: no collection is made and the window is not stopped (the record is taken, not waited for). A GUARD: GREEN today (the gone arm judges it history by the own tombstone) and it must
/// stay green at P7: the remake fires for a document, a delete, CreateIndex, DropIndex, ConfigureValidator and ConfigureVectors, and NOT for a loss record (B3). Mutant (P7): a loss record counts as a fact that remakes.
#[test]
fn pf1_a_relayed_loss_record_above_the_floor_of_a_gone_collection_does_not_remake_it() {
    let (m, _dm, _d, loss) = a_gone_field_collection_and_a_loss_record_above_its_floor();
    let outcome = window(&m, std::slice::from_ref(&loss));
    assert!(
        m.get_collection(DB, "orders").is_err(),
        "pf1: a loss record is no fact that makes a collection again"
    );
    assert_eq!(
        (outcome.unknown_collection, outcome.purge_pending),
        (0, 0),
        "pf1: and the window is not stopped: {outcome:?}"
    );
}

/// The values parked under (collection, document key), as stored: `expires (8 bytes, big-endian) || body`.
fn parked_under(e: &Engine, collection: CollectionId, key: &[u8]) -> Vec<Vec<u8>> {
    let txn = e.db().begin_read().unwrap();
    let table = txn.open_table(crate::tables::LOSS_PENDING).unwrap();
    table
        .range((collection.0, key, &[][..])..=(collection.0, key, &[0xff; 64][..]))
        .unwrap()
        .map(|row| row.unwrap().1.value().to_vec())
        .collect()
}

/// pf2 (ruled, 1:10 PM): above the floor of a gone `field` collection the record is APPENDED (served onward) and PARKED by collection id and document key with the row's R (its expiry is the loser's wall plus R), and no collection is made. RED today (the gone arm judges
/// the record history by the own tombstone: nothing is parked), GREEN at P7. Mutants (P7): the record taken as history; parked with the node default for R and not the row's; parked but not appended (not served onward).
#[test]
fn pf2_a_relayed_loss_record_above_the_floor_of_a_gone_collection_is_appended_and_parked_with_the_rows_r()
 {
    let (m, _dm, d, loss) = a_gone_field_collection_and_a_loss_record_above_its_floor();
    let r = options_of(&m)
        .expect("premise: the member holds the collection's row")
        .join_retention_ms
        .expect("premise: a field row holds R");
    window(&m, std::slice::from_ref(&loss));
    assert!(m.get_collection(DB, "orders").is_err(), "pf2: no collection is made");
    assert!(
        everything(&m).iter().any(|e| e.stamp == loss.stamp),
        "pf2: the record is appended, so the member serves it onward"
    );
    let key = crate::docs::doc_key(&DocId::Int64(1)).unwrap();
    let parked = parked_under(&m, id_of("orders"), &key);
    assert_eq!(parked.len(), 1, "pf2: and it is parked by collection and document");
    let expires = u64::from_be_bytes(parked[0][..8].try_into().expect("eight bytes"));
    let loser_wall = d.stamp.hlc.wall_ms + 1_000;
    assert_eq!(
        expires,
        loser_wall + r,
        "pf2: parked with the row's R: it expires at the loser's wall plus R"
    );
}

/// pf3, the ORDER row: drop@4, write@5 (document 2, above the drop), LossRecord@6 (for document 2, stamped above the write) on a `field` collection, delivered in the orders (5, 6) and (6, 5) to members that hold the collection and its first document and took the drop. Both orders end with
/// the collection standing, document 2 held, the SAME loss record on it, and the record in the member's oplog. RED today (the write meets a gone collection and is lost, and the record is judged history, in both orders), GREEN at P7. Mutant (P7): the record taken as history
/// before the write remakes the collection: the order (6, 5) then loses it and the two orders differ.
#[test]
fn pf3_a_write_and_a_loss_record_above_a_drop_end_alike_in_both_orders() {
    let (x, _dx) = engine();
    let orders = x.create_collection_with_policy(DB, "orders", MergePolicy::Field, None).unwrap();
    x.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    let first = everything(&x);
    let (z, _dz) = engine();
    window(&z, &first);
    pause();
    assert!(z.drop_collection(DB, "orders").unwrap());
    let d4 = own(&z, OpKind::DropCollection).pop().expect("drop@4");
    pause();
    x.insert(&orders, doc! { "_id": 2, "v": "after the drop" }).unwrap();
    let w5 = everything(&x)
        .into_iter()
        .find(|e| e.doc_id == Some(DocId::Int64(2)) && e.collection == id_of("orders"))
        .expect("write@5");
    let (key, l6) = loss_entry_for(&x, 2, w5.stamp.hlc.wall_ms + 100);
    assert!(
        d4.stamp.hlc < w5.stamp.hlc && w5.stamp.hlc < l6.stamp.hlc,
        "premise: drop@4 < write@5 < LossRecord@6"
    );
    let mut ends = Vec::new();
    for (what, order) in [("(5, 6)", [&w5, &l6]), ("(6, 5)", [&l6, &w5])] {
        let (m, _dm) = engine();
        window(&m, &first);
        window(&m, std::slice::from_ref(&d4));
        m.finish_purges_now().unwrap();
        assert!(
            m.get_collection(DB, "orders").is_err(),
            "premise: the collection is gone before the facts"
        );
        for entry in order {
            window(&m, std::slice::from_ref(entry));
        }
        let losses: Option<Vec<LossKey>> =
            m.snapshot_page(None, Some(id_of("orders"))).unwrap().documents.first().map(|doc| {
                crate::field_write::load(doc.meta.as_deref())
                    .unwrap()
                    .losses
                    .keys()
                    .cloned()
                    .collect()
            });
        ends.push((what, holds(&m), losses, everything(&m).iter().any(|e| e.stamp == l6.stamp)));
    }
    for (what, got, losses, appended) in &ends {
        assert_eq!(*got, (true, 1), "pf3 {what}: the collection stands with document 2");
        assert_eq!(
            losses.as_ref(),
            Some(&vec![key.clone()]),
            "pf3 {what}: the loss record is on the document"
        );
        assert!(*appended, "pf3 {what}: and the entry is in the member's oplog");
    }
}

// ---- ph1: a stage created below a database drop (the Redated marker ruling) -----------------------------------------------------------------------------

/// The drop of the whole database by `by`, stamped `wall_ms`.
fn database_drop_entry(by: &Engine, wall_ms: u64) -> OplogEntry {
    crate::engine::ddl_entry(
        Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        OpKind::DropCollection,
        kimmy_core::database_drop_id(DB),
        &CollectionRef::new(DB, kimmy_core::DATABASE_DROP_NAME),
    )
    .expect("the database drop encodes")
}

/// The register events of `orders` that are a logical drop (a drop of the name, with no physical).
fn logical_drops(m: &Engine) -> usize {
    let txn = m.db().begin_read().unwrap();
    crate::register::timeline_read(&txn, DB, "orders")
        .unwrap()
        .events()
        .iter()
        .filter(|(_, event)| event.physical().is_none())
        .count()
}

/// ph1, a GUARD (hardproblems' 2:37 PM ruling): a member holds `orders` and a stage of it (`orders.__stg.h1`) created BELOW a database drop D (the author's creation is stamped before D). The stage's creation and D reach it in both orders, and the two orders end ALIKE: the same logical
/// drops in the register, the same own tombstone for `orders`, the same standing of `orders` and of the stage. The EXACT COUNT of logical drops is not asserted: a stage creation that arrives after D is refused under D, so the count is 0 as built. Expected GREEN today. Mutant: a seed made only
/// when the stage's creation arrives before D (the orders then differ).
#[test]
fn ph1_a_stage_created_below_a_database_drop_leaves_both_orders_alike() {
    let (x, _dx) = engine();
    let stage = "orders.__stg.h1";
    x.create_stage(DB, stage, MergePolicy::Document, Hlc::new(physical_now_ms() + 60_000, 0))
        .expect("the stage");
    let sc = everything(&x)
        .into_iter()
        .find(|e| e.kind == OpKind::CreateCollection && e.collection == id_of(stage))
        .expect("the stage's creation");
    let (y, _dy) = engine();
    let d = database_drop_entry(&y, sc.stamp.hlc.wall_ms + 10_000);
    assert!(sc.stamp.hlc < d.stamp.hlc, "premise: the stage was created below the database's drop");
    let mut ends = Vec::new();
    for (what, order) in [("the stage first", [&sc, &d]), ("the drop first", [&d, &sc])] {
        let (m, _dm) = engine();
        let orders = m.create_collection(DB, "orders").unwrap();
        m.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
        for entry in order {
            window(&m, std::slice::from_ref(entry));
        }
        ends.push((
            what,
            logical_drops(&m),
            m.collection_dropped_at(id_of("orders")).unwrap(),
            m.get_collection(DB, "orders").is_ok(),
            m.get_collection(DB, stage).is_ok(),
        ));
    }
    let (first, second) = (&ends[0], &ends[1]);
    assert_eq!(
        (first.1, first.2, first.3, first.4),
        (second.1, second.2, second.3, second.4),
        "ph1: the two orders end alike ({first:?} against {second:?})"
    );
}

// ---- ph2: the Redated marker's case: W, D, sc, B in two orders --------------------------------------------------------------------------------------------

/// A binding of `physical` to the name `orders`, by `by`, stamped `wall_ms`.
fn bind_entry(by: &Engine, wall_ms: u64, physical: &str) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        kind: OpKind::BindCollection,
        collection: id_of("orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionBind {
                db: DB.into(),
                name: "orders".into(),
                physical: physical.into(),
            })
            .expect("the binding encodes"),
        ),
        meta: None,
    }
}

/// What the register, the tombstones and the catalogue hold of `orders` and of the stage, in a form two members can be compared by.
#[derive(Debug, PartialEq)]
struct Standing {
    events: Vec<(String, Option<String>)>,
    own_tombstone: Option<Stamp>,
    orders_stands: bool,
    orders_redated: Option<bool>,
    stage_stands: bool,
}

fn standing(m: &Engine, stage: &str) -> Standing {
    let txn = m.db().begin_read().unwrap();
    let events = crate::register::timeline_read(&txn, DB, "orders")
        .unwrap()
        .events()
        .iter()
        .map(|(at, event)| (format!("{at:?}"), event.physical().map(|p| p.to_string())))
        .collect();
    let orders = m.get_collection(DB, "orders").ok();
    Standing {
        events,
        own_tombstone: m.collection_dropped_at(id_of("orders")).unwrap(),
        orders_stands: orders.is_some(),
        orders_redated: orders.map(|c| c.is_redated()),
        stage_stands: m.get_collection(DB, stage).is_ok(),
    }
}

/// ph2: the case the Redated ruling is about. `orders` is held (with its first document); a stage of it is created at sc; a database drop D is stamped above sc; a write W is made above D by a member that had not heard D; a binding B of the stage is stamped above D and inside the stage's
/// deadline. ONE member takes sc, D, W and B in two orders: "W, D, sc, B" (the write meets a standing collection, then D re-dates its life) and "D, W, sc, B" (D buries it, then W remakes it); each from a fresh member that holds `orders`. Both end ALIKE in the register's events for `orders`,
/// the own tombstone, `is_redated()` and whether `orders` and the stage stand. The count of logical drops is NOT asserted (if B answers UnknownStage because sc was refused under D it is 0 in both; the row still guards "alike"). Expected GREEN today (hardproblems). Installed with P7b.
/// Mutant (P7b): the remake sets no Redated marker (the "D, W, sc, B" member then differs from the re-dated one on `is_redated()`).
#[test]
fn ph2_a_write_above_a_database_drop_and_a_binding_of_a_stage_end_alike_re_dated_or_remade() {
    let stage = "orders.__stg.h2";
    let (y, _dy) = engine();
    let orders = y.create_collection(DB, "orders").unwrap();
    y.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    let prefix = everything(&y);
    let (x, _dx) = engine();
    x.create_stage(DB, stage, MergePolicy::Document, Hlc::new(physical_now_ms() + 60_000, 0))
        .expect("the stage");
    let sc = everything(&x)
        .into_iter()
        .find(|e| e.kind == OpKind::CreateCollection && e.collection == id_of(stage))
        .expect("the stage's creation");
    let (z, _dz) = engine();
    let d = database_drop_entry(&z, sc.stamp.hlc.wall_ms + 10);
    std::thread::sleep(std::time::Duration::from_millis(30));
    y.insert(&orders, doc! { "_id": 2, "v": "above the drop" }).unwrap();
    let w = everything(&y)
        .into_iter()
        .find(|e| e.doc_id == Some(DocId::Int64(2)) && e.collection == id_of("orders"))
        .expect("the write");
    let b = bind_entry(&x, w.stamp.hlc.wall_ms + 10, stage);
    assert!(
        sc.stamp.hlc < d.stamp.hlc && d.stamp.hlc < w.stamp.hlc && w.stamp.hlc < b.stamp.hlc,
        "premise: sc < D < W < B"
    );
    assert!(
        b.stamp.hlc.wall_ms < physical_now_ms() + 60_000,
        "premise: B is inside the stage's deadline"
    );
    let mut ends = Vec::new();
    for (what, order) in [("W, D, sc, B", [&w, &d, &sc, &b]), ("D, W, sc, B", [&d, &w, &sc, &b])] {
        let (m, _dm) = engine();
        for entry in &prefix {
            window(&m, std::slice::from_ref(entry));
        }
        assert!(
            m.get_collection(DB, "orders").is_ok(),
            "premise: the member holds orders before the facts"
        );
        for entry in order {
            window(&m, std::slice::from_ref(entry));
        }
        ends.push((what, standing(&m, stage)));
    }
    assert_eq!(
        ends[0].1, ends[1].1,
        "ph2: the two orders end alike ({} against {}): {:?} against {:?}",
        ends[0].0, ends[1].0, ends[0].1, ends[1].1
    );
}

// ---- pg1: the later-life stop, the one stop left after P7b ------------------------------------------------------------------------------------------------

/// pg1 (hardproblems' 3:33 PM): a definition above a tombstone, at a member that holds the drop and NO options row for the id (it never saw the collection's creation, so `memo_gone_plan` has no row to make it from). The sender `a` holds the collection standing and creates the index `x`
/// above the drop (`z` took the collection and dropped it, `a` had not heard). The member takes the drop, then the `CreateIndex`: the window STOPS at it (`unknown_collection`; the transport plans the scoped fetch from that stop), and nothing is made by the stop. After the fetch (a scoped
/// page of `a`'s collection: its creation is at or below the floor; it carries the options row AND the author's index definition stamped above the floor, so the page's own `restore_collection` remakes the collection from that definition, P7b's page site (2)) the retried entry is NOT stopped (applied or superseded, whichever the code does) and: the collection is held, dated just above the floor WITH the Redated marker (a FACT above the floor, the row's `at` at or below it: the drop's remains, per the 3:33 PM dating split), with the index, and the window is not stopped. The CreateIndex variant of
/// "CreateIndex or ConfigureValidator". Expected GREEN at P7b. Mutants (P7b): the definition judged history at the first window (the stop removed); the remake made without a row (the first window does not stop); the retry stopped again after the page (the row is not read).
#[test]
fn pg1_a_definition_above_a_tombstone_stops_a_member_with_no_row_and_is_applied_after_the_fetch() {
    let (a, _da) = engine();
    let orders = a.create_collection(DB, "orders").unwrap();
    a.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    let (z, _dz) = engine();
    window(&z, &everything(&a));
    pause();
    assert!(z.drop_collection(DB, "orders").unwrap());
    let d3 = own(&z, OpKind::DropCollection).pop().expect("the drop");
    pause();
    a.create_index(DB, "orders", vec![IndexField::ascending("v")], false, Some("x".into()))
        .unwrap();
    let i = own(&a, OpKind::CreateIndex).pop().expect("the index creation");
    assert!(d3.stamp.hlc < i.stamp.hlc, "premise: the CreateIndex is stamped above the drop");

    let (m, _dm) = engine();
    window(&m, std::slice::from_ref(&d3));
    assert_eq!(
        m.collection_dropped_at(id_of("orders")).unwrap(),
        Some(d3.stamp),
        "premise: the member holds the drop"
    );
    assert!(m.get_collection(DB, "orders").is_err(), "premise: and no collection");
    assert!(options_of(&m).is_none(), "premise: and no options row for the id");

    let first = window(&m, std::slice::from_ref(&i));
    assert!(
        first.unknown_collection > 0,
        "pg1: the window stops at a definition above the tombstone when no row can make the collection: {first:?}"
    );
    if let Some(stopped) = &first.unknown {
        assert_eq!(
            stopped.id,
            id_of("orders"),
            "pg1: and the stop names the collection the fetch is for"
        );
    }
    assert!(m.get_collection(DB, "orders").is_err(), "pg1: nothing is made before the fetch");

    // the fetch: the scoped page of `a`'s standing collection
    let mut progress = SnapshotProgress::of_collection(id_of("orders"));
    let page = a.snapshot_page(None, Some(id_of("orders"))).unwrap();
    m.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    let row = options_of(&m).expect("premise: the page carried the options row");
    assert!(
        row.at <= d3.stamp.hlc,
        "premise: the row's `at` is at or below the drop (the creation was before it), so only the FACT can make the collection"
    );
    // The page itself may make the collection: it carries the author's index definition, stamped above the floor, so `restore_collection` remakes from it (P7b's page site (2), correct under the rulings). Not asserted either way.

    let retry = window(&m, std::slice::from_ref(&i));
    assert_eq!(
        retry.unknown_collection, 0,
        "pg1: after the fetch the window is not stopped: {retry:?}"
    );
    let held = m.get_collection(DB, "orders").expect("pg1: the collection is held after the fetch");
    assert_eq!(
        held.created,
        d3.stamp.hlc.successor(),
        "pg1: dated just above the floor: the drop's remains"
    );
    assert!(
        held.is_redated(),
        "pg1: with the Redated marker set (made by a FACT above the floor, the row's `at` at or below it)"
    );
    assert!(held.index("x").is_some(), "pg1: and the definition that made it is applied");
}
