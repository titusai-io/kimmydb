//! The names table and the base options (ADR-272; unit N): every site that writes them, and the one rule that orders the options.
//!
//! The writer sites, and the test that covers each:
//! - `register::store` (an event in effect, parked): the first two tests;
//! - `register::write_stage_row`: the third;
//! - `register::apply_carried` (the carry's stage rows): the join test;
//! - `create_collection_in_txn` (a local creation): the local-creation test;
//! - `note_creation_seen`, from the entries route (history under a drop; a standing collection) and from the page route (the policy stamp,
//!   not `created`; a stamp the sender's coverage does not name): the entries and page tests;
//! - the shadow a replicated vector configuration makes (`configure_vectors_inner`, the one creation from elsewhere that passes neither
//!   route top): its own test;
//! - the tie rule: the three order tests.
//!
//! Every row is read back from the table as stored (`carried_rows`) and decoded HERE, by the layout the table documents, not by the
//! product's own decoder, so a writer that wrote a wrong shape fails on the test's reading and not on its own.

use kimmy_core::{
    CollectionCreate, CollectionId, Epoch, Hlc, MergePolicy, NodeId, OpKind, OplogEntry,
    RegisterEvent, Stamp,
};
use redb::ReadableDatabase;

use crate::register::{BaseOptions, Carried};
use crate::snapshot::Carry;
use crate::{Engine, SnapshotProgress, WalkScope, WriterHolder, physical_now_ms};

const DB: &str = "app";
const STAGE: &str = "orders.__stg.a1";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn st(hlc: u64, origin: u8) -> Stamp {
    Stamp::new(Hlc::new(hlc, 0), NodeId::from_bytes([origin; 16]), Epoch::ZERO)
}

/// Run `f` in a write transaction of the engine and commit it.
fn in_txn<T>(e: &Engine, f: impl FnOnce(&redb::WriteTransaction) -> T) -> T {
    let txn = e.begin_write(WriterHolder::Ddl).unwrap();
    let out = f(&txn);
    txn.commit().unwrap();
    out
}

fn rows(e: &Engine, which: Carried) -> Vec<(Vec<u8>, Vec<u8>)> {
    let txn = e.db().begin_read().unwrap();
    crate::register::carried_rows(&txn, which).unwrap()
}

fn id_key(db: &str, name: &str) -> Vec<u8> {
    CollectionId::derive(db, name).0.to_be_bytes().to_vec()
}

/// What the names row of `physical` says, as `(database, logical, physical)`, decoded from its stored bytes by the table's documented
/// layout: `db length (u16, big-endian) || db || logical length (u16) || logical || physical`.
fn names_of(e: &Engine, physical: &str) -> Option<(String, String, String)> {
    let key = id_key(DB, physical);
    let (_, value) = rows(e, Carried::Names).into_iter().find(|(k, _)| *k == key)?;
    let text = |bytes: &[u8]| String::from_utf8(bytes.to_vec()).unwrap();
    let db_len = u16::from_be_bytes([value[0], value[1]]) as usize;
    let logical_at = 2 + db_len;
    let logical_len = u16::from_be_bytes([value[logical_at], value[logical_at + 1]]) as usize;
    let physical_at = logical_at + 2 + logical_len;
    Some((
        text(&value[2..logical_at]),
        text(&value[logical_at + 2..physical_at]),
        text(&value[physical_at..]),
    ))
}

/// The options row of the collection `name`, if the table holds one.
fn options_of(e: &Engine, name: &str) -> Option<BaseOptions> {
    let key = id_key(DB, name);
    let (_, value) = rows(e, Carried::Options).into_iter().find(|(k, _)| *k == key)?;
    Some(serde_json::from_slice(&value).unwrap())
}

fn named(physical: &str, logical: &str) -> Option<(String, String, String)> {
    Some((DB.to_string(), logical.to_string(), physical.to_string()))
}

// -- `register::store`: an event in effect and an event ahead of its position ---------------------------------------------------------------

#[test]
fn an_event_in_effect_names_its_identity_and_a_recreations_physical() {
    let (e, _dir) = engine();
    assert!(names_of(&e, "orders").is_none(), "premise: nothing is named before the event");
    in_txn(&e, |txn| {
        crate::register::put(
            txn,
            DB,
            "orders",
            st(5, 1),
            RegisterEvent::Create { physical: "orders.r1".into() },
        )
        .unwrap();
    });
    assert_eq!(names_of(&e, "orders"), named("orders", "orders"), "the identity: its first row");
    assert_eq!(names_of(&e, "orders.r1"), named("orders.r1", "orders"), "and the recreation's");
}

#[test]
fn an_event_known_only_ahead_of_its_position_names_the_same_things() {
    let (e, _dir) = engine();
    in_txn(&e, |txn| {
        crate::register::park(
            txn,
            DB,
            "orders",
            st(5, 1),
            RegisterEvent::Create { physical: "orders.r1".into() },
        )
        .unwrap();
    });
    assert_eq!(
        names_of(&e, "orders"),
        named("orders", "orders"),
        "a name whose only row is parked"
    );
    assert_eq!(names_of(&e, "orders.r1"), named("orders.r1", "orders"));
}

// -- `register::write_stage_row` ------------------------------------------------------------------------------------------------------------

#[test]
fn a_stage_creation_names_the_stage_and_its_identity() {
    let (e, _dir) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    e.create_stage(DB, STAGE, MergePolicy::Document, deadline).unwrap();
    assert_eq!(names_of(&e, STAGE), named(STAGE, "orders"), "the stage's own physical");
    assert_eq!(names_of(&e, "orders"), named("orders", "orders"), "and the identity it belongs to");
}

#[test]
fn a_second_write_of_a_names_row_changes_nothing() {
    let (e, _dir) = engine();
    in_txn(&e, |txn| {
        assert!(crate::register::note_names(txn, DB, "orders", "orders").unwrap(), "first write");
        assert!(!crate::register::note_names(txn, DB, "orders", "orders").unwrap(), "no-op");
    });
    assert_eq!(rows(&e, Carried::Names).len(), 1, "one row");
}

// -- `register::apply_carried`: a join names the carry's stage rows ---------------------------------------------------------------------------

/// The page carries a stage row and the member's own definition of the stage is withheld, so that the only route by which this member can
/// learn the stage's names is the carry's stage row.
#[test]
fn a_join_names_the_stage_and_its_identity_from_the_carry_alone() {
    let (a, _da) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE, MergePolicy::Document, deadline).unwrap();
    let mut page = a.snapshot_page(None, None).unwrap();
    assert!(page.register.is_some(), "premise: the page carries the register");
    page.collections.retain(|state| state.stage.is_none());
    let (b, _db) = engine();
    assert!(names_of(&b, STAGE).is_none(), "premise: the joiner names nothing yet");
    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page)
        .expect("the page applies");
    assert_eq!(
        names_of(&b, STAGE),
        named(STAGE, "orders"),
        "the carry's stage row names the stage"
    );
    assert_eq!(names_of(&b, "orders"), named("orders", "orders"), "and its identity");
}

// -- `create_collection_in_txn`: a local creation ----------------------------------------------------------------------------------------

#[test]
fn a_local_creation_writes_the_options_row_of_its_collection() {
    let (e, _dir) = engine();
    assert!(options_of(&e, "orders").is_none(), "premise: no row before the creation");
    let meta = e.create_collection(DB, "orders").unwrap();
    assert_eq!(
        options_of(&e, "orders"),
        Some(BaseOptions {
            db: DB.to_string(),
            name: "orders".to_string(),
            at: meta.merge_policy_at.unwrap_or(meta.created),
            merge_policy: MergePolicy::Document,
            join_retention_ms: None,
            loss_limits: None,
            stage: None,
        }),
        "the creation's own Hlc and options, with the names of the creation"
    );
}

// -- The tie rule: the order of the options row, and that it does not depend on the order the creations were seen in -------------------------

fn options(at_ms: u64, policy: MergePolicy, r: Option<u64>) -> BaseOptions {
    BaseOptions {
        db: DB.to_string(),
        name: "orders".to_string(),
        at: Hlc::new(at_ms, 0),
        merge_policy: policy,
        join_retention_ms: r,
        loss_limits: None,
        stage: None,
    }
}

/// The row a fresh member ends with after `list` is noted in the order given.
fn after(list: &[&BaseOptions]) -> BaseOptions {
    let (e, _dir) = engine();
    for one in list {
        in_txn(&e, |txn| {
            crate::register::note_base_options(txn, one).unwrap();
        });
    }
    options_of(&e, "orders").expect("a row")
}

#[test]
fn the_options_row_rises_to_the_later_creation_in_either_order() {
    let (early, late) =
        (options(10, MergePolicy::Document, None), options(20, MergePolicy::Document, None));
    assert_eq!(after(&[&early, &late]), late, "the later one arrives second");
    assert_eq!(
        after(&[&late, &early]),
        late,
        "the later one arrives first: the older never replaces it"
    );
}

#[test]
fn an_equal_hlc_is_settled_by_the_policy_text_in_either_order() {
    let (document, field) =
        (options(10, MergePolicy::Document, None), options(10, MergePolicy::Field, Some(1)));
    // "field" sorts after "document"
    assert_eq!(after(&[&document, &field]), field);
    assert_eq!(after(&[&field, &document]), field);
}

/// An equal Hlc and policy with another `R` is settled by the greater encoding of the whole row. The encoding is JSON, so numbers compare
/// as TEXT: 9 is greater than 10. That is deliberate (every member compares the same bytes, and the order only has to be total); the
/// test pins it so that nobody makes the comparison numeric on one member.
#[test]
fn an_equal_hlc_and_policy_with_another_r_is_settled_by_the_greater_encoding_as_text() {
    let (nine, ten) =
        (options(10, MergePolicy::Field, Some(9)), options(10, MergePolicy::Field, Some(10)));
    assert_eq!(after(&[&nine, &ten]).join_retention_ms, Some(9), "9 over 10, as text");
    assert_eq!(after(&[&ten, &nine]).join_retention_ms, Some(9), "in either order");
}

#[test]
fn a_creation_keeps_r_and_k_only_for_a_field_collection() {
    let document = BaseOptions::of_creation(
        DB,
        "orders",
        Hlc::new(1, 0),
        MergePolicy::Document,
        Some(5),
        None,
        None,
    );
    assert_eq!(document.join_retention_ms, None, "a document collection has none");
    let field = BaseOptions::of_creation(
        DB,
        "orders",
        Hlc::new(1, 0),
        MergePolicy::Field,
        Some(5),
        None,
        None,
    );
    assert_eq!(field.join_retention_ms, Some(5), "a field collection keeps the creator's R");
}

// -- `note_creation_seen`, from the entries route ------------------------------------------------------------------------------------------

/// A `CreateCollection` entry for `orders` by `by` at `wall_ms`.
fn creation(by: &Engine, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        kind: OpKind::CreateCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionCreate {
                db: DB.into(),
                name: "orders".into(),
                merge_policy: MergePolicy::Document,
                validator: None,
                join_retention_ms: None,
                loss_limits: None,
                stage: None,
            })
            .expect("the creation encodes"),
        ),
        meta: None,
    }
}

fn drop_entry(by: &Engine, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        kind: OpKind::DropCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&kimmy_core::CollectionRef::new(DB, "orders"))
                .expect("the drop encodes"),
        ),
        meta: None,
    }
}

fn applied_in_order(entries: &[&OplogEntry]) -> (Engine, tempfile::TempDir) {
    let (b, dir) = engine();
    for entry in entries {
        b.apply_batch(std::slice::from_ref(*entry)).expect("the entry applies");
    }
    (b, dir)
}

/// A creation judged history under a drop makes no collection and still raises the row: it was SEEN. The row names the LATEST creation the
/// member has met, not the latest it applied.
#[test]
fn a_creation_that_is_history_under_a_drop_still_raises_the_options_row() {
    let base = physical_now_ms();
    let (a, _da) = engine();
    let (c, _dc) = engine();
    let first = creation(&a, base - 3_000);
    let dropped = drop_entry(&a, base - 2_000);
    // below the drop, so history, and above the first creation, so it rises
    let late = creation(&c, base - 2_500);
    let (b, _db) = applied_in_order(&[&first, &dropped, &late]);
    assert!(
        b.get_collection(DB, "orders").is_err(),
        "premise: the drop buried it and the late creation is history"
    );
    let row = options_of(&b, "orders").expect("the creations were seen");
    assert_eq!(
        row.at,
        Hlc::new(base - 2_500, 0),
        "the row rose to the creation that was judged history"
    );
}

/// A creation that meets a collection already standing raises the row too, whichever order the two creations arrive in.
#[test]
fn a_creation_met_by_a_standing_collection_raises_the_row_in_either_order() {
    let base = physical_now_ms();
    let (a, _da) = engine();
    let (c, _dc) = engine();
    let earlier = creation(&a, base - 3_000);
    let later = creation(&c, base - 1_000);
    let (forward, _d1) = applied_in_order(&[&earlier, &later]);
    assert!(forward.get_collection(DB, "orders").is_ok(), "premise: the collection stands");
    assert_eq!(
        options_of(&forward, "orders").unwrap().at,
        Hlc::new(base - 1_000, 0),
        "later second"
    );
    let (backward, _d2) = applied_in_order(&[&later, &earlier]);
    assert_eq!(
        options_of(&backward, "orders").unwrap().at,
        Hlc::new(base - 1_000, 0),
        "later first"
    );
}

// -- `note_creation_seen`, from the page route ---------------------------------------------------------------------------------------------

/// The page's creation stamp is `created`, which sits too low after a later creation and too high after a re-dated life; the row's Hlc is
/// the policy stamp (`merge_policy_at`, else `created`). The page here names a policy stamp BELOW `created` and inside the sender's
/// coverage: the row must carry it, and never `created`.
#[test]
fn a_page_raises_the_row_with_its_policy_stamp_and_not_created() {
    let (a, _da) = engine();
    a.create_collection(DB, "orders").unwrap();
    let mut page = a.snapshot_page(None, None).unwrap();
    let state = page
        .collections
        .iter_mut()
        .find(|state| state.name == "orders")
        .expect("premise: the page defines orders");
    let created = state.created;
    let lower = Hlc::new(created.wall_ms - 5, 0);
    state.merge_policy_at = Some(lower);
    // the sender's own row rides the page's carry and would rise over this one (the carry has its own tests below): this test is of the
    // definition's route alone
    page.carried.clear();
    let (b, _db) = engine();
    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page)
        .expect("the page applies");
    let row = options_of(&b, "orders").expect("the page's creation was seen");
    assert_eq!(row.at, lower, "the policy stamp");
    assert_ne!(row.at, created, "and not created");
}

/// A policy stamp the sender's coverage does not name raises nothing, as it adopts nothing (ADR-163).
#[test]
fn a_page_whose_coverage_does_not_name_the_policy_stamp_raises_no_row() {
    let (a, _da) = engine();
    a.create_collection(DB, "orders").unwrap();
    let mut page = a.snapshot_page(None, None).unwrap();
    let state = page
        .collections
        .iter_mut()
        .find(|state| state.name == "orders")
        .expect("premise: the page defines orders");
    state.merge_policy_at = Some(Hlc::new(state.created.wall_ms + 60_000, 0));
    // the definition's route alone: the sender's own row rides the carry (tested below) and would be held
    page.carried.clear();
    let (b, _db) = engine();
    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page)
        .expect("the page applies");
    assert!(b.get_collection(DB, "orders").is_ok(), "premise: the collection was made");
    assert!(options_of(&b, "orders").is_none(), "no row: the stamp is above what the sender named");
}

// -- The shadow a replicated vector configuration makes: the one creation from elsewhere that passes neither route's top ----------------------

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
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

/// The shadow's own `CreateCollection` entry is withheld, so the configuration entry is the ONLY way the receiver learns of the shadow:
/// it makes the shadow through `create_collection_in_txn` with the entry's stamp as its origin, and writes the shadow's options row at
/// that site. The row's `Hlc` is the CONSTANT zero of a vector shadow, and not the configuration entry's: a shadow has no facts of its own.
#[test]
fn a_replicated_vector_configuration_writes_the_options_row_of_the_shadow_it_makes() {
    let (a, _da) = engine();
    a.create_collection(DB, "orders").unwrap();
    a.configure_vectors(DB, "orders", vector_config()).unwrap();
    let shadow = kimmy_core::vector_meta::shadow_name("orders");
    let shadow_id = CollectionId::derive(DB, &shadow);
    let entries: Vec<OplogEntry> = everything(&a)
        .into_iter()
        .filter(|e| !(e.kind == OpKind::CreateCollection && e.collection == shadow_id))
        .collect();
    assert!(
        entries.iter().any(|e| e.kind == OpKind::ConfigureVectors),
        "premise: the configuration's entry is delivered"
    );
    let (b, _db) = engine();
    b.apply_batch(&entries).expect("the entries apply");
    assert!(b.get_collection(DB, &shadow).is_ok(), "premise: the configuration made the shadow");
    let row = options_of(&b, &shadow).expect("the shadow's creation was seen at that site");
    assert_eq!(row.at, Hlc::ZERO, "a constant Hlc: a shadow has no facts of its own");
    assert_eq!(row.merge_policy, MergePolicy::Document, "a shadow is a document collection");
    assert_eq!(
        (row.db.as_str(), row.name.as_str()),
        (DB, shadow.as_str()),
        "and names its collection"
    );
}

/// The stored bytes of the options row of `name`, which two members hold the same row by when the VALUES are equal.
fn options_bytes(e: &Engine, name: &str) -> Option<Vec<u8>> {
    let key = id_key(DB, name);
    rows(e, Carried::Options).into_iter().find(|(k, _)| *k == key).map(|(_, value)| value)
}

/// A vector shadow has no facts of its own: whichever route made it, the options row is the same BYTES on every member. The owner makes it
/// by its own configuration (a local creation), an entry-fed member by the shadow's creation entry, a joiner by a page, and a member that
/// is given only the configuration entries makes it from the configuration. After TWO configurations a peer's row must not have risen
/// above the owner's, which the owner's local path never writes again.
#[test]
fn every_route_leaves_a_vector_shadows_options_row_byte_identical_after_two_configurations() {
    let (a, _da) = engine();
    a.create_collection(DB, "orders").unwrap();
    a.configure_vectors(DB, "orders", vector_config()).unwrap();
    let second = kimmy_core::VectorConfig { fields: vec!["body".into()], ..vector_config() };
    a.configure_vectors(DB, "orders", second).unwrap();
    let shadow = kimmy_core::vector_meta::shadow_name("orders");
    let shadow_id = CollectionId::derive(DB, &shadow);

    let owner = options_bytes(&a, &shadow).expect("premise: the owner holds the shadow's row");
    let row: BaseOptions = serde_json::from_slice(&owner).unwrap();
    assert_eq!(row.at, Hlc::ZERO, "premise: the owner's row is the constant");

    let all = everything(&a);
    let (fed, _d1) = engine();
    fed.apply_batch(&all).expect("the entry-fed member applies every entry");
    assert_eq!(options_bytes(&fed, &shadow), Some(owner.clone()), "entry-fed");

    let without_the_shadows_creation: Vec<OplogEntry> = all
        .into_iter()
        .filter(|e| !(e.kind == OpKind::CreateCollection && e.collection == shadow_id))
        .collect();
    let (made, _d2) = engine();
    made.apply_batch(&without_the_shadows_creation).expect("the configuration makes the shadow");
    assert!(
        made.get_collection(DB, &shadow).is_ok(),
        "premise: the shadow was made from the entry"
    );
    assert_eq!(options_bytes(&made, &shadow), Some(owner.clone()), "made from a configuration");

    let page = a.snapshot_page(None, None).unwrap();
    let (joined, _d3) = engine();
    joined
        .apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page)
        .expect("the page applies");
    assert!(joined.get_collection(DB, &shadow).is_ok(), "premise: the page made the shadow");
    assert_eq!(options_bytes(&joined, &shadow), Some(owner), "joined by a page");
}

// -- The carry: the names table and the base options ride the first pages of a whole-database snapshot, before the documents -------------------

/// A sender with `n` collections `c0`.. in the database, and one document in the first of them.
fn a_sender_with(n: usize) -> (Engine, tempfile::TempDir) {
    let (a, dir) = engine();
    for i in 0..n {
        let meta = a.create_collection(DB, &format!("c{i}")).unwrap();
        if i == 0 {
            a.insert(&meta, bson::doc! { "_id": 1, "v": "one" }).unwrap();
        }
    }
    (a, dir)
}

/// A sender with a live collection `c0` holding a document, and `n` collections that existed and were dropped: the options of a gone
/// collection are a row only the carry brings (a live one's arrives as the same row from its definition, so it is no news to a joiner and
/// costs it no write).
fn a_sender_with_ghosts(n: usize) -> (Engine, tempfile::TempDir) {
    let (a, dir) = a_sender_with(1);
    for i in 0..n {
        let name = format!("g{i}");
        a.create_collection(DB, &name).unwrap();
        a.drop_collection(DB, &name).unwrap();
    }
    (a, dir)
}

/// Every page of the pull `progress` is of, from `from` into `to`, the way the transport asks for them: the cursor the progress holds is
/// the one the next page is asked after.
fn pulled(to: &Engine, from: &Engine, progress: &mut SnapshotProgress) -> Vec<crate::SnapshotPage> {
    let mut pages = Vec::new();
    while !progress.is_complete() {
        assert!(pages.len() < 100, "the pull does not end");
        let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        to.apply_snapshot_page(from.node_id(), progress, &page).expect("the page applies");
        pages.push(page);
    }
    pages
}

/// A page budget that is small against the carry and still holds one document: the larger of three options rows (a row costs its key, its
/// value and the carried row's frame) and the first document with the cursor a page cuts after it. Measured from the sender, so that a change of
/// the layout or of the format moves the budget with it and not the test.
fn a_small_room_that_holds_a_document(e: &Engine) -> usize {
    let row = rows(e, Carried::Options)
        .iter()
        .map(|(key, value)| key.len() + value.len() + crate::snapshot::CARRIED_ROW_FRAME)
        .max()
        .expect("premise: the sender has an options row");
    let whole = e.snapshot_page(None, None).unwrap();
    let document = whole.documents.first().expect("premise: the sender has a document");
    let document = document.body.as_ref().map_or(0, Vec::len)
        + crate::snapshot::id_wire_len(&document.id)
        + crate::snapshot::DOCUMENT_OVERHEAD
        + crate::snapshot::cursor_reserve(8, 0);
    (3 * row + 10).max(document + 16)
}

/// The keys of the rows of one carried table, sorted.
fn keys(e: &Engine, which: Carried) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = rows(e, which).into_iter().map(|(key, _)| key).collect();
    keys.sort();
    keys
}

/// A whole-database page that holds the whole database carries the options of every collection, and the joiner ends holding them: the
/// rows as the sender stores them, not rows it made from the definitions.
#[test]
fn a_whole_database_page_carries_the_options_of_every_collection() {
    let (a, _da) = a_sender_with(3);
    let page = a.snapshot_page(None, None).unwrap();
    assert!(page.next.is_none(), "premise: the whole database is one page");
    let carried: Vec<Vec<u8>> = {
        let mut keys: Vec<Vec<u8>> = page
            .carried
            .iter()
            .filter(|row| row.table == crate::CARRY_OPTIONS)
            .map(|row| row.key.to_vec())
            .collect();
        keys.sort();
        keys
    };
    assert_eq!(carried, keys(&a, Carried::Options), "every options row of the sender, once");
    assert_eq!(carried.len(), 3, "premise: three collections, three rows");
    let (b, _db) = engine();
    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page)
        .expect("the page applies");
    assert_eq!(rows(&b, Carried::Options), rows(&a, Carried::Options), "the rows as stored");
}

/// With a room of a few rows, a carry of thirty is several pages before the documents: the first page keeps the definitions, no page
/// of the carry holds a document, the page that ends the carry names where the documents begin, and the join ends holding every row and
/// the document, with the progress complete only after the documents.
#[test]
fn a_carry_that_does_not_fit_the_first_page_goes_on_pages_before_the_documents() {
    let (a, _da) = a_sender_with(30);
    a.set_snapshot_room_for_test(Some(a_small_room_that_holds_a_document(&a)));
    let (b, _db) = engine();
    let mut progress = SnapshotProgress::whole_database();
    let pages = pulled(&b, &a, &mut progress);
    assert!(pages.len() >= 5, "premise: the carry took several pages and then the documents");
    let first = &pages[0];
    assert!(!first.collections.is_empty(), "the first page keeps the definitions");
    assert!(first.documents.is_empty(), "no document before the carry is done");
    assert!(
        first.next.as_ref().is_some_and(|cursor| matches!(cursor.carry, Carry::At(_))),
        "its cursor says the carry has rows left"
    );
    let carry_pages = pages.iter().take_while(|page| !page.carried.is_empty()).count();
    assert!(carry_pages >= 4, "thirty rows, a few a page: {carry_pages} pages carried rows");
    for page in &pages[1..carry_pages] {
        assert!(page.collections.is_empty(), "the definitions are on the first page only");
        assert!(page.documents.is_empty(), "no document on a page of the carry");
    }
    let last_of_the_carry = &pages[carry_pages - 1];
    assert!(
        last_of_the_carry.next.as_ref().is_some_and(|cursor| cursor.carry == Carry::Done),
        "the page that ends the carry is not the last, and its cursor has no carry"
    );
    let total: usize = pages.iter().map(|page| page.carried.len()).sum();
    assert_eq!(total, rows(&a, Carried::Options).len() + rows(&a, Carried::Names).len());
    assert!(progress.is_complete(), "complete after the documents");
    assert_eq!(keys(&b, Carried::Options), keys(&a, Carried::Options), "every row arrived");
    let received = b.get_collection(DB, "c0").unwrap();
    assert_eq!(b.count(&received, WalkScope::Request).unwrap(), 1, "and the document");
}

/// A join that stops between two pages of the carry resumes from the progress it stored, which holds the carry's cursor and the rows
/// the pages before it staged: it ends with every row, and not with the rows of the pages it asked for again only.
#[test]
fn a_join_stopped_in_the_carry_resumes_from_its_stored_progress() {
    let (a, _da) = a_sender_with_ghosts(30);
    a.set_snapshot_room_for_test(Some(a_small_room_that_holds_a_document(&a)));
    let (b, dir) = engine();
    let mut progress = SnapshotProgress::whole_database();
    for _ in 0..2 {
        let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    }
    assert!(
        progress.after().is_some_and(|cursor| matches!(cursor.carry, Carry::At(_))),
        "premise: stopped in the carry"
    );
    // a restart: the member opens its store again and takes what it stored
    drop(b);
    let b = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    let mut resumed = b
        .snapshots_to_resume()
        .unwrap()
        .into_iter()
        .find(|(peer, _)| *peer == a.node_id())
        .map(|(_, stored)| stored)
        .expect("the stopped join was stored");
    assert_eq!(resumed.after(), progress.after(), "the stored cursor is where it stopped");
    pulled(&b, &a, &mut resumed);
    assert_eq!(keys(&b, Carried::Options), keys(&a, Carried::Options), "every row, both halves");
}

/// The staging is keyed by sender: a pull from ANOTHER sender that completes merges none of the rows a first sender's abandoned pull staged
/// (here the options of a collection that is gone, a row only the carry brings), and leaves them where they are until that sender's own
/// pull starts again or is forgotten.
#[test]
fn a_pull_from_another_sender_does_not_merge_the_rows_a_first_sender_staged() {
    let (a, _da) = a_sender_with_ghosts(30);
    a.create_collection(DB, "ghost").unwrap();
    a.drop_collection(DB, "ghost").unwrap();
    assert!(
        options_of(&a, "ghost").is_some(),
        "premise: the options of a gone collection are kept"
    );
    a.set_snapshot_room_for_test(Some(a_small_room_that_holds_a_document(&a)));
    let (b, _db) = engine();
    stopped_after_the_carry(&b, &a);
    let staged = b.carry_stage_rows_for_test(a.node_id());
    assert!(
        staged
            .iter()
            .any(|(table, key, _)| *table == crate::CARRY_OPTIONS && *key == id_key(DB, "ghost")),
        "premise: the abandoned pull staged the ghost's row"
    );
    let (c, _dc) = a_sender_with(2);
    let mut fresh = SnapshotProgress::whole_database();
    pulled(&b, &c, &mut fresh);
    assert!(
        options_of(&b, "ghost").is_none(),
        "what the abandoned pull staged is not merged by the one that completed"
    );
    let held = keys(&b, Carried::Options);
    for key in keys(&c, Carried::Options) {
        assert!(held.contains(&key), "the completed pull's own rows are held");
    }
    // staged by sender, so a pull of another is not the first's to merge or to clear: they stand until that sender's pull restarts or is
    // forgotten
    assert_eq!(
        b.carry_stage_rows_for_test(a.node_id()),
        staged,
        "the first sender's staging is as it was"
    );
}

/// A pull of `from` into `to`, stopped where the carry is done and the documents have not begun: every row of the carry is staged.
fn stopped_after_the_carry(to: &Engine, from: &Engine) -> SnapshotProgress {
    let mut progress = SnapshotProgress::whole_database();
    while progress.after().is_none_or(|cursor| matches!(cursor.carry, Carry::At(_))) {
        let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        to.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
    }
    assert!(!progress.is_complete(), "premise: the pull stopped before the documents");
    progress
}

/// The first page of a pull empties what an EARLIER pull of the same sender left staged, whether the new pull is one page (the page that
/// completes it discards the staged rows and merges its own) or several (its first page replaces the staging). The pages of the second pull
/// are applied under the first sender's node id, which is what makes it the same sender's.
#[test]
fn the_first_page_of_a_pull_empties_what_the_same_sender_left_staged() {
    for pages in ["one page", "several pages"] {
        let (a, _da) = a_sender_with_ghosts(30);
        a.set_snapshot_room_for_test(Some(a_small_room_that_holds_a_document(&a)));
        let (b, _db) = engine();
        stopped_after_the_carry(&b, &a);
        assert!(
            !b.carry_stage_rows_for_test(a.node_id()).is_empty(),
            "{pages}: premise: the abandoned pull staged rows"
        );
        let (c, _dc) = a_sender_with(if pages == "one page" { 2 } else { 12 });
        if pages != "one page" {
            c.set_snapshot_room_for_test(Some(a_small_room_that_holds_a_document(&c)));
        }
        let mut fresh = SnapshotProgress::whole_database();
        while !fresh.is_complete() {
            let page = c.snapshot_page(fresh.after().cloned(), None).unwrap();
            b.apply_snapshot_page(a.node_id(), &mut fresh, &page).unwrap();
        }
        assert!(
            fresh.pages() > 1 || pages == "one page",
            "{pages}: premise: the second pull took several pages"
        );
        assert!(
            options_of(&b, "g0").is_none() && options_of(&b, "g29").is_none(),
            "{pages}: what the abandoned pull staged is not merged by the one that replaced it"
        );
        assert!(
            b.carry_stage_rows_for_test(a.node_id()).is_empty(),
            "{pages}: and the staging is empty"
        );
    }
}

/// Forgetting a pull forgets what it staged.
#[test]
fn forgetting_a_pull_forgets_its_staged_rows() {
    let (a, _da) = a_sender_with_ghosts(30);
    a.set_snapshot_room_for_test(Some(a_small_room_that_holds_a_document(&a)));
    let (b, _db) = engine();
    stopped_after_the_carry(&b, &a);
    assert!(!b.carry_stage_rows_for_test(a.node_id()).is_empty(), "premise: rows are staged");
    b.forget_snapshot_progress(a.node_id(), None).unwrap();
    assert!(b.carry_stage_rows_for_test(a.node_id()).is_empty(), "forgotten with the pull");
}

/// A carried options row whose key does not derive from the names it holds is a damaged page: it fails, the progress does not move, and
/// the row is held nowhere.
#[test]
fn a_carried_row_whose_key_does_not_derive_from_its_names_refuses_the_page() {
    let (a, _da) = a_sender_with(1);
    let mut page = a.snapshot_page(None, None).unwrap();
    let row = page
        .carried
        .iter_mut()
        .find(|row| row.table == crate::CARRY_OPTIONS)
        .expect("premise: the page carries an options row");
    let mut key = row.key.to_vec();
    key[7] ^= 1;
    row.key = serde_bytes::ByteBuf::from(key.clone());
    let (b, _db) = engine();
    let mut progress = SnapshotProgress::whole_database();
    let refused = b.apply_snapshot_page(a.node_id(), &mut progress, &page);
    assert!(refused.is_err(), "the page is refused: {refused:?}");
    assert!(!progress.is_complete() && progress.pages() == 0, "the progress did not move");
    assert!(!keys(&b, Carried::Options).contains(&key), "and the damaged row is held nowhere");
}

/// A member that met two creations of one name with EQUAL options holds the definition's policy stamp at the first (the second changes no
/// option), and the options row at the LATER creation's Hlc. A joiner must hold the later one too, and the definition alone cannot tell it:
/// the carry of the options row does.
#[test]
fn a_joiner_from_a_member_that_met_two_creations_of_equal_options_holds_the_later_at() {
    let base = physical_now_ms();
    let (a, _da) = engine();
    let (c, _dc) = engine();
    let earlier = creation(&a, base - 3_000);
    let later = creation(&c, base - 1_000);
    let (source, _ds) = applied_in_order(&[&earlier, &later]);
    let state = source.get_collection(DB, "orders").expect("premise: the collection stands");
    assert_eq!(
        state.merge_policy_at,
        Some(Hlc::new(base - 3_000, 0)),
        "premise: the definition's policy stamp stayed at the first creation"
    );
    assert_eq!(
        options_of(&source, "orders").unwrap().at,
        Hlc::new(base - 1_000, 0),
        "premise: the options row rose to the later one"
    );
    let (joiner, _dj) = engine();
    pulled(&joiner, &source, &mut SnapshotProgress::whole_database());
    assert_eq!(
        options_of(&joiner, "orders").unwrap().at,
        Hlc::new(base - 1_000, 0),
        "the joiner holds the later creation's Hlc, as the source does"
    );
}

/// The cursor and the page are required on the wire: one that does not name its carry is malformed, and so is a stored progress written
/// before the carry existed (an undecodable record reads as none, and the pull starts again from its first page).
#[test]
fn a_cursor_that_does_not_name_its_carry_is_malformed() {
    let cursor = crate::SnapshotCursor {
        collection: CollectionId(7),
        after_key: vec![1],
        until_key: None,
        carry: Carry::Done,
    };
    let mut value = serde_json::to_value(&cursor).unwrap();
    assert!(value.get("carry").is_some(), "premise: a done carry is written, as \"Done\"");
    serde_json::from_value::<crate::SnapshotCursor>(value.clone()).expect("the whole cursor reads");
    value.as_object_mut().unwrap().remove("carry");
    assert!(
        serde_json::from_value::<crate::SnapshotCursor>(value).is_err(),
        "a cursor without the field does not read"
    );
}

/// The wire form of a names row: db length (u16, big-endian) || db || logical length (u16) || logical || physical.
fn names_value_of(db: &str, logical: &str, physical: &str) -> Vec<u8> {
    let mut value = Vec::new();
    value.extend_from_slice(&(db.len() as u16).to_be_bytes());
    value.extend_from_slice(db.as_bytes());
    value.extend_from_slice(&(logical.len() as u16).to_be_bytes());
    value.extend_from_slice(logical.as_bytes());
    value.extend_from_slice(physical.as_bytes());
    value
}

/// A sender with a stage, and the page of its whole database with the names row of the stage made wrong only in its logical name.
fn a_page_with_a_wrong_logical_names_row() -> (Engine, tempfile::TempDir, crate::SnapshotPage) {
    let (a, dir) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE, MergePolicy::Document, deadline).unwrap();
    let mut page = a.snapshot_page(None, None).unwrap();
    let row = page
        .carried
        .iter_mut()
        .find(|row| row.table == crate::CARRY_NAMES && row.key.to_vec() == id_key(DB, STAGE))
        .expect("premise: the page carries the stage's names row");
    // the key as it was, and a logical name of another name
    row.value = serde_bytes::ByteBuf::from(names_value_of(DB, "another", STAGE));
    (a, dir, page)
}

/// A names row whose logical name is not its physical's (the physical itself, or the name before a stage's or a recreation's mark) is a
/// damaged page, refused when it arrives: held, it would name the events of another name. NOTHING else on the page holds the id (no
/// definition, no register), so it is this rule alone that refuses it: a row that passed would be news, and merge.
#[test]
fn a_carried_names_row_of_another_logical_name_refuses_the_page() {
    let (a, _da, mut page) = a_page_with_a_wrong_logical_names_row();
    page.collections.clear();
    page.register = None;
    let (b, _db) = engine();
    let refused =
        b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page);
    assert!(refused.is_err(), "the page is refused: {refused:?}");
    assert!(names_of(&b, STAGE).is_none(), "and no names row is held for the stage");
}

/// The same row on a page whose definitions made the stage first: the member holds the real row for the id, the page is refused and the
/// real row stands.
#[test]
fn a_carried_names_row_of_another_logical_name_is_refused_beside_the_held_one() {
    let (a, _da, page) = a_page_with_a_wrong_logical_names_row();
    let (b, _db) = engine();
    let refused =
        b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page);
    assert!(refused.is_err(), "the page is refused: {refused:?}");
    assert_eq!(names_of(&b, STAGE), named(STAGE, "orders"), "and the real row stands");
}

/// A names row held under an id that OTHER names derive (a collision of the 64-bit ids, which no row of this member can make: the value of
/// a row is fixed by its key and the logical-name rule) is refused where a row of it arrives, and not at the merge of the last page.
#[test]
fn a_names_row_held_under_an_id_other_names_derive_is_refused_on_arrival() {
    let (e, _dir) = engine();
    let txn = e.begin_write(WriterHolder::Ddl).unwrap();
    // planted by hand: the key of `orders` over the value of other names
    crate::register::restore_row(
        &txn,
        Carried::Names,
        &id_key(DB, "orders"),
        &names_value_of(DB, "other", "other"),
    )
    .unwrap();
    let held = crate::register::CarryTables::open(&txn).unwrap();
    let row = crate::register::CarriedRowKind::Names {
        db: DB.to_string(),
        logical: "orders".to_string(),
        physical: "orders".to_string(),
    };
    assert!(held.is_news(&row).is_err(), "a row for the id with other names is damage");
    drop(held);
    txn.abort().unwrap();
}

/// The bound a carried row is counted at holds of the real encoding, at the widest index a page can reach. A page of at most
/// `SNAPSHOT_PAGE_BYTES` of rows that each cost at least their key and the bound cannot hold a million of them, so its array index has at
/// most 6 digits, which the cost of the page's LAST row measures: the size of a page of a million rows less that of one of 999,999, less
/// the key.
#[test]
fn the_carried_row_overhead_bounds_the_encoding() {
    const ROWS: usize = 1_000_000;
    const KEY: usize = 8;
    const {
        assert!(
            crate::SNAPSHOT_PAGE_BYTES / (KEY + crate::snapshot::CARRIED_ROW_FRAME) < ROWS,
            "premise: a page cannot hold a million rows, so no index has 7 digits"
        );
    }
    let page_of = |rows: Vec<crate::CarriedRow>| crate::SnapshotPage {
        collections: Vec::new(),
        documents: Vec::new(),
        next: None,
        versions: Default::default(),
        dropped: None,
        dropped_collections: Vec::new(),
        deleted_documents: Vec::new(),
        dropped_indexes: Vec::new(),
        tombstone_horizon: None,
        register: None,
        carried: rows,
        dropped_database: None,
    };
    let size = |page: &crate::SnapshotPage| bson::serialize_to_vec(page).unwrap().len();
    let mut rows: Vec<crate::CarriedRow> = (0..ROWS)
        .map(|i| crate::CarriedRow {
            table: crate::CARRY_OPTIONS,
            key: serde_bytes::ByteBuf::from((i as u64).to_be_bytes().to_vec()),
            value: serde_bytes::ByteBuf::new(),
        })
        .collect();
    let full = size(&page_of(rows.clone()));
    rows.pop();
    let one_less = size(&page_of(rows));
    let last = full - one_less - KEY;
    assert!(
        last <= crate::snapshot::CARRIED_ROW_FRAME,
        "the last row costs {last} bytes beyond its key and value, at index {}; the bound is {}",
        ROWS - 1,
        crate::snapshot::CARRIED_ROW_FRAME
    );
}

// -- The scoped carry: a scoped page carries the scoped collection's names row and options row, and names the database's drop ----------------
//
// `SnapshotPage::dropped_database: Option<Stamp>` and every literal). Uses the module's helpers (`engine`, `a_sender_with`, `pulled`, `rows`,
// `options_of`, `names_of`, `id_key`, `keys`) and `crate::vector_meta` for the shadow's name.

fn scoped(id: CollectionId) -> SnapshotProgress {
    SnapshotProgress::of_collection(id)
}

/// A scoped page carries the options row of the scoped collection, as the sender stores it, and the row of no other collection.
#[test]
fn a_scoped_page_carries_the_options_row_of_the_scoped_collection_and_no_other() {
    let (a, _da) = a_sender_with(3);
    let id = CollectionId::derive(DB, "c1");
    let page = a.snapshot_page(None, Some(id)).unwrap();
    let carried: Vec<(u8, Vec<u8>)> =
        page.carried.iter().map(|row| (row.table, row.key.to_vec())).collect();
    assert_eq!(carried, vec![(crate::CARRY_OPTIONS, id_key(DB, "c1"))], "one row, c1's");
    let held = rows(&a, Carried::Options)
        .into_iter()
        .find(|(key, _)| *key == id_key(DB, "c1"))
        .expect("premise: the sender holds the row");
    assert_eq!(page.carried[0].value.to_vec(), held.1, "as the sender stores it");
}

/// The page of a stage carries its names row too: the stage's own physical, naming the logical name it belongs to.
#[test]
fn a_scoped_page_of_a_stage_carries_its_names_row() {
    let (a, _da) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE, MergePolicy::Document, deadline).unwrap();
    let page = a.snapshot_page(None, Some(CollectionId::derive(DB, STAGE))).unwrap();
    assert!(
        page.carried
            .iter()
            .any(|row| row.table == crate::CARRY_NAMES && row.key.to_vec() == id_key(DB, STAGE)),
        "the stage's names row rides the page: {:?}",
        page.carried
    );
}

/// A pull of a collection the sender dropped has no definition to bring the options of: the carried row does, and the receiver holds it
/// with no collection.
#[test]
fn a_scoped_pull_of_a_dropped_collection_leaves_the_receiver_its_options_row() {
    let (a, _da) = engine();
    a.create_collection(DB, "ghost").unwrap();
    a.drop_collection(DB, "ghost").unwrap();
    let (b, _db) = engine();
    let id = CollectionId::derive(DB, "ghost");
    pulled(&b, &a, &mut scoped(id));
    assert!(b.get_collection(DB, "ghost").is_err(), "premise: no collection is made");
    assert_eq!(options_of(&b, "ghost"), options_of(&a, "ghost"), "the sender's row");
    assert!(options_of(&b, "ghost").is_some(), "and it is there");
}

/// Every row a scoped page carries is the scoped collection's: one keyed by another id is a damaged page.
#[test]
fn a_scoped_carried_row_of_another_collection_refuses_the_page() {
    let (a, _da) = a_sender_with(2);
    let mut page = a.snapshot_page(None, Some(CollectionId::derive(DB, "c0"))).unwrap();
    let other = rows(&a, Carried::Options)
        .into_iter()
        .find(|(key, _)| *key == id_key(DB, "c1"))
        .expect("premise: the sender holds c1's row");
    page.carried.push(crate::CarriedRow {
        table: crate::CARRY_OPTIONS,
        key: serde_bytes::ByteBuf::from(other.0),
        value: serde_bytes::ByteBuf::from(other.1),
    });
    let (b, _db) = engine();
    let refused =
        b.apply_snapshot_page(a.node_id(), &mut scoped(CollectionId::derive(DB, "c0")), &page);
    assert!(refused.is_err(), "the page is refused: {refused:?}");
    assert!(options_of(&b, "c1").is_none(), "and the other collection's row is held nowhere");
}

// -- `dropped_database` ------------------------------------------------------------------------------------------------------------------

/// A sender whose collection `orders` was dropped (its own tombstone, at the first stamp) and whose database was dropped LATER (its
/// tombstone, at the second, which the collection's own does not follow): the stamps of the two.
fn a_sender_with_a_later_database_drop() -> (Engine, tempfile::TempDir, Stamp, Stamp) {
    let (a, dir) = engine();
    a.create_collection(DB, "orders").unwrap();
    a.create_collection(DB, "keep").unwrap();
    a.drop_collection(DB, "orders").unwrap();
    let own = a
        .collection_dropped_at(CollectionId::derive(DB, "orders"))
        .unwrap()
        .expect("premise: the collection's own tombstone");
    a.drop_database(DB).unwrap();
    let database = a.database_dropped_at(DB).unwrap().expect("premise: the database's drop");
    assert!(database > own, "premise: the database was dropped after the collection");
    assert_eq!(
        a.collection_dropped_at(CollectionId::derive(DB, "orders")).unwrap(),
        Some(own),
        "premise: the collection's own tombstone stays at its own drop"
    );
    (a, dir, own, database)
}

#[test]
fn a_scoped_page_of_a_collection_gone_before_a_database_drop_names_both_drops() {
    let (a, _da, own, database) = a_sender_with_a_later_database_drop();
    let id = CollectionId::derive(DB, "orders");
    let page = a.snapshot_page(None, Some(id)).unwrap();
    assert_eq!(page.dropped, Some(own), "the collection's own drop");
    assert_eq!(page.dropped_database, Some(database), "and the database's");
    // a whole-database page and a page of a collection that stands name none
    assert_eq!(a.snapshot_page(None, None).unwrap().dropped_database, None, "a whole page: none");
    let (c, _dc) = engine();
    c.create_collection(DB, "orders").unwrap();
    let standing = c.snapshot_page(None, Some(id)).unwrap();
    assert_eq!(standing.dropped_database, None, "a collection that stands: no drop to name");
}

/// The receiver raises the collection's tombstone, and its shadow's, to the database's stamp, and does NOT record the database's own:
/// another collection of that database that it holds stands.
#[test]
fn a_scoped_receiver_raises_the_collections_tombstones_and_never_records_the_databases() {
    let (a, _da, own, database) = a_sender_with_a_later_database_drop();
    let id = CollectionId::derive(DB, "orders");
    let shadow = CollectionId::derive(DB, &kimmy_core::vector_meta::shadow_name("orders"));
    let page = a.snapshot_page(None, Some(id)).unwrap();
    let (b, _db) = engine();
    b.create_collection(DB, "other").unwrap();
    b.apply_snapshot_page(a.node_id(), &mut scoped(id), &page).unwrap();
    let raised = b.collection_dropped_at(id).unwrap();
    assert_eq!(raised, Some(database), "the collection's tombstone, raised from {own:?}");
    assert_eq!(b.collection_dropped_at(shadow).unwrap(), Some(database), "and its shadow's");
    assert_eq!(b.database_dropped_at(DB).unwrap(), None, "the database's own is not recorded");
    assert!(b.get_collection(DB, "other").is_ok(), "so the other collection of it stands");
}

/// The sender is believed only up to its own coverage (ADR-163): a drop stamped above it raises nothing.
#[test]
fn a_scoped_page_whose_database_drop_is_above_its_coverage_is_ignored() {
    let (a, _da, own, database) = a_sender_with_a_later_database_drop();
    let id = CollectionId::derive(DB, "orders");
    let mut page = a.snapshot_page(None, Some(id)).unwrap();
    page.dropped_database = Some(Stamp::new(
        Hlc::new(database.hlc.wall_ms + 60_000, 0),
        database.origin().node,
        database.origin().epoch,
    ));
    let (b, _db) = engine();
    b.apply_snapshot_page(a.node_id(), &mut scoped(id), &page).unwrap();
    assert_eq!(b.collection_dropped_at(id).unwrap(), Some(own), "only the collection's own drop");
}

/// A receiver with neither the collection nor a names row learns the database from the page's own rows (the options row names its
/// database and its collection), and checks that they derive the scoped id: a row that does not is a damaged page.
#[test]
fn a_scoped_receiver_checks_the_rows_it_learns_the_database_from_derive_the_scoped_id() {
    let (a, _da, _own, _database) = a_sender_with_a_later_database_drop();
    let id = CollectionId::derive(DB, "orders");
    let page = a.snapshot_page(None, Some(id)).unwrap();
    assert!(!page.carried.is_empty(), "premise: the page carries the collection's row");
    let mut damaged = page.clone();
    // another collection's value under this key: its names do not derive the scoped id
    let (b0, _d0) = engine();
    b0.create_collection(DB, "elsewhere").unwrap();
    let other = rows(&b0, Carried::Options).into_iter().next().unwrap();
    damaged.carried = vec![crate::CarriedRow {
        table: crate::CARRY_OPTIONS,
        key: serde_bytes::ByteBuf::from(id_key(DB, "orders")),
        value: serde_bytes::ByteBuf::from(other.1),
    }];
    let (b, _db) = engine();
    let refused = b.apply_snapshot_page(a.node_id(), &mut scoped(id), &damaged);
    assert!(refused.is_err(), "refused: {refused:?}");
    assert_eq!(b.collection_dropped_at(id).unwrap(), None, "and nothing is recorded");
}

/// The field is required on the wire. (Read as BSON, the page's own encoding: its version vector is keyed by origins, which JSON cannot hold.)
#[test]
fn a_page_without_its_database_drop_does_not_decode() {
    let (a, _da) = engine();
    let page = a.snapshot_page(None, None).unwrap();
    let mut value = bson::serialize_to_document(&page).unwrap();
    assert!(value.contains_key("dropped_database"), "premise: the field is written, as null");
    bson::deserialize_from_document::<crate::SnapshotPage>(value.clone())
        .expect("the whole page reads");
    value.remove("dropped_database");
    assert!(
        bson::deserialize_from_document::<crate::SnapshotPage>(value).is_err(),
        "a page without the field does not read"
    );
}

// -- Rulings of 4:3x: scoped pulls STAGE (keyed by peer AND scope), and the carry page's cursor reserve is tested -------------------------------

/// A scoped pull and a whole-database pull from the SAME peer run interleaved, and neither empties or merges the other's staging: the
/// whole pull is stopped after its carry (rows staged under the peer and no scope), and a scoped pull of a gone collection of that peer
/// completes in between (its page 0 empties only the staging of ITS scope, and its completion merges only its own).
#[test]
fn a_scoped_pull_and_a_whole_pull_from_one_peer_do_not_touch_each_others_staging() {
    let (a, _da) = a_sender_with_ghosts(30);
    a.set_snapshot_room_for_test(Some(a_small_room_that_holds_a_document(&a)));
    let (b, _db) = engine();
    let mut whole = stopped_after_the_carry(&b, &a);
    let staged = b.carry_stage_rows_for_test(a.node_id());
    assert!(!staged.is_empty(), "premise: the whole pull staged rows");
    // the scoped pull of one gone collection, to its end, between two pages of the whole pull
    let ghost = CollectionId::derive(DB, "g3");
    pulled(&b, &a, &mut scoped(ghost));
    assert_eq!(options_of(&b, "g3"), options_of(&a, "g3"), "the scoped pull merged its own row");
    let left = b.carry_stage_rows_for_test(a.node_id());
    assert_eq!(left, staged, "and left the whole pull's staging");
    // the whole pull goes on and ends with every row
    pulled(&b, &a, &mut whole);
    assert_eq!(keys(&b, Carried::Options), keys(&a, Carried::Options), "the whole pull merged");
}

/// A carry page cut by the budget is counted within its room: the rows as the sender bounds them (key, value and the carried row's frame)
/// and the cursor it cuts at, reserved for the widest carry key, never exceed the page's rows room; and the page encodes within it. A room
/// that is not a multiple of a row, so that the rows fill it to within a cursor of its end.
#[test]
fn a_carry_page_cut_by_the_budget_is_counted_within_its_room() {
    let (a, _da) = a_sender_with_ghosts(40);
    let widest = rows(&a, Carried::Options)
        .iter()
        .map(|(key, value)| key.len() + value.len() + crate::snapshot::CARRIED_ROW_FRAME)
        .max()
        .unwrap();
    let reserve = crate::snapshot::cursor_reserve(8, 0);
    // rows fill the room to within less than the cursor's reserve of its end, for the rows of this sender
    let room = 4 * widest + reserve / 2;
    a.set_snapshot_room_for_test(Some(room));
    let (b, _db) = engine();
    let mut progress = SnapshotProgress::whole_database();
    let mut cut = 0;
    while !progress.is_complete() {
        let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        let in_the_carry = matches!(page.next.as_ref().map(|c| &c.carry), Some(Carry::At(_)));
        if page.carried.len() > 1 && in_the_carry {
            cut += 1;
            let counted: usize = page
                .carried
                .iter()
                .map(|row| row.key.len() + row.value.len() + crate::snapshot::CARRIED_ROW_FRAME)
                .sum();
            assert!(
                counted + reserve <= room,
                "{} carry rows count {counted} and the cursor {reserve}: over the room {room}",
                page.carried.len()
            );
        }
        b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    }
    assert!(cut >= 3, "premise: the carry was cut by the budget on several pages: {cut}");
}

// -- A collection this member HOLDS, when the database's drop reaches it through a scoped page -------------------------------------------------

fn pause() {
    std::thread::sleep(std::time::Duration::from_millis(15));
}

/// A record of an earlier life, planted in the collection `id` of `e` at `stamp`, as the sweep tests plant one: what a partial copy of a life
/// that has ended leaves under a life that stands.
fn plant(e: &Engine, id: CollectionId, doc_id: i64, stamp: Stamp) {
    let body = bson::serialize_to_vec(&bson::doc! { "_id": doc_id, "old": true }).unwrap();
    let key = crate::docs::doc_key(&kimmy_core::DocId::Int64(doc_id)).unwrap();
    let record = kimmy_core::DocRecord::live(stamp, body);
    let txn = e.begin_write(WriterHolder::Replication).unwrap();
    {
        let mut docs = txn.open_table(crate::tables::DOCS).unwrap();
        crate::live_count::put_record(
            &txn,
            &mut docs,
            id.0,
            &key,
            &crate::codec::encode_doc_record(&record),
        )
        .unwrap();
    }
    txn.commit().unwrap();
}

/// A sender whose collection `orders` was dropped (d) and whose database was dropped later (D), the two stamps, and a pause after each so
/// that a life made between them is stamped between them.
fn a_sender_with_the_two_drops_apart() -> (Engine, tempfile::TempDir, Stamp, Stamp) {
    let (s, dir) = engine();
    s.create_collection(DB, "orders").unwrap();
    s.create_collection(DB, "keep").unwrap();
    s.drop_collection(DB, "orders").unwrap();
    let d = s.collection_dropped_at(CollectionId::derive(DB, "orders")).unwrap().unwrap();
    pause();
    s.drop_database(DB).unwrap();
    let big_d = s.database_dropped_at(DB).unwrap().unwrap();
    assert!(big_d.hlc.wall_ms > d.hlc.wall_ms + 5, "premise: the drops are apart");
    (s, dir, d, big_d)
}

/// A holder whose life of the collection stands ABOVE the database's drop, and holds a record stamped between the collection's drop and the
/// database's: the drop of the database is a carried drop of that collection, and the life it ignores is swept of what is at or below it.
/// The life stands, and holds nothing at or below the database's stamp, with no restart.
#[test]
fn a_holder_whose_life_is_above_the_databases_drop_is_swept_of_what_is_at_or_below_it() {
    let (s, _ds, d, big_d) = a_sender_with_the_two_drops_apart();
    let id = CollectionId::derive(DB, "orders");
    pause();
    let (r, _dr) = engine();
    r.create_collection(DB, "orders").unwrap();
    let between = Hlc::new((d.hlc.wall_ms + big_d.hlc.wall_ms) / 2, 0);
    plant(&r, id, 7, Stamp::new(between, s.node_id(), s.epoch()));
    assert_eq!(r.count_by_id(id).unwrap(), Some(1), "premise: the record of the earlier life");
    pulled(&r, &s, &mut scoped(id));
    assert!(r.get_collection(DB, "orders").is_ok(), "the life above the drop stands");
    assert_eq!(r.count_by_id(id).unwrap(), Some(0), "and holds nothing at or below the drop");
}

/// A holder whose life was made after the collection's drop and before the database's: the database's drop ends it, with what it holds at or
/// below that stamp, and it is not left standing under a tombstone above its creation.
#[test]
fn a_holder_whose_life_is_at_or_below_the_databases_drop_is_ended_by_it() {
    let (s, _ds) = engine();
    s.create_collection(DB, "orders").unwrap();
    s.create_collection(DB, "keep").unwrap();
    s.drop_collection(DB, "orders").unwrap();
    pause();
    let (r, _dr) = engine();
    let held = r.create_collection(DB, "orders").unwrap();
    r.insert(&held, bson::doc! { "_id": 1 }).unwrap();
    pause();
    s.drop_database(DB).unwrap();
    let id = CollectionId::derive(DB, "orders");
    let d = s.collection_dropped_at(id).unwrap().unwrap();
    let big_d = s.database_dropped_at(DB).unwrap().unwrap();
    assert!(held.created > d.hlc && held.created <= big_d.hlc, "premise: the life is between");
    pulled(&r, &s, &mut scoped(id));
    assert!(r.get_collection(DB, "orders").is_err(), "the database's drop ended the life");
}

/// A scoped pull of two pages, whose sender drops the collection between them, ends on a page with no document, delete or grant: the rows
/// the first page staged are still merged, in a transaction the completion opens for them.
#[test]
fn a_scoped_pull_that_ends_on_a_page_of_nothing_still_merges_what_it_staged() {
    let base = physical_now_ms();
    let (a, _da) = engine();
    let (c, _dc) = engine();
    let earlier = creation(&a, base - 3_000);
    let later = creation(&c, base - 1_000);
    let (source, _ds) = applied_in_order(&[&earlier, &later]);
    let meta = source.get_collection(DB, "orders").unwrap();
    for i in 0..4 {
        source.insert(&meta, bson::doc! { "_id": i, "pad": "x".repeat(200) }).unwrap();
    }
    assert_eq!(
        source.get_collection(DB, "orders").unwrap().merge_policy_at,
        Some(Hlc::new(base - 3_000, 0)),
        "premise: the definition's policy stamp stayed at the first creation"
    );
    source.set_snapshot_room_for_test(Some(a_small_room_that_holds_a_document(&source)));
    // between the two pages the sender drops the collection: the second page is the drop and nothing else
    let id = meta.id;
    source.set_snapshot_reader_hook_for_test(Some(std::sync::Arc::new(move |e, after| {
        if after.is_some() {
            e.drop_collection(DB, "orders").unwrap();
        }
    })));
    let (b, _db) = engine();
    let mut progress = scoped(id);
    let pages = pulled(&b, &source, &mut progress);
    assert_eq!(pages.len(), 2, "premise: two pages, the second without a document");
    let nothing = pages[1].documents.is_empty() && pages[1].carried.is_empty();
    assert!(nothing, "premise: and nothing else");
    assert_eq!(
        options_of(&b, "orders").unwrap().at,
        Hlc::new(base - 1_000, 0),
        "the first page's row, which only the carry brings, was merged"
    );
}
