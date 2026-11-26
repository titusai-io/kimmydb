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
