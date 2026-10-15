//! The definition repair's policy (repair design section 12): a scoped snapshot's first
//! page applied under `Definitions` builds at most one plain index this member lacks and
//! changes nothing else. Each test builds a sender `a` and a requester `b`, takes the
//! sender's first scoped page, and applies it.

use bson::doc;
use kimmy_core::{CollectionId, Hlc};

use crate::{DefinitionRestore, Engine, IndexField, SnapshotApplied, SnapshotProgress};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn orders() -> CollectionId {
    CollectionId::derive("shop", "orders")
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn pull(into: &Engine, from: &Engine) {
    let entries =
        from.entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background).unwrap().entries;
    into.apply_batch(&entries).unwrap();
}

/// A sender and a requester that have replicated everything so far.
fn pair() -> (Engine, Engine, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    a.create_collection("shop", "orders").unwrap();
    pull(&b, &a);
    (a, b, [da, db])
}

fn names(e: &Engine) -> Vec<String> {
    let mut names: Vec<_> =
        e.list_indexes("shop", "orders").unwrap().into_iter().map(|i| i.name).collect();
    names.sort();
    names
}

/// The sender's first scoped page applied to `into` under `Definitions` with `limit`.
fn repair(into: &Engine, from: &Engine, limit: u64) -> SnapshotApplied {
    repair_with(into, from, limit, |_| {})
}

fn repair_with(
    into: &Engine,
    from: &Engine,
    limit: u64,
    shape: impl FnOnce(&mut crate::SnapshotPage),
) -> SnapshotApplied {
    let mut progress = SnapshotProgress::of_collection_definitions(orders(), limit);
    assert_eq!(progress.policy(), crate::SnapshotPolicy::Definitions);
    let mut page = from.snapshot_page(None, progress.scope()).unwrap();
    shape(&mut page);
    let done = into.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
    assert!(progress.is_complete(), "the pull ends after its one page");
    done
}

fn index(a: &Engine, field: &str, name: &str) {
    a.create_index("shop", "orders", vec![IndexField::ascending(field)], false, Some(name.into()))
        .unwrap();
}

const BIG: u64 = 1_000_000;

#[test]
fn a_plain_index_this_member_lacks_is_built_and_nothing_else_is() {
    let (a, b, _d) = pair();
    let ca = a.get_collection("shop", "orders").unwrap();
    a.insert(&ca, doc! { "_id": 1, "v": 1 }).unwrap();
    later();
    index(&a, "v", "by_v");
    let done = repair(&b, &a, BIG);
    assert_eq!(done.definitions.built, 1);
    assert_eq!(done.definitions.counts().iter().sum::<usize>(), 1, "{:?}", done.definitions);
    assert_eq!(names(&b), vec!["by_v".to_string()]);
    // The definition is the sender's, so the definition digests agree.
    let cb = b.get_collection("shop", "orders").unwrap();
    let ca = a.get_collection("shop", "orders").unwrap();
    assert_eq!(
        cb.indexes[0].fields, ca.indexes[0].fields,
        "the built index has the sender's definition"
    );
    // The page's documents are ignored: the requester still holds none.
    assert_eq!(b.count_by_id(orders()).unwrap(), Some(0));
    assert_eq!(done.applied, 0);
}

#[test]
fn the_built_index_is_logged_at_its_own_stamp_and_the_page_grants_nothing() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "by_v");
    let before = b.witnessed_vector().unwrap();
    repair(&b, &a, BIG);
    let cb = b.get_collection("shop", "orders").unwrap();
    let ca = a.get_collection("shop", "orders").unwrap();
    assert_eq!(cb.indexes[0].created, ca.indexes[0].created, "the sender's stamp");
    // The restore appends the creation as an entry at that stamp so the member serves it on.
    let served = b.entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background).unwrap();
    assert!(
        served.entries.iter().any(|e| e.stamp == ca.indexes[0].created.unwrap()),
        "the definition was not appended"
    );
    // The page granted nothing: the creation is appended to the oplog (re-logging a restored
    // definition is not minting one), and the witnessed vector is exactly as it was.
    assert_eq!(b.witnessed_vector().unwrap(), before, "the page granted coverage");
}

#[test]
fn a_unique_index_is_counted_and_not_built() {
    let (a, b, _d) = pair();
    later();
    a.create_index("shop", "orders", vec![IndexField::ascending("v")], true, Some("u".into()))
        .unwrap();
    let done = repair(&b, &a, BIG);
    assert_eq!(done.definitions, DefinitionRestore { unique: 1, ..Default::default() });
    assert!(names(&b).is_empty());
}

#[test]
fn a_ttl_index_is_counted_and_not_built() {
    let (a, b, _d) = pair();
    later();
    a.create_index_with(
        "shop",
        "orders",
        vec![IndexField::ascending("at")],
        false,
        crate::Enforcement::Local,
        Some("ttl".into()),
        Some(60),
        None,
    )
    .unwrap();
    let done = repair(&b, &a, BIG);
    assert_eq!(done.definitions, DefinitionRestore { ttl: 1, ..Default::default() });
    assert!(names(&b).is_empty());
}

#[test]
fn a_rival_under_the_same_name_is_counted_and_the_local_index_is_untouched() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "by_x");
    later();
    index(&b, "w", "by_x");
    let before = b.get_collection("shop", "orders").unwrap();
    let done = repair(&b, &a, BIG);
    assert_eq!(done.definitions, DefinitionRestore { rival: 1, ..Default::default() });
    let after = b.get_collection("shop", "orders").unwrap();
    assert_eq!(after.indexes, before.indexes, "the supersede branch must be unreachable");
}

#[test]
fn an_index_already_held_under_the_same_definition_is_nothing() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "by_v");
    pull(&b, &a);
    let done = repair(&b, &a, BIG);
    assert_eq!(done.definitions, DefinitionRestore::default());
}

#[test]
fn an_index_with_no_stamp_is_history() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "by_v");
    let done = repair_with(&b, &a, BIG, |page| {
        for state in &mut page.collections {
            for index in &mut state.indexes {
                index.created = None;
            }
        }
    });
    assert_eq!(done.definitions, DefinitionRestore { history: 1, ..Default::default() });
    assert!(names(&b).is_empty());
}

#[test]
fn an_index_this_member_dropped_after_it_was_made_is_tombstoned() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "by_v");
    pull(&b, &a);
    later();
    assert!(b.drop_index("shop", "orders", "by_v").unwrap());
    let done = repair(&b, &a, BIG);
    assert_eq!(done.definitions, DefinitionRestore { tombstoned: 1, ..Default::default() });
    assert!(names(&b).is_empty(), "an index this member dropped was rebuilt from a peer");
}

#[test]
fn an_index_at_or_below_either_horizon_is_ambiguous() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "by_v");
    let created = a.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap().hlc;
    // The sender's horizon on the page is at the index's stamp.
    let done = repair_with(&b, &a, BIG, |page| page.tombstone_horizon = Some(created));
    assert_eq!(done.definitions, DefinitionRestore { ambiguous: 1, ..Default::default() });
    // And this member's own.
    b.test_set_tombstone_horizon(created).unwrap();
    let done = repair_with(&b, &a, BIG, |page| page.tombstone_horizon = Some(Hlc::ZERO));
    assert_eq!(done.definitions, DefinitionRestore { ambiguous: 1, ..Default::default() });
    assert!(names(&b).is_empty());
}

#[test]
fn a_sender_that_sends_no_horizon_admits_nothing() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "by_v");
    let done = repair_with(&b, &a, BIG, |page| page.tombstone_horizon = None);
    assert_eq!(done.definitions, DefinitionRestore { ambiguous: 1, ..Default::default() });
    assert!(names(&b).is_empty());
}

#[test]
fn a_collection_gone_here_is_counted_gone() {
    let (a, _b, _d) = pair();
    later();
    index(&a, "v", "by_v");
    let (c, _dc) = engine();
    let done = repair(&c, &a, BIG);
    assert_eq!(done.definitions, DefinitionRestore { gone: 1, ..Default::default() });
    assert!(c.get_collection("shop", "orders").is_err(), "the repair creates no collection");
}

#[test]
fn at_most_one_index_is_built_per_repair() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "one");
    later();
    index(&a, "w", "two");
    let done = repair(&b, &a, BIG);
    assert_eq!(done.definitions.built, 1);
    assert_eq!(names(&b), vec!["one".to_string()], "the first that passes is the one");
    // The next repair builds the other.
    let done = repair(&b, &a, BIG);
    assert_eq!(done.definitions.built, 1);
    assert_eq!(names(&b), vec!["one".to_string(), "two".to_string()]);
}

#[test]
fn an_older_ambiguous_index_does_not_spend_the_repairs_one_build() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "old");
    let old = a.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap().hlc;
    later();
    index(&a, "w", "new");
    // The sender's horizon sits at the older index's stamp: it is ambiguous, the newer is not.
    let done = repair_with(&b, &a, BIG, |page| page.tombstone_horizon = Some(old));
    assert_eq!(done.definitions.ambiguous, 1);
    assert_eq!(done.definitions.built, 1);
    assert_eq!(names(&b), vec!["new".to_string()]);
}

#[test]
fn a_collection_over_the_gate_is_counted_too_large_and_built_for_nothing() {
    let (a, b, _d) = pair();
    let cb = b.get_collection("shop", "orders").unwrap();
    b.insert(&cb, doc! { "_id": 1 }).unwrap();
    b.insert(&cb, doc! { "_id": 2 }).unwrap();
    later();
    index(&a, "v", "by_v");
    let done = repair(&b, &a, 1);
    assert_eq!(done.definitions, DefinitionRestore { too_large: 1, ..Default::default() });
    assert!(names(&b).is_empty());
    // At the gate it builds.
    let done = repair(&b, &a, 2);
    assert_eq!(done.definitions.built, 1);
}

#[test]
fn a_drop_landing_between_the_check_and_the_build_wins() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "by_v");
    let b = std::rc::Rc::new(b);
    let competing = std::rc::Rc::clone(&b);
    let (done, ()) = crate::sync::race_hooks::race(
        crate::sync::race_hooks::Race::DefinitionRepair,
        move || {
            // This member creates and drops the name, after the page's definition was made.
            later();
            competing
                .create_index(
                    "shop",
                    "orders",
                    vec![IndexField::ascending("z")],
                    false,
                    Some("by_v".into()),
                )
                .unwrap();
            later();
            assert!(competing.drop_index("shop", "orders", "by_v").unwrap());
        },
        || repair(&b, &a, BIG),
    );
    assert_eq!(done.definitions, DefinitionRestore { tombstoned: 1, ..Default::default() });
    assert!(names(&b).is_empty(), "an index dropped under the repair was rebuilt");
}

#[test]
fn a_rival_landing_between_the_check_and_the_build_is_not_replaced() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "by_v");
    let b = std::rc::Rc::new(b);
    let competing = std::rc::Rc::clone(&b);
    let (done, ()) = crate::sync::race_hooks::race(
        crate::sync::race_hooks::Race::DefinitionRepair,
        move || {
            later();
            competing
                .create_index(
                    "shop",
                    "orders",
                    vec![IndexField::ascending("z")],
                    false,
                    Some("by_v".into()),
                )
                .unwrap();
        },
        || repair(&b, &a, BIG),
    );
    assert_eq!(done.definitions, DefinitionRestore { rival: 1, ..Default::default() });
    let held = b.get_collection("shop", "orders").unwrap();
    assert_eq!(held.indexes[0].fields, vec![IndexField::ascending("z")]);
}

#[test]
fn an_index_this_build_cannot_apply_is_refused_counted_and_leaves_a_hint() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "bad");
    let done = repair_with(&b, &a, BIG, |page| {
        for state in &mut page.collections {
            for index in &mut state.indexes {
                // A filter shape no build can parse.
                index.partial_filter = Some(doc! { "v": { "$nope": 1 } });
            }
        }
    });
    assert_eq!(done.definitions, DefinitionRestore { refused: 1, ..Default::default() });
    assert!(names(&b).is_empty());
    // And the refusal is remembered, so the next comparison is explained and not a finding.
    assert_eq!(b.refusal_hints(orders()).unwrap().len(), 1, "no refusal hint was written");
}

#[test]
fn a_page_whose_sender_dropped_the_collection_ends_the_pull_abandoned() {
    let (a, b, _d) = pair();
    let mut progress = SnapshotProgress::of_collection_definitions(orders(), BIG);
    let mut page = a.snapshot_page(None, progress.scope()).unwrap();
    page.dropped = Some(kimmy_core::Stamp::new(Hlc::new(9, 0), a.node_id()));
    let done = b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    assert!(done.sender_dropped);
    assert_eq!(done.definitions, DefinitionRestore::default());
}

#[test]
fn a_vector_configuration_and_the_pages_drops_are_not_applied() {
    let (a, b, _d) = pair();
    later();
    a.configure_vectors(
        "shop",
        "orders",
        kimmy_core::VectorConfig {
            fields: vec!["text".into()],
            provider: kimmy_core::ProviderConfig::Byo {},
            dim: 4,
            metric: Default::default(),
            document_prefix: None,
            query_prefix: None,
            chunk: Default::default(),
        },
    )
    .unwrap();
    later();
    index(&a, "v", "by_v");
    let built = a.get_collection("shop", "orders").unwrap();
    let id = built.indexes.iter().find(|i| i.name == "by_v").unwrap().id;
    let before = b.get_collection("shop", "orders").unwrap();
    let done = repair_with(&b, &a, BIG, |page| {
        assert!(page.collections[0].vector.is_some(), "premise: the page carries the vectors");
        // The page says the sender dropped the index it also carries, at a stamp above its
        // creation: under the `All` route that would remove it again.
        let above =
            kimmy_core::Stamp::new(Hlc::new(crate::physical_now_ms() + 60_000, 0), a_node());
        page.dropped_indexes.push((orders(), id, above));
    });
    assert_eq!(done.definitions.built, 1);
    let after = b.get_collection("shop", "orders").unwrap();
    assert_eq!(after.vector, before.vector, "the vector configuration was applied");
    assert!(after.vector.is_none());
    assert_eq!(names(&b), vec!["by_v".to_string()], "a drop the page carried was applied");
    assert!(b.get_collection("shop", &kimmy_core::vector_meta::shadow_name("orders")).is_err());
}

fn a_node() -> kimmy_core::NodeId {
    kimmy_core::NodeId::from_bytes([7; 16])
}

/// A tombstoned index is dropped from the choice before the repair's one build is taken,
/// so it cannot spend it (it would otherwise be chosen first, as the older, and vetoed).
#[test]
fn a_tombstoned_index_does_not_spend_the_repairs_one_build() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "dropped_here");
    pull(&b, &a);
    later();
    assert!(b.drop_index("shop", "orders", "dropped_here").unwrap());
    later();
    index(&a, "w", "fresh");
    let done = repair(&b, &a, BIG);
    assert_eq!(done.definitions.tombstoned, 1);
    assert_eq!(done.definitions.built, 1);
    assert_eq!(names(&b), vec!["fresh".to_string()]);
}

#[test]
fn a_horizon_raised_between_the_check_and_the_build_wins() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "by_v");
    let created = a.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap().hlc;
    let b = std::rc::Rc::new(b);
    let competing = std::rc::Rc::clone(&b);
    let (done, ()) = crate::sync::race_hooks::race(
        crate::sync::race_hooks::Race::DefinitionRepair,
        // A retention pass collected tombstones up to the index's stamp.
        move || competing.test_set_tombstone_horizon(created).unwrap(),
        || repair(&b, &a, BIG),
    );
    assert_eq!(done.definitions, DefinitionRestore { ambiguous: 1, ..Default::default() });
    assert!(names(&b).is_empty());
}

#[test]
fn a_vector_shadow_is_not_repaired() {
    let (a, _b, _d) = pair();
    let shadow = kimmy_core::vector_meta::shadow_name("orders");
    a.create_system_collection("shop", &shadow).unwrap();
    let (c, _dc) = engine();
    c.create_system_collection("shop", &shadow).unwrap();
    later();
    a.create_index("shop", &shadow, vec![IndexField::ascending("v")], false, Some("s".into()))
        .unwrap();
    let id = CollectionId::derive("shop", &shadow);
    let mut progress = SnapshotProgress::of_collection_definitions(id, BIG);
    let page = a.snapshot_page(None, progress.scope()).unwrap();
    let done = c.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    assert_eq!(done.definitions, DefinitionRestore::default());
    assert!(c.list_indexes("shop", &shadow).unwrap().is_empty());
}

/// The standing life's own floor: an index made at or before the drop that began this life
/// belongs to a life this member has buried.
#[test]
fn an_index_at_or_below_the_standing_lifes_floor_is_history() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "by_v");
    later();
    // This member drops and recreates the collection: its floor is above the index's stamp.
    assert!(b.drop_collection("shop", "orders").unwrap());
    b.finish_purges_now().unwrap();
    later();
    b.create_collection("shop", "orders").unwrap();
    let floor = b.get_collection("shop", "orders").unwrap().incarnation_floor.unwrap();
    assert!(a.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap().hlc <= floor);
    let done = repair(&b, &a, BIG);
    assert_eq!(done.definitions, DefinitionRestore { history: 1, ..Default::default() });
    assert!(names(&b).is_empty());
}

/// The same floor raised between the check and the build: the closure judges it again.
#[test]
fn a_floor_raised_between_the_check_and_the_build_wins() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "by_v");
    let b = std::rc::Rc::new(b);
    let competing = std::rc::Rc::clone(&b);
    let (done, ()) = crate::sync::race_hooks::race(
        crate::sync::race_hooks::Race::DefinitionRepair,
        move || {
            later();
            assert!(competing.drop_collection("shop", "orders").unwrap());
            competing.finish_purges_now().unwrap();
            later();
            competing.create_collection("shop", "orders").unwrap();
        },
        || repair(&b, &a, BIG),
    );
    assert_eq!(done.definitions, DefinitionRestore { history: 1, ..Default::default() });
    assert!(names(&b).is_empty());
}

/// This member's own horizon is read for the choice of the one build, not only under the
/// writer: an index at or below it is ambiguous and does not spend the repair's build.
#[test]
fn this_members_own_horizon_is_read_before_the_choice_of_the_build() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "old");
    let old = a.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap().hlc;
    later();
    index(&a, "w", "new");
    b.test_set_tombstone_horizon(old).unwrap();
    let done = repair_with(&b, &a, BIG, |page| page.tombstone_horizon = Some(Hlc::ZERO));
    assert_eq!((done.definitions.ambiguous, done.definitions.built), (1, 1));
    assert_eq!(names(&b), vec!["new".to_string()]);
}

/// A sender that sends no horizon admits nothing: every index is ambiguous, not one.
#[test]
fn a_sender_without_a_horizon_leaves_every_index_ambiguous() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "one");
    later();
    index(&a, "w", "two");
    let done = repair_with(&b, &a, BIG, |page| page.tombstone_horizon = None);
    assert_eq!(done.definitions, DefinitionRestore { ambiguous: 2, ..Default::default() });
    assert!(names(&b).is_empty());
}

/// A rival that lands between the check and the build, older than the page's definition:
/// the replicated route would supersede it by stamp, and the repair must not. The page's
/// definition is given a stamp ahead of this machine's clock so that the rival is older.
#[test]
fn a_rival_older_than_the_pages_definition_is_not_superseded() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "by_v");
    let ahead = Hlc::new(crate::physical_now_ms() + 3_600_000, 0);
    let b = std::rc::Rc::new(b);
    let competing = std::rc::Rc::clone(&b);
    let node = a.node_id();
    let (done, ()) = crate::sync::race_hooks::race(
        crate::sync::race_hooks::Race::DefinitionRepair,
        move || {
            later();
            competing
                .create_index(
                    "shop",
                    "orders",
                    vec![IndexField::ascending("z")],
                    false,
                    Some("by_v".into()),
                )
                .unwrap();
        },
        || {
            repair_with(&b, &a, BIG, move |page| {
                for state in &mut page.collections {
                    for index in &mut state.indexes {
                        index.created = Some(kimmy_core::Stamp::new(ahead, node));
                    }
                }
                page.tombstone_horizon = Some(Hlc::ZERO);
            })
        },
    );
    assert_eq!(done.definitions.built, 0, "{:?}", done.definitions);
    let held = b.get_collection("shop", "orders").unwrap();
    assert_eq!(held.indexes[0].fields, vec![IndexField::ascending("z")], "the rival was replaced");
}

/// An index of the life this member buried does not spend the repair's one build either.
#[test]
fn a_buried_lifes_index_does_not_spend_the_repairs_one_build() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "old");
    later();
    assert!(b.drop_collection("shop", "orders").unwrap());
    b.finish_purges_now().unwrap();
    later();
    b.create_collection("shop", "orders").unwrap();
    later();
    index(&a, "w", "new");
    let done = repair(&b, &a, BIG);
    assert_eq!((done.definitions.history, done.definitions.built), (1, 1));
    assert_eq!(names(&b), vec!["new".to_string()]);
}

/// S dropped life 0 and made life 1 with an index; O holds life 0 (its tombstone aged out).
#[test]
fn an_index_of_the_life_after_a_drop_that_ended_the_standing_life_is_not_built_into_it() {
    let (s, ds) = engine();
    let (o, d_o) = engine();
    let _keep = (ds, d_o);
    s.create_collection("shop", "orders").unwrap();
    pull(&o, &s);
    later();
    assert!(s.drop_collection("shop", "orders").unwrap());
    s.finish_purges_now().unwrap();
    later();
    s.create_collection("shop", "orders").unwrap();
    later();
    index(&s, "v", "by_v");
    let txn = s.begin_write(crate::WriterHolder::Ddl).unwrap();
    txn.open_table(crate::tables::COLLECTIONS_DROPPED).unwrap().remove(orders().0).unwrap();
    txn.commit().unwrap();
    o.absorb_witnessed(&s.witnessed_vector().unwrap()).unwrap();
    let done = repair(&o, &s, BIG);
    assert!(done.history, "the page's life was not recognised as the successor");
    assert_eq!(done.definitions, DefinitionRestore { history: 1, ..Default::default() });
    assert!(names(&o).is_empty(), "life 1's index was built into life 0");
}

/// M2. The definition repair's history clause reads the standing life's floor, which a
/// late-drop sweep sets only in the commit of its last chunk; the sweep removes the old
/// life's definitions first. A definition repair landing between two chunks builds an
/// index of the life the drop ended (created before the drop, so the sweep would have
/// removed it), and the floor then lands over it: the member keeps an index stamped at or
/// below its own floor for good, which no other member holds and no repair removes. The
/// document repair refuses the same window (`owes_a_sweep_in_txn`); this one does not.
#[test]
fn a_definition_repair_between_two_sweep_chunks_builds_no_index_of_the_buried_life() {
    let (a, _da) = engine();
    let (d, _dd) = engine();
    let (e, _de) = engine();
    let on_a = a.create_collection("shop", "orders").unwrap();
    a.insert(&on_a, doc! { "_id": 1i64 }).unwrap();
    pull(&d, &a);
    pull(&e, &a);
    later();
    // The old life gains a plain index; d loses its entry (witnessed, not applied: the
    // later inserts carry its vector past it), e has it.
    a.create_index("shop", "orders", vec![IndexField::ascending("v")], false, Some("old_i".into()))
        .unwrap();
    later();
    // More than a chunk of the life, so the sweep takes two.
    let docs: Vec<_> = (1_000..2_400i64).map(|i| doc! { "_id": i }).collect();
    a.insert_many(&on_a, docs).unwrap();
    let entries =
        a.entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background).unwrap().entries;
    let lost: Vec<_> =
        entries.iter().filter(|x| x.kind != kimmy_core::OpKind::CreateIndex).cloned().collect();
    d.apply_batch(&lost).unwrap();
    pull(&e, &a);
    assert!(d.list_indexes("shop", "orders").unwrap().is_empty(), "premise: d lost the index");
    assert_eq!(e.list_indexes("shop", "orders").unwrap().len(), 1, "premise: e holds it");
    let created = e.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap();
    later();
    // a drops the life; d writes after the drop before it hears of it, so its life is kept
    // (re-dated) and owes the sweep when the drop arrives.
    assert!(a.drop_collection("shop", "orders").unwrap());
    let dropped = a.collection_dropped_at(orders()).unwrap().unwrap();
    later();
    d.create_index("shop", "orders", vec![IndexField::ascending("k")], false, Some("k_1".into()))
        .unwrap();
    // Between the sweep's first and second chunk (the writer released), a definition repair
    // from e, which still holds the old life, runs on d.
    let d = std::sync::Arc::new(d);
    let e = std::sync::Arc::new(e);
    let (on_d, from) = (std::sync::Arc::clone(&d), std::sync::Arc::clone(&e));
    let ran = std::rc::Rc::new(std::cell::Cell::new(None));
    let into = std::rc::Rc::clone(&ran);
    crate::late_drop::hooks::run_after_the_next_chunk(move || {
        let mut progress = SnapshotProgress::of_collection_definitions(orders(), 1_000_000);
        let page = from.snapshot_page(None, progress.scope()).unwrap();
        let done = on_d.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
        into.set(Some(done.definitions.built));
    });
    let start = d.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
    let entries = a.entries_for_peer(start, 10_000, crate::WalkScope::Background).unwrap().entries;
    d.apply_batch(&entries).unwrap();
    let built = ran.get().expect("premise: the sweep ran more than one chunk");
    let kept = d.get_collection("shop", "orders").unwrap();
    assert!(kept.is_redated(), "premise: the life was kept");
    assert_eq!(kept.incarnation_floor, Some(dropped.hlc), "premise: the sweep finished");
    assert!(created.hlc <= dropped.hlc, "premise: the index predates the drop");
    let below: Vec<_> = kept
        .indexes
        .iter()
        .filter(|i| i.created.is_some_and(|c| c.hlc <= dropped.hlc))
        .map(|i| i.name.clone())
        .collect();
    assert!(
        below.is_empty(),
        "the definition repair (built {built}) left an index of the buried life at or below \
         the floor: {below:?}"
    );
}

/// The same definition landing between the check and the build is nothing to repair: it is not
/// a rival and not a refusal.
#[test]
fn the_same_definition_landing_between_the_check_and_the_build_is_nothing() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "by_v");
    let b = std::rc::Rc::new(b);
    let competing = std::rc::Rc::clone(&b);
    let (done, ()) = crate::sync::race_hooks::race(
        crate::sync::race_hooks::Race::DefinitionRepair,
        move || {
            later();
            competing
                .create_index(
                    "shop",
                    "orders",
                    vec![IndexField::ascending("v")],
                    false,
                    Some("by_v".into()),
                )
                .unwrap();
        },
        || repair(&b, &a, BIG),
    );
    assert_eq!(done.definitions, DefinitionRestore::default(), "{:?}", done.definitions);
    assert_eq!(names(&b), vec!["by_v".to_string()]);
}

/// An unstamped rival (a definition older than ADR-132) that lands in the race is a rival,
/// not a refusal: no hint is written, or the next comparison would call the difference
/// explained and the finding would stop being reported.
#[test]
fn an_unstamped_rival_in_the_race_is_a_rival_and_leaves_no_refusal_hint() {
    let (a, b, _d) = pair();
    later();
    index(&a, "v", "by_v");
    let b = std::rc::Rc::new(b);
    let competing = std::rc::Rc::clone(&b);
    let (done, ()) = crate::sync::race_hooks::race(
        crate::sync::race_hooks::Race::DefinitionRepair,
        move || {
            competing
                .create_index_inner(
                    "shop",
                    "orders",
                    vec![IndexField::ascending("z")],
                    false,
                    crate::Enforcement::Local,
                    Some("by_v".into()),
                    None,
                    None,
                    crate::index::CreateOrigin::Replicated { created: None, logged: None },
                    &|_, _| false,
                )
                .unwrap();
        },
        || repair(&b, &a, BIG),
    );
    assert_eq!(done.definitions.refused, 0, "{:?}", done.definitions);
    assert_eq!(done.definitions.built, 0);
    assert!(b.refusal_hints(orders()).unwrap().is_empty(), "a rival was written as a refusal");
}
