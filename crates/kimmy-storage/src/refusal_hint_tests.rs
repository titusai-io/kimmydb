//! Refusal hints (B3 half 2, design 3.5): where they are written, what is in
//! them, where they go, and that a refusal that cannot be remembered fails the
//! batch and is never swallowed.

use bson::{doc, spec::BinarySubtype};
use kimmy_core::{CollectionId, IndexField, OpKind, Stamp, VectorConfig};

use crate::definition_digest::{DefinitionKey, index_hash, vector_hash};
use crate::refusal_hint::HintKind;
use crate::{Engine, SnapshotProgress, WalkScope};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn pull(into: &Engine, from: &Engine) -> crate::SyncOutcome {
    let mine = into.version_vector().unwrap();
    let theirs = from.version_vector().unwrap();
    match mine.behind(&theirs) {
        Some(start) => {
            let entries =
                from.entries_for_peer(start, 1024, WalkScope::Background).unwrap().entries;
            into.apply_batch(&entries).unwrap()
        }
        None => crate::SyncOutcome::default(),
    }
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn field(path: &str) -> Vec<IndexField> {
    vec![IndexField::ascending(path)]
}

fn local_config() -> VectorConfig {
    // A provider this build cannot hold: `local` needs a feature the default
    // build does not have, and a replicated configuration naming it is refused.
    VectorConfig {
        fields: vec!["text".into()],
        provider: kimmy_core::ProviderConfig::Local { model: "m".into() },
        dim: 4,
        metric: Default::default(),
        document_prefix: None,
        query_prefix: None,
        chunk: Default::default(),
    }
}

/// A index definition's hash is the same wherever it is computed: from the entry
/// body the origin sent, from the origin's stored index after the JSON round
/// trip, and from the receiver's stored index. A hint stores the first and the
/// peer lists the third, so a refusal is only ever explained if these agree, for
/// a filter with a nested document, a Long and a Date.
#[test]
fn a_refused_definitions_hash_is_the_one_the_origin_computes_from_its_stored_index() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.create_collection("shop", "orders").unwrap();
    b.create_collection("shop", "orders").unwrap();
    let filter = doc! {
        "k": { "a": 1, "b": "x" },
        "n": { "$gte": 5_i64 },
        "when": { "$gte": bson::DateTime::from_millis(86_400_000) },
        "blob": bson::Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2] },
    };
    a.create_index_with(
        "shop",
        "orders",
        field("n"),
        false,
        kimmy_core::Enforcement::Local,
        Some("filtered".into()),
        None,
        Some(filter),
    )
    .unwrap();
    let origin = index_hash(a.get_collection("shop", "orders").unwrap().index("filtered").unwrap());
    let start = b.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
    let entries = a.entries_for_peer(start, 1024, WalkScope::Background).unwrap().entries;
    let entry = entries.iter().find(|e| e.kind == OpKind::CreateIndex).unwrap();
    let body: kimmy_core::IndexCreate =
        bson::deserialize_from_slice(entry.body.as_ref().unwrap()).unwrap();
    assert_eq!(index_hash(&body.index), origin, "the entry body");
    b.apply_batch(&entries).unwrap();
    let received =
        index_hash(b.get_collection("shop", "orders").unwrap().index("filtered").unwrap());
    assert_eq!(received, origin, "the receiver's stored index");
}

/// Site one: a replicated `CreateIndex` this member refuses. Here a rival it
/// cannot arbitrate (the held definition has no stamp to compare). The hint holds
/// the hash of what was refused, the entry's stamp and this build's version.
#[test]
fn a_replicated_index_refused_leaves_a_hint_with_the_hash_of_what_was_refused() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.create_collection("shop", "orders").unwrap();
    b.create_collection("shop", "orders").unwrap();
    b.create_index("shop", "orders", field("email"), false, Some("by_email".into())).unwrap();
    let mut meta = b.get_collection("shop", "orders").unwrap();
    meta.indexes[0].created = None;
    {
        let txn = b.begin_write(crate::WriterHolder::Ddl).unwrap();
        Engine::put_collection_meta(&txn, &meta).unwrap();
        txn.commit().unwrap();
    }
    later();
    let refused =
        a.create_index("shop", "orders", field("email"), true, Some("by_email".into())).unwrap();
    let outcome = pull(&b, &a);
    assert_eq!(outcome.ddl_refused, 1, "premise: refused: {outcome:?}");
    let hints = b.refusal_hints(CollectionId::derive("shop", "orders")).unwrap();
    assert_eq!(hints.len(), 1, "{hints:?}");
    assert_eq!(hints[0].kind, HintKind::Index);
    assert_eq!(hints[0].name, "by_email");
    assert_eq!(hints[0].hash, index_hash(&refused), "the refused definition, not the held one");
    assert_eq!(Some(hints[0].stamp), refused.created);
    assert_eq!(hints[0].version, env!("CARGO_PKG_VERSION"));
}

/// Site two: a replicated `ConfigureVectors` this member cannot apply.
#[test]
fn a_replicated_vector_configuration_refused_leaves_a_hint_under_the_reserved_name() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let on_a = a.create_collection("shop", "orders").unwrap();
    b.create_collection("shop", "orders").unwrap();
    later();
    // An entry a peer on another build could send: this build refuses it.
    let stamp = a.next_stamp();
    let entry = crate::engine::ddl_entry(
        stamp,
        OpKind::ConfigureVectors,
        on_a.id,
        &kimmy_core::VectorSet {
            db: "shop".into(),
            collection: "orders".into(),
            config: Some(local_config()),
        },
    )
    .unwrap();
    let outcome = b.apply_batch(std::slice::from_ref(&entry)).unwrap();
    assert_eq!(outcome.ddl_refused, 1, "premise: refused: {outcome:?}");
    let hints = b.refusal_hints(on_a.id).unwrap();
    assert_eq!(hints.len(), 1, "{hints:?}");
    assert_eq!((hints[0].kind, hints[0].name.as_str()), (HintKind::Vector, ""));
    assert_eq!(hints[0].hash, vector_hash(&local_config()));
    assert_eq!(hints[0].stamp, stamp);
}

/// Site three, the opposite shape: a peer's drop of an index this member holds
/// and could not apply leaves a `drop` hint holding the hash of the index this
/// member holds. (A replicated drop is refused in no way this build reaches, so
/// the writer is called as the arm calls it.)
#[test]
fn a_refused_drop_of_an_index_this_member_holds_leaves_a_drop_hint_with_the_held_hash() {
    let (b, _db) = engine();
    b.create_collection("shop", "orders").unwrap();
    b.create_index("shop", "orders", field("a"), false, Some("held".into())).unwrap();
    let held = index_hash(b.get_collection("shop", "orders").unwrap().index("held").unwrap());
    let stamp = Stamp::new(kimmy_core::Hlc::new(900, 0), kimmy_core::NodeId::from_bytes([5; 16]));
    b.note_refused_index_drop("shop", "orders", "held", stamp).unwrap();
    // Nothing held, nothing to explain: no hint for a drop of an index not here.
    b.note_refused_index_drop("shop", "orders", "absent", stamp).unwrap();
    let hints = b.refusal_hints(CollectionId::derive("shop", "orders")).unwrap();
    assert_eq!(hints.len(), 1, "{hints:?}");
    assert_eq!(
        (hints[0].kind, hints[0].name.as_str(), hints[0].hash),
        (HintKind::DropIndex, "held", held)
    );
    // And a refused turning-off of a configuration this member holds is the
    // same shape, under the reserved name.
    b.configure_vectors("shop", "orders", crate::definition_probe_tests::byo()).unwrap();
    b.note_refused_vector("shop", "orders", None, stamp).unwrap();
    let hints = b.refusal_hints(CollectionId::derive("shop", "orders")).unwrap();
    let vector = hints
        .iter()
        .find(|h| h.kind.definition() == crate::definition_digest::DefinitionKind::Vector)
        .expect("a vector hint");
    assert_eq!(vector.kind, HintKind::DropVector);
    assert_eq!(vector.hash, vector_hash(&crate::definition_probe_tests::byo()));
}

/// Site four: an index in a snapshot page this member cannot build.
#[test]
fn a_snapshot_index_refused_leaves_a_hint() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.create_collection("shop", "orders").unwrap();
    a.create_index(
        "shop",
        "orders",
        vec![IndexField::ascending("tags"), IndexField::ascending("cats")],
        false,
        None,
    )
    .unwrap();
    b.create_collection("shop", "orders").unwrap();
    let mut page = a.snapshot_page(None, None).unwrap();
    for state in &mut page.collections {
        for index in &mut state.indexes {
            // A TTL over two fields: what no member can mint and a page from
            // another build could carry.
            index.expire_after_secs = Some(60);
        }
    }
    let carried = page.collections[0].indexes[0].clone();
    let outcome =
        b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    assert_eq!(outcome.ddl_refused, 1, "premise: refused: {outcome:?}");
    let hints = b.refusal_hints(CollectionId::derive("shop", "orders")).unwrap();
    assert_eq!(hints.len(), 1, "{hints:?}");
    assert_eq!((hints[0].kind, hints[0].hash), (HintKind::Index, index_hash(&carried)));
    assert_eq!(Some(hints[0].stamp), carried.created);
}

/// Site five: a vector configuration in a snapshot page this member cannot hold.
#[test]
fn a_snapshot_vector_configuration_refused_leaves_a_hint() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.create_collection("shop", "orders").unwrap();
    a.configure_vectors("shop", "orders", crate::definition_probe_tests::byo()).unwrap();
    b.create_collection("shop", "orders").unwrap();
    let mut page = a.snapshot_page(None, None).unwrap();
    for state in &mut page.collections {
        if state.name == "orders" {
            state.vector = Some(local_config());
        }
    }
    let stamp =
        page.collections.iter().find(|s| s.name == "orders").unwrap().vector_changed.unwrap();
    let outcome =
        b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    assert_eq!(outcome.ddl_refused, 1, "premise: refused: {outcome:?}");
    let hints = b.refusal_hints(CollectionId::derive("shop", "orders")).unwrap();
    let hint = hints.iter().find(|h| h.key() == DefinitionKey::vector()).expect("a vector hint");
    assert_eq!(
        (hint.kind, hint.hash, hint.stamp),
        (HintKind::Vector, vector_hash(&local_config()), stamp)
    );
}

/// A hint that cannot be written fails the batch: the entry is not witnessed and
/// is served again. Never swallowed, or the refusal would stay silent for good.
#[test]
fn a_hint_that_cannot_be_written_fails_the_batch_and_the_entry_is_served_again() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let on_a = a.create_collection("shop", "orders").unwrap();
    b.create_collection("shop", "orders").unwrap();
    later();
    let stamp = a.next_stamp();
    let entry = crate::engine::ddl_entry(
        stamp,
        OpKind::ConfigureVectors,
        on_a.id,
        &kimmy_core::VectorSet {
            db: "shop".into(),
            collection: "orders".into(),
            config: Some(local_config()),
        },
    )
    .unwrap();
    b.fail_next_hint_write_for_test();
    let result = b.apply_batch(std::slice::from_ref(&entry));
    assert!(result.is_err(), "the batch fails: {result:?}");
    assert!(b.refusal_hints(on_a.id).unwrap().is_empty(), "and nothing was recorded");
    // Not witnessed: delivered again, it is refused again and now remembered.
    let outcome = b.apply_batch(std::slice::from_ref(&entry)).unwrap();
    assert_eq!(outcome.ddl_refused, 1, "{outcome:?}");
    assert_eq!(b.refusal_hints(on_a.id).unwrap().len(), 1);
}

/// The hints of a collection go when it is buried, and those of an index when
/// that index is dropped; the neighbours' stay.
#[test]
fn hints_are_cleared_with_the_purge_and_with_the_drop_of_their_index() {
    let (b, _db) = engine();
    let orders = b.create_collection("shop", "orders").unwrap();
    let items = b.create_collection("shop", "items").unwrap();
    b.create_index("shop", "orders", field("a"), false, Some("a_1".into())).unwrap();
    let stamp = Stamp::new(kimmy_core::Hlc::new(900, 0), kimmy_core::NodeId::from_bytes([5; 16]));
    for (collection, name) in [(orders.id, "a_1"), (orders.id, "other"), (items.id, "x")] {
        b.record_refusal_hint(&crate::RefusalHint::new(
            collection,
            HintKind::Index,
            name,
            stamp,
            1,
        ))
        .unwrap();
    }
    // The drop of an index clears the hints about that name only.
    assert!(b.drop_index("shop", "orders", "a_1").unwrap());
    let names: Vec<_> = b.refusal_hints(orders.id).unwrap().into_iter().map(|h| h.name).collect();
    assert_eq!(names, vec!["other".to_string()]);
    // The burial of a collection clears all of its hints.
    assert!(b.drop_collection("shop", "orders").unwrap());
    b.finish_purges_now().unwrap();
    assert!(b.refusal_hints(orders.id).unwrap().is_empty(), "gone with the collection");
    assert_eq!(b.refusal_hints(items.id).unwrap().len(), 1, "the neighbour's stays");
}

/// A backup carries `META` rows whatever their key, so a restored store holds its
/// hints; and they are never trusted without being re-derived.
#[test]
fn a_hint_is_a_meta_row_and_so_a_backup_carries_it() {
    let (b, dir) = engine();
    let orders = b.create_collection("shop", "orders").unwrap();
    let stamp = Stamp::new(kimmy_core::Hlc::new(900, 0), kimmy_core::NodeId::from_bytes([5; 16]));
    b.record_refusal_hint(&crate::RefusalHint::new(orders.id, HintKind::Index, "x", stamp, 7))
        .unwrap();
    let _ = &dir;
    let mut bytes = Vec::new();
    b.backup_to(&mut bytes, WalkScope::Background).unwrap();
    let restored_dir = tempfile::tempdir().unwrap();
    let target = restored_dir.path().join("kimmy.redb");
    crate::backup::restore(&target, &mut bytes.as_slice()).unwrap();
    let restored = Engine::open(&target).unwrap();
    assert_eq!(restored.refusal_hints(orders.id).unwrap().len(), 1);
}

/// The same for an index refusal: the replicated `CreateIndex` is not witnessed
/// when its hint cannot be written, and is served again.
#[test]
fn an_index_refusal_whose_hint_cannot_be_written_fails_the_batch_too() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.create_collection("shop", "orders").unwrap();
    b.create_collection("shop", "orders").unwrap();
    b.create_index("shop", "orders", field("email"), false, Some("by_email".into())).unwrap();
    let mut meta = b.get_collection("shop", "orders").unwrap();
    meta.indexes[0].created = None;
    {
        let txn = b.begin_write(crate::WriterHolder::Ddl).unwrap();
        Engine::put_collection_meta(&txn, &meta).unwrap();
        txn.commit().unwrap();
    }
    later();
    a.create_index("shop", "orders", field("email"), true, Some("by_email".into())).unwrap();
    let start = b.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
    let entries = a.entries_for_peer(start, 1024, WalkScope::Background).unwrap().entries;
    let id = CollectionId::derive("shop", "orders");
    b.fail_next_hint_write_for_test();
    let result = b.apply_batch(&entries);
    assert!(result.is_err(), "the batch fails: {result:?}");
    assert!(b.refusal_hints(id).unwrap().is_empty(), "and nothing was recorded");
    let outcome = b.apply_batch(&entries).unwrap();
    assert_eq!(outcome.ddl_refused, 1, "{outcome:?}");
    assert_eq!(b.refusal_hints(id).unwrap().len(), 1);
}

/// A newer refusal replaces an older one for the same name, and an older never
/// replaces a newer.
#[test]
fn an_older_refusal_never_replaces_a_newer_one() {
    let (b, _db) = engine();
    let orders = b.create_collection("shop", "orders").unwrap();
    let node = kimmy_core::NodeId::from_bytes([5; 16]);
    let at = |wall: u64| Stamp::new(kimmy_core::Hlc::new(wall, 0), node);
    let hint = |wall: u64, hash: u128| {
        crate::RefusalHint::new(orders.id, HintKind::Index, "x", at(wall), hash)
    };
    b.record_refusal_hint(&hint(900, 2)).unwrap();
    b.record_refusal_hint(&hint(800, 1)).unwrap();
    let held = b.refusal_hints(orders.id).unwrap();
    assert_eq!((held[0].stamp, held[0].hash), (at(900), 2), "the older one was refused");
    b.record_refusal_hint(&hint(1_000, 3)).unwrap();
    let held = b.refusal_hints(orders.id).unwrap();
    assert_eq!((held.len(), held[0].stamp, held[0].hash), (1, at(1_000), 3));
}

/// A collection's hints are the keys under its own id and no other: an id that
/// begins another's digits is not in its range, and clearing one leaves the other.
#[test]
fn a_collections_hint_range_does_not_reach_a_neighbour_whose_id_begins_the_same() {
    let (b, _db) = engine();
    let stamp = Stamp::new(kimmy_core::Hlc::new(900, 0), kimmy_core::NodeId::from_bytes([5; 16]));
    let (short, long) = (CollectionId(1), CollectionId(10));
    for id in [short, long] {
        b.record_refusal_hint(&crate::RefusalHint::new(id, HintKind::Index, "x", stamp, 1))
            .unwrap();
    }
    assert_eq!(b.refusal_hints(short).unwrap().len(), 1, "only its own");
    let txn = b.begin_write(crate::WriterHolder::Ddl).unwrap();
    assert_eq!(Engine::clear_refusal_hints_in(&txn, short).unwrap(), 1);
    txn.commit().unwrap();
    assert_eq!(b.refusal_hints(long).unwrap().len(), 1, "the neighbour's stays");
}

/// The burial of a collection with a vector configuration clears the hints of its
/// shadow as well: the shadow is a collection of its own with its own id.
#[test]
fn the_burial_clears_the_hints_of_the_collections_shadow() {
    let (b, _db) = engine();
    let orders = b.create_collection("shop", "orders").unwrap();
    b.configure_vectors("shop", "orders", crate::definition_probe_tests::byo()).unwrap();
    let shadow = CollectionId::derive("shop", &kimmy_core::vector_meta::shadow_name("orders"));
    let stamp = Stamp::new(kimmy_core::Hlc::new(900, 0), kimmy_core::NodeId::from_bytes([5; 16]));
    b.record_refusal_hint(&crate::RefusalHint::new(shadow, HintKind::Index, "x", stamp, 1))
        .unwrap();
    assert_eq!(b.refusal_hints(shadow).unwrap().len(), 1, "premise");
    assert!(b.drop_collection("shop", "orders").unwrap());
    b.finish_purges_now().unwrap();
    assert!(b.refusal_hints(orders.id).unwrap().is_empty());
    assert!(b.refusal_hints(shadow).unwrap().is_empty(), "the shadow's went with it");
}

// -- An identical hint is not a commit (ADR-125: no commit trickle) --------------------

/// A refusal that is met again on every round (a permanent one, re-met on each
/// repair or catch-up page) must not commit and fsync an identical row each time.
#[test]
fn an_identical_hint_is_no_commit_and_a_different_one_is() {
    let (b, _db) = engine();
    let orders = b.create_collection("shop", "orders").unwrap();
    let node = kimmy_core::NodeId::from_bytes([5; 16]);
    let at = |wall: u64| Stamp::new(kimmy_core::Hlc::new(wall, 0), node);
    let hint = |wall: u64, hash: u128| {
        crate::RefusalHint::new(orders.id, HintKind::Index, "x", at(wall), hash)
    };
    let before = b.commits();
    b.record_refusal_hint(&hint(900, 1)).unwrap();
    assert_eq!(b.commits(), before + 1, "the first one commits");
    b.record_refusal_hint(&hint(900, 1)).unwrap();
    assert_eq!(b.commits(), before + 1, "an identical repeat commits nothing");
    b.record_refusal_hint(&hint(900, 2)).unwrap();
    assert_eq!(b.commits(), before + 2, "another hash is written");
    b.record_refusal_hint(&hint(1_000, 2)).unwrap();
    assert_eq!(b.commits(), before + 3, "a newer stamp is written");
    let mut other_build = hint(1_000, 2);
    other_build.version = "0.0.0-other".into();
    b.record_refusal_hint(&other_build).unwrap();
    assert_eq!(b.commits(), before + 4, "another build is written");
}

/// The three writers the refusal sites call: each commits once and not again for
/// the same refusal.
#[test]
fn a_refusal_met_again_commits_its_hint_once_at_every_writer() {
    let (b, _db) = engine();
    b.create_collection("shop", "orders").unwrap();
    b.create_index("shop", "orders", field("held"), false, Some("held".into())).unwrap();
    b.configure_vectors("shop", "orders", crate::definition_probe_tests::byo()).unwrap();
    let stamp = Stamp::new(kimmy_core::Hlc::new(900, 0), kimmy_core::NodeId::from_bytes([5; 16]));
    let index = b.get_collection("shop", "orders").unwrap().index("held").unwrap().clone();
    let id = CollectionId::derive("shop", "orders");
    type Note = Box<dyn Fn(&Engine) -> crate::Result<()>>;
    let notes: Vec<(&str, Note)> = vec![
        ("an index", Box::new(move |e| e.note_refused_index(id, &index, stamp))),
        ("a vector configuration", {
            let config = local_config();
            Box::new(move |e| e.note_refused_vector("shop", "orders", Some(&config), stamp))
        }),
        (
            "a vector turning-off",
            Box::new(move |e| e.note_refused_vector("shop", "orders", None, stamp)),
        ),
        (
            "an index drop",
            Box::new(move |e| e.note_refused_index_drop("shop", "orders", "held", stamp)),
        ),
    ];
    for (what, note) in notes {
        let before = b.commits();
        note(&b).unwrap();
        assert_eq!(b.commits(), before + 1, "{what}: the first commits");
        note(&b).unwrap();
        note(&b).unwrap();
        assert_eq!(b.commits(), before + 1, "{what}: a repeat commits nothing");
    }
}

/// A replicated refused entry delivered again leaves its hint as it was and
/// commits no hint: the first delivery costs the hint's commit more than the next.
#[test]
fn a_re_delivered_refused_entry_rewrites_no_hint() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let on_a = a.create_collection("shop", "orders").unwrap();
    b.create_collection("shop", "orders").unwrap();
    later();
    let entry = crate::engine::ddl_entry(
        a.next_stamp(),
        OpKind::ConfigureVectors,
        on_a.id,
        &kimmy_core::VectorSet {
            db: "shop".into(),
            collection: "orders".into(),
            config: Some(local_config()),
        },
    )
    .unwrap();
    let start = b.commits();
    assert_eq!(b.apply_batch(std::slice::from_ref(&entry)).unwrap().ddl_refused, 1);
    let first = b.commits() - start;
    let hints = b.refusal_hints(on_a.id).unwrap();
    let start = b.commits();
    for _ in 0..2 {
        assert_eq!(b.apply_batch(std::slice::from_ref(&entry)).unwrap().ddl_refused, 1);
    }
    assert_eq!(b.refusal_hints(on_a.id).unwrap(), hints, "the row is as it was");
    assert_eq!((b.commits() - start) / 2 + 1, first, "a repeat costs the hint's commit less");
}

// -- A hint is mended with the definition it was about ---------------------------------

fn both_shapes(e: &Engine, id: CollectionId, kind: HintKind, names: &[&str]) {
    let stamp = Stamp::new(kimmy_core::Hlc::new(900, 0), kimmy_core::NodeId::from_bytes([5; 16]));
    for name in names {
        e.record_refusal_hint(&crate::RefusalHint::new(id, kind, *name, stamp, 7)).unwrap();
    }
}

fn held_names(e: &Engine, id: CollectionId) -> Vec<(HintKind, String)> {
    e.refusal_hints(id).unwrap().into_iter().map(|h| (h.kind, h.name)).collect()
}

/// The drop of an index clears its hints of every kind, and no other index's.
#[test]
fn dropping_an_index_clears_its_hints_of_every_kind() {
    let (b, _db) = engine();
    let orders = b.create_collection("shop", "orders").unwrap();
    b.create_index("shop", "orders", field("a"), false, Some("a_1".into())).unwrap();
    both_shapes(&b, orders.id, HintKind::Index, &["a_1", "keep"]);
    both_shapes(&b, orders.id, HintKind::DropIndex, &["a_1"]);
    assert!(b.drop_index("shop", "orders", "a_1").unwrap());
    assert_eq!(held_names(&b, orders.id), vec![(HintKind::Index, "keep".to_string())]);
}

/// The drop of an index this member does not hold records a tombstone and nothing
/// else of the index, but it is still the drop of the name: the name's hints of
/// every kind go with it, on the local path and on the replicated one, and no
/// other name's do.
#[test]
fn dropping_an_index_this_member_does_not_hold_clears_its_hints_of_every_kind() {
    // Local: a client's drop of a name this member never held.
    let (b, _db) = engine();
    let orders = b.create_collection("shop", "orders").unwrap();
    both_shapes(&b, orders.id, HintKind::Index, &["ghost", "keep"]);
    both_shapes(&b, orders.id, HintKind::DropIndex, &["ghost"]);
    assert!(!b.drop_index("shop", "orders", "ghost").unwrap(), "premise: not held");
    assert_eq!(held_names(&b, orders.id), vec![(HintKind::Index, "keep".to_string())]);

    // Replicated: a peer's drop entry, the create it ends never having arrived.
    let (a, _da) = engine();
    let (c, _dc) = engine();
    a.create_collection("shop", "orders").unwrap();
    c.create_collection("shop", "orders").unwrap();
    let id = CollectionId::derive("shop", "orders");
    a.create_index("shop", "orders", field("x"), false, Some("x".into())).unwrap();
    later();
    a.drop_index("shop", "orders", "x").unwrap();
    both_shapes(&c, id, HintKind::Index, &["x", "keep"]);
    both_shapes(&c, id, HintKind::DropIndex, &["x"]);
    let start = c.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
    let entries = a.entries_for_peer(start, 1024, WalkScope::Background).unwrap().entries;
    let drops: Vec<_> = entries.into_iter().filter(|e| e.kind == OpKind::DropIndex).collect();
    assert_eq!(drops.len(), 1);
    c.apply_batch(&drops).unwrap();
    assert!(c.get_collection("shop", "orders").unwrap().index("x").is_none(), "premise");
    assert_eq!(held_names(&c, id), vec![(HintKind::Index, "keep".to_string())]);
}

/// A create of the definition here (a client's, a peer's entry, a snapshot's) is
/// the fix of the refusal: its hints, of either shape, go in the transaction that
/// stores it, and an index of another name keeps its own.
#[test]
fn creating_a_definition_clears_its_hints_by_every_route() {
    // A client's create.
    let (b, _db) = engine();
    let orders = b.create_collection("shop", "orders").unwrap();
    both_shapes(&b, orders.id, HintKind::Index, &["x", "other"]);
    both_shapes(&b, orders.id, HintKind::DropIndex, &["x"]);
    b.create_index("shop", "orders", field("x"), false, Some("x".into())).unwrap();
    assert_eq!(held_names(&b, orders.id), vec![(HintKind::Index, "other".to_string())]);

    // A peer's entry.
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.create_collection("shop", "orders").unwrap();
    b.create_collection("shop", "orders").unwrap();
    both_shapes(&b, CollectionId::derive("shop", "orders"), HintKind::Index, &["y", "other"]);
    later();
    a.create_index("shop", "orders", field("y"), false, Some("y".into())).unwrap();
    pull(&b, &a);
    assert_eq!(
        held_names(&b, CollectionId::derive("shop", "orders")),
        vec![(HintKind::Index, "other".to_string())]
    );

    // A snapshot's.
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.create_collection("shop", "orders").unwrap();
    a.create_index("shop", "orders", field("z"), false, Some("z".into())).unwrap();
    b.create_collection("shop", "orders").unwrap();
    both_shapes(&b, CollectionId::derive("shop", "orders"), HintKind::Index, &["z", "other"]);
    let page = a.snapshot_page(None, None).unwrap();
    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    assert!(b.get_collection("shop", "orders").unwrap().index("z").is_some(), "premise");
    assert_eq!(
        held_names(&b, CollectionId::derive("shop", "orders")),
        vec![(HintKind::Index, "other".to_string())]
    );
}

/// The vector configuration is its own definition: setting it, or turning it off,
/// clears the hints of both shapes about it, and an index of any name keeps its own.
#[test]
fn setting_or_turning_off_the_vector_configuration_clears_its_hints() {
    let (b, _db) = engine();
    let orders = b.create_collection("shop", "orders").unwrap();
    for turn_off in [false, true] {
        both_shapes(&b, orders.id, HintKind::Vector, &[""]);
        both_shapes(&b, orders.id, HintKind::DropVector, &[""]);
        both_shapes(&b, orders.id, HintKind::Index, &["__vector"]);
        if turn_off {
            b.disable_vectors("shop", "orders", false).unwrap();
        } else {
            b.configure_vectors("shop", "orders", crate::definition_probe_tests::byo()).unwrap();
        }
        assert_eq!(
            held_names(&b, orders.id),
            vec![(HintKind::Index, "__vector".to_string())],
            "turn_off {turn_off}"
        );
        let txn = b.begin_write(crate::WriterHolder::Ddl).unwrap();
        Engine::clear_refusal_hints_for_in(&txn, orders.id, &DefinitionKey::index("__vector"))
            .unwrap();
        txn.commit().unwrap();
    }
}

/// A row the build cannot read is no error to read the rest, nor to clear.
#[test]
fn an_unreadable_row_is_skipped_by_the_read_and_removed_by_the_collections_clear() {
    let (b, _db) = engine();
    let orders = b.create_collection("shop", "orders").unwrap();
    both_shapes(&b, orders.id, HintKind::Index, &["x"]);
    {
        let txn = b.begin_write(crate::WriterHolder::Ddl).unwrap();
        {
            let mut meta = txn.open_table(crate::tables::META).unwrap();
            let key = format!("ddl_refused:{}:future:x", orders.id.0);
            meta.insert(key.as_str(), [1u8, 2, 3].as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }
    assert_eq!(held_names(&b, orders.id), vec![(HintKind::Index, "x".to_string())]);
    let txn = b.begin_write(crate::WriterHolder::Ddl).unwrap();
    assert_eq!(Engine::clear_refusal_hints_in(&txn, orders.id).unwrap(), 2, "both rows go");
    txn.commit().unwrap();
}

/// A definition the member holds identically, whose stamp is raised to a peer's
/// newer one (the merge-forward of a concurrent identical create), is settled
/// here: its hints go with the stamp.
#[test]
fn settling_an_identical_definition_on_a_newer_stamp_clears_its_hints() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.create_collection("shop", "orders").unwrap();
    b.create_collection("shop", "orders").unwrap();
    let id = CollectionId::derive("shop", "orders");
    b.create_index("shop", "orders", field("x"), false, Some("x".into())).unwrap();
    later();
    a.create_index("shop", "orders", field("x"), false, Some("x".into())).unwrap();
    both_shapes(&b, id, HintKind::Index, &["x", "other"]);
    let before = b.get_collection("shop", "orders").unwrap().index("x").unwrap().created;
    pull(&b, &a);
    let after = b.get_collection("shop", "orders").unwrap().index("x").unwrap().created;
    assert!(after > before, "premise: the stamp was raised to the newer");
    assert_eq!(held_names(&b, id), vec![(HintKind::Index, "other".to_string())]);
}
