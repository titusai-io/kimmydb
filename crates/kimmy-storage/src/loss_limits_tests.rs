//! A collection's loss limits (`K` records per document, a cut for each value) reach every storage route that joins a state
//! or receives a record (the Step 4 design, C6b). Every test makes its collection with limits that are NOT the node's
//! default (K = 5 and a cut of 100 bytes, where the default is 4 and 64), so a route that silently built its options from the
//! default would keep four records and fail.

use std::cmp::Ordering;

use bson::{Bson, Document, doc};
use kimmy_core::merge_state::{
    LossKey, LossKind, LossValue, MStamp, MergeState, Path, Value, WriterId,
};
use kimmy_core::{DocId, Hlc, LossLimits, MergePolicy, OpKind, OplogEntry, Stamp};

use crate::{Candidates, CollectionMeta, Engine, ModifySpec, SnapshotProgress, WalkScope, tables};

const K: usize = 5;

fn limits() -> LossLimits {
    LossLimits { cap: K as u16, cut_bytes: 100 }
}

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn orders() -> kimmy_core::CollectionId {
    kimmy_core::CollectionId::derive("shop", "orders")
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

/// Two members of one `field` collection made with `limits()`: the first creates it, the second takes the definition from its
/// entries, and keeps the node default (K = 4) for any collection it would make itself.
fn pair() -> (Engine, Engine, CollectionMeta, CollectionMeta, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    assert_ne!(
        a.default_loss_limits(),
        limits(),
        "premise: the node default is not the collection's"
    );
    a.set_default_loss_limits(limits()).unwrap();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    assert_eq!(
        cb.loss_limits,
        Some(limits()),
        "premise: the definition carries the limits to the second member"
    );
    assert_eq!(
        b.default_loss_limits(),
        LossLimits::DEFAULT,
        "premise: and the second member's own default is another"
    );
    (a, b, ca, cb, [da, db])
}

fn writer_of(e: &Engine) -> WriterId {
    WriterId { node: e.node_id(), epoch: e.epoch() }
}

/// The key of a clash at `x` whose loser was stamped `wall` by `writer`. Records rank by their loser's stamp first.
fn key_at(writer: WriterId, wall: u64) -> LossKey {
    LossKey::field(
        LossKind::Clash,
        Path::flat("x").unwrap(),
        MStamp { hlc: Hlc::new(wall, 0), writer },
    )
}

/// The `n` walls of the records of a test, ascending, just ahead of now.
fn walls(n: usize) -> Vec<u64> {
    let base = crate::physical_now_ms() + 10;
    (0..n as u64).map(|i| base + 2 * i).collect()
}

/// The entry `from` appends for the record of the document `1` that lost at `wall`.
fn loss_entry(from: &Engine, coll: &CollectionMeta, wall: u64) -> (LossKey, OplogEntry) {
    let key = key_at(writer_of(from), wall);
    let entry = OplogEntry {
        stamp: Stamp::new(Hlc::new(wall + 1, 0), from.node_id(), from.epoch()),
        kind: OpKind::LossRecord,
        collection: coll.id,
        doc_id: Some(DocId::Int64(1)),
        body: Some(MergeState::encode_loss_record(&key, &LossValue::Value(Value::Int64(7)))),
        meta: None,
    };
    (key, entry)
}

fn stored_state(from: &Engine) -> MergeState {
    let page = from.snapshot_page(None, Some(orders())).unwrap();
    assert_eq!(page.documents.len(), 1, "premise: one row");
    crate::field_write::load(page.documents[0].meta.as_deref()).unwrap()
}

fn held(e: &Engine) -> Vec<LossKey> {
    stored_state(e).losses.keys().cloned().collect()
}

fn refused(e: &Engine, reason: &str) -> u64 {
    e.loss_records_refused().into_iter().find(|(label, _)| *label == reason).unwrap().1
}

/// Put `records` into the stored state of the one document of `e` by hand, as a state that came with more than `K` of them
/// would hold them (a state is joined and capped when it arrives; this is what a store written by a build without the cap,
/// or by an attacker, could hold).
fn put_records(e: &Engine, coll: &CollectionMeta, records: &[LossKey]) {
    let key = crate::docs::doc_key(&DocId::Int64(1)).unwrap();
    let txn = e.begin_write(crate::engine::WriterHolder::Write).unwrap();
    {
        use redb::ReadableTable;
        let mut docs = txn.open_table(tables::DOCS).unwrap();
        let raw = docs.get((coll.id.0, key.as_slice())).unwrap().unwrap().value().to_vec();
        let held = crate::codec::decode_doc_record_with_meta(&raw).unwrap();
        let mut state = crate::field_write::load(held.meta.as_deref()).unwrap();
        state.losses.clear();
        for r in records {
            state.losses.insert(r.clone(), LossValue::Value(Value::Int64(7)));
        }
        let mut stored = kimmy_core::DocRecord::live(held.stamp, held.body.clone());
        stored.meta = Some(state.encode_compact());
        crate::live_count::put_record(
            &txn,
            &mut docs,
            coll.id.0,
            &key,
            &crate::codec::encode_doc_record(&stored),
        )
        .unwrap();
    }
    txn.commit().unwrap();
}

/// The `K` greatest of six records (the records rank by the wall of their loser's stamp), and the sixth, which is the least.
fn the_k_greatest(six: &[LossKey]) -> Vec<LossKey> {
    let mut sorted = six.to_vec();
    sorted.sort_by(|x, y| x.rank_cmp(y));
    sorted.split_off(sorted.len() - K)
}

fn six_keys(from: &Engine) -> Vec<LossKey> {
    walls(K + 1).into_iter().map(|w| key_at(writer_of(from), w)).collect()
}

// ---- route 4: a loss entry received ---------------------------------------------------------------

#[test]
fn route_4_a_received_loss_entry_is_capped_at_the_collections_k_whatever_the_order_it_arrives_in() {
    for ascending in [true, false] {
        let (a, b, ca, cb, _d) = pair();
        a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
        b.apply_batch(&everything(&a)).unwrap();
        let mut entries: Vec<(LossKey, OplogEntry)> =
            walls(K + 1).into_iter().map(|w| loss_entry(&a, &ca, w)).collect();
        if !ascending {
            entries.reverse();
        }
        for (_, entry) in &entries {
            b.apply_remote(&cb, entry).unwrap();
        }
        let all: Vec<LossKey> = entries.iter().map(|(k, _)| k.clone()).collect();
        let mut got = held(&b);
        got.sort_by(|x, y| x.rank_cmp(y));
        assert_eq!(
            got,
            the_k_greatest(&all),
            "ascending: {ascending}: the K greatest, and no more"
        );
        // The sixth record in either order costs one record: the least is displaced (ascending: it was held when the
        // greatest came) or refused (descending: it came last).
        let (cap, evicted) = (refused(&b, "cap"), refused(&b, "evicted"));
        assert_eq!(
            (cap, evicted),
            if ascending { (0, 1) } else { (1, 0) },
            "ascending: {ascending}"
        );
    }
}

// ---- route 5: the parked records folded in by the write that gives the document its row ------------

#[test]
fn route_5_the_records_parked_for_a_document_with_no_row_are_capped_when_the_row_comes() {
    let (a, b, ca, cb, _d) = pair();
    let entries: Vec<(LossKey, OplogEntry)> =
        walls(K + 1).into_iter().map(|w| loss_entry(&a, &ca, w)).collect();
    for (_, entry) in &entries {
        b.apply_batch(std::slice::from_ref(entry)).unwrap();
    }
    assert_eq!(
        b.get(&cb, &DocId::Int64(1)).unwrap(),
        None,
        "premise: the document has no row, so the six records are parked and no row was made of them"
    );

    b.insert(&cb, doc! { "_id": 1, "x": 5 }).unwrap();

    let all: Vec<LossKey> = entries.iter().map(|(k, _)| k.clone()).collect();
    let mut got = held(&b);
    got.sort_by(|x, y| x.rank_cmp(y));
    assert_eq!(got, the_k_greatest(&all), "the row holds the K greatest of the six");
    assert_eq!(refused(&b, "cap") + refused(&b, "evicted"), 1, "and one record was not kept");
}

// ---- route 1: a local write ------------------------------------------------------------------------

#[test]
fn route_1_a_local_write_caps_the_state_it_writes_at_the_collections_k() {
    let (a, b, ca, cb, _d) = pair();
    let id = a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    let six = six_keys(&a);
    put_records(&b, &cb, &six);
    assert_eq!(held(&b).len(), K + 1, "premise: a stored state holds six records");

    b.replace(&cb, &id, doc! { "_id": 1, "x": 2 }, false).unwrap();

    let mut got = held(&b);
    got.sort_by(|x, y| x.rank_cmp(y));
    assert_eq!(got, the_k_greatest(&six), "the write left the K greatest");
}

// ---- route 8: the update pipeline ---------------------------------------------------------------------

/// Adds one to `n` on every match that has one.
struct Inc;

impl ModifySpec for Inc {
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

#[test]
fn route_8_a_find_and_modify_caps_the_state_it_writes_at_the_collections_k() {
    let (a, b, ca, cb, _d) = pair();
    a.insert(&ca, doc! { "_id": 1, "n": 1_i64 }).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    let six = six_keys(&a);
    put_records(&b, &cb, &six);
    assert_eq!(held(&b).len(), K + 1, "premise: a stored state holds six records");

    let out = b.find_and_modify(&cb, &Candidates::Scan, &Inc).unwrap();
    assert!(out.matched, "{out:?}");

    let mut got = held(&b);
    got.sort_by(|x, y| x.rank_cmp(y));
    assert_eq!(got, the_k_greatest(&six), "the update left the K greatest");
}

// ---- route 2: a remote write entry whose join detects more than K losses --------------------------------

#[test]
fn route_2_a_remote_write_that_races_a_local_one_on_eight_fields_leaves_the_k_greatest_clashes() {
    let (a, b, ca, cb, _d) = pair();
    let id = a.insert(&ca, doc! { "_id": 1, "k": 0 }).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    let fields = |v: i32| {
        let mut d = doc! { "_id": 1, "k": 0 };
        for i in 0..8 {
            d.insert(format!("f{i}"), v);
        }
        d
    };
    // concurrent: neither has seen the other's write
    a.replace(&ca, &id, fields(1), false).unwrap();
    b.replace(&cb, &id, fields(2), false).unwrap();

    b.apply_batch(&everything(&a)).unwrap();
    a.apply_batch(&everything(&b)).unwrap();

    for (name, e) in [("a", &a), ("b", &b)] {
        let state = stored_state(e);
        let clashes = state.losses.keys().filter(|k| k.kind == LossKind::Clash).count();
        assert_eq!(state.losses.len(), K, "member {name}: the state holds K records, no more");
        assert_eq!(clashes, K, "member {name}: all clashes");
    }
    assert_eq!(held(&a), held(&b), "and the two hold the same K");
    // eight clashes were detected at each member's join: K kept, three not
    assert_eq!(
        refused(&b, "cap") + refused(&b, "evicted"),
        3,
        "three of the eight were not kept at b"
    );
}

// ---- route 3: a snapshot page --------------------------------------------------------------------------

#[test]
fn route_3_a_snapshot_page_that_brings_records_is_capped_at_the_collections_k_when_it_is_joined() {
    let (a, b, ca, cb, _d) = pair();
    let id = a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    let _ = id;
    // five records held at a, five other (greater) records held at b: the join of the two states is ten
    let w = walls(2 * K);
    let from_a: Vec<LossKey> = w[..K].iter().map(|w| key_at(writer_of(&a), *w)).collect();
    let from_b: Vec<LossKey> = w[K..].iter().map(|w| key_at(writer_of(&b), *w)).collect();
    put_records(&a, &ca, &from_a);
    put_records(&b, &cb, &from_b);

    let page = a.snapshot_page(None, Some(orders())).unwrap();
    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();

    let all: Vec<LossKey> = from_a.iter().chain(from_b.iter()).cloned().collect();
    let mut got = held(&b);
    got.sort_by(|x, y| x.rank_cmp(y));
    let mut want = all.clone();
    want.sort_by(|x, y| x.rank_cmp(y));
    let want = want.split_off(want.len() - K);
    assert_eq!(got, want, "the join of the page and the row kept the K greatest of the ten");
    assert_eq!(
        refused(&b, "cap") + refused(&b, "evicted"),
        K as u64,
        "five of the ten were not kept"
    );
}

// ---- routes 6 and 7, and every other site that builds options -----------------------------------------------

#[test]
fn the_options_every_route_builds_carry_the_collections_limits_and_not_the_nodes() {
    let (_a, b, _ca, cb, _d) = pair();
    // The rewind (rewind.rs), the late drop (late_drop.rs), the loss pending retention pass, the apply, the write and the
    // update routes all build their options through `field_writer`, the one constructor.
    let options = b.field_writer(&cb).unwrap().options();
    assert_eq!(options.loss_cap(), K, "the collection's K, not the node's 4");
    assert_eq!(options.loss_cut(), 100, "the collection's cut, not the node's 64");
}

/// No storage source builds its join options from the default, outside the one constructor and the tests: a route that did
/// would cap at the widest limits and never at the collection's.
#[test]
fn no_storage_route_builds_join_options_from_the_default() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if !name.ends_with(".rs") || name.ends_with("_tests.rs") || name == "field_write.rs" {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        // cut the inline test modules off: what is under `#[cfg(test)]` at the end of a file is not a route
        let production = text.split("\n#[cfg(test)]\nmod tests").next().unwrap();
        for (n, line) in production.lines().enumerate() {
            let code = line.split("//").next().unwrap();
            if code.contains("JoinOptions::default()") || code.contains("JoinOptions {") {
                offenders.push(format!("{name}:{}: {}", n + 1, line.trim()));
            }
        }
    }
    assert!(offenders.is_empty(), "options built outside field_writer: {offenders:#?}");
}

// ---- Claim 4 under a real cap ----------------------------------------------------------------------------------

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(20));
}

/// The records `e` appended an entry of its own for, one per entry, by key.
fn own_records(e: &Engine) -> Vec<LossKey> {
    everything(e)
        .into_iter()
        .filter(|entry| entry.kind == OpKind::LossRecord && entry.stamp.node == e.node_id())
        .map(|entry| MergeState::decode_loss_record(entry.body.as_deref().unwrap()).unwrap().0)
        .collect()
}

/// Delivery within `R` keeps at-least-once past `K` (Claim 4 of the design): three members that wrote eight fields of one
/// document concurrently detect sixteen clashes between them, more than the K of the collection. Every member converges to
/// the same `K` greatest, `cap_K(U)`, and each appended one entry of its own, once, for each record of that set. The
/// third member takes the others' state as a snapshot page and not as entries.
#[test]
fn claim_4_every_member_holds_and_reports_each_record_of_the_k_greatest_of_what_the_cluster_detected()
 {
    let (a, b, ca, cb, _d) = pair();
    let (c, _dc) = engine();
    c.apply_batch(&everything(&a)).unwrap();
    let cc = c.get_collection("shop", "orders").unwrap();
    assert_eq!(
        cc.loss_limits,
        Some(limits()),
        "premise: the third member took the limits from the definition too"
    );
    let id = a.insert(&ca, doc! { "_id": 1, "k": 0 }).unwrap();
    later();
    b.apply_batch(&everything(&a)).unwrap();
    c.apply_batch(&everything(&a)).unwrap();

    let fields = |v: i32| {
        let mut d = doc! { "_id": 1, "k": 0 };
        for i in 0..8 {
            d.insert(format!("f{i}"), v);
        }
        d
    };
    // concurrent: none of the three has seen another's write
    let mut stamps = Vec::new();
    for (e, coll, v) in [(&a, &ca, 1), (&b, &cb, 2), (&c, &cc, 3)] {
        later();
        let out = e.replace(coll, &id, fields(v), false).unwrap();
        stamps.push(crate::field_write::m_stamp(out.stamp.expect("the write was made")));
    }
    later();

    // a and b take entries; c takes a's state as a page (and a, b take c's entries)
    for _ in 0..3 {
        a.apply_batch(&everything(&b)).unwrap();
        a.apply_batch(&everything(&c)).unwrap();
        b.apply_batch(&everything(&a)).unwrap();
        b.apply_batch(&everything(&c)).unwrap();
        let page = a.snapshot_page(None, Some(orders())).unwrap();
        c.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
        c.apply_batch(&everything(&b)).unwrap();
        later();
    }

    // U: the loser of every field, the two older writes of the three, at each of the eight fields
    let winner = *stamps.iter().max().unwrap();
    let mut universe: Vec<LossKey> = Vec::new();
    for s in stamps.iter().filter(|s| **s != winner) {
        for i in 0..8 {
            universe.push(LossKey::field(
                LossKind::Clash,
                Path::flat(&format!("f{i}")).unwrap(),
                *s,
            ));
        }
    }
    assert_eq!(universe.len(), 16, "premise: sixteen clashes, more than K = {K}");
    universe.sort_by(|x, y| x.rank_cmp(y));
    let want: Vec<LossKey> = universe.split_off(universe.len() - K);

    let mut sets = Vec::new();
    for (name, e) in [("a", &a), ("b", &b), ("c", &c)] {
        let mut got = held(e);
        got.sort_by(|x, y| x.rank_cmp(y));
        assert_eq!(got, want, "member {name}: cap_K of everything the cluster detected");
        sets.push(got);
        let own = own_records(e);
        for key in &want {
            assert_eq!(
                own.iter().filter(|k| *k == key).count(),
                1,
                "member {name}: exactly one entry of its own for {key:?}"
            );
        }
        let mut distinct = own.clone();
        distinct.sort_by(|x, y| x.rank_cmp(y));
        distinct.dedup();
        assert_eq!(distinct.len(), own.len(), "member {name}: no record is given two entries");
    }
    assert!(sets.windows(2).all(|w| w[0] == w[1]), "the three members hold one record set");
    // and the cap really bit: records were refused or displaced at every member
    for (name, e) in [("a", &a), ("b", &b), ("c", &c)] {
        assert!(
            refused(e, "cap") + refused(e, "evicted") > 0,
            "member {name}: the cap refused or displaced a record"
        );
    }
}

// ---- the limits belong to the creation, as `R` does ---------------------------------------------------------

/// A creation cannot have two sets of loss limits: the same creation met again with other limits is refused by name
/// (counted, not applied) and the collection keeps the ones it holds.
#[test]
fn a_creation_met_again_with_other_loss_limits_is_refused_by_name() {
    let (a, b, _ca, _cb, _d) = pair();
    let creation = everything(&a);
    let held_before = b.get_collection("shop", "orders").unwrap();

    let mut again = creation.last().unwrap().clone();
    let mut body: kimmy_core::CollectionCreate =
        bson::deserialize_from_slice(again.body.as_deref().unwrap()).unwrap();
    body.loss_limits = Some(LossLimits { cap: 9, cut_bytes: 300 });
    again.body = Some(bson::serialize_to_vec(&body).unwrap());
    let outcome = b.apply_batch(std::slice::from_ref(&again)).unwrap();

    assert_eq!(outcome.ddl_refused, 1, "a creation cannot have two sets of limits: {outcome:?}");
    assert_eq!(
        b.get_collection("shop", "orders").unwrap().loss_limits,
        held_before.loss_limits,
        "and the ones held stand"
    );
}

/// The same for a snapshot page: refused by name, never applied over the held limits.
#[test]
fn a_page_carrying_other_loss_limits_for_the_same_creation_is_refused_by_name() {
    let (a, b, _ca, _cb, _d) = pair();
    let mut page = a.snapshot_page(None, None).unwrap();
    assert_eq!(
        page.collections[0].loss_limits,
        Some(limits()),
        "premise: the page carries the limits"
    );
    page.collections[0].loss_limits = Some(LossLimits { cap: 9, cut_bytes: 300 });

    let refused =
        b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page);

    match refused {
        Err(crate::StorageError::Core(e)) => {
            assert!(e.to_string().contains("two sets of loss limits"), "{e}");
        }
        other => panic!("refused by name, not {other:?}"),
    }
    assert_eq!(b.get_collection("shop", "orders").unwrap().loss_limits, Some(limits()));
}

/// A creation whose limits are outside the bounds, or that carries none, is not a valid creation: refused as corrupt, so no
/// `field` collection is ever held without limits inside them.
#[test]
fn a_creation_of_a_field_collection_without_limits_or_outside_their_bounds_is_refused() {
    for bad in [
        None,
        Some(LossLimits { cap: 3, cut_bytes: 64 }),
        Some(LossLimits { cap: 65, cut_bytes: 64 }),
        Some(LossLimits { cap: 4, cut_bytes: 63 }),
        Some(LossLimits { cap: 4, cut_bytes: 4097 }),
    ] {
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
        let mut creation = everything(&a).last().unwrap().clone();
        let mut body: kimmy_core::CollectionCreate =
            bson::deserialize_from_slice(creation.body.as_deref().unwrap()).unwrap();
        body.loss_limits = bad;
        creation.body = Some(bson::serialize_to_vec(&body).unwrap());

        let applied = b.apply_batch(std::slice::from_ref(&creation));

        assert!(
            applied.is_err() || b.get_collection("shop", "orders").is_err(),
            "limits {bad:?}: the collection was not made, or the entry refused: {applied:?}"
        );
    }
}

/// Two creations of one name under the same policy and different limits settle on the later creation's limits, on every
/// member, whichever creation reaches it first.
#[test]
fn the_loss_limits_follow_the_later_creation_on_every_member() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.set_default_loss_limits(LossLimits { cap: 6, cut_bytes: 200 }).unwrap();
    b.set_default_loss_limits(LossLimits { cap: 9, cut_bytes: 300 }).unwrap();
    a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    b.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();

    let (from_a, from_b) = (everything(&a), everything(&b));
    a.apply_batch(&from_b).unwrap();
    b.apply_batch(&from_a).unwrap();

    for engine in [&a, &b] {
        assert_eq!(
            engine.get_collection("shop", "orders").unwrap().loss_limits,
            Some(LossLimits { cap: 9, cut_bytes: 300 }),
            "every member ends with the later creation's limits"
        );
    }
}
