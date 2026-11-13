//! The epoch (Q7 Step 1, D5 and D13): minted at the open that finds none, never carried across a
//! restore, cleared by a rewind, and part of the origin every local write and every vector entry
//! names.

use std::path::Path;

use bson::doc;
use kimmy_core::{Epoch, Hlc, Origin, VersionVector};
use redb::ReadableDatabase;

use crate::backup::restore;
use crate::{Engine, WalkScope, tables};

fn open(path: &Path) -> Engine {
    Engine::open(path).unwrap()
}

/// The epoch row of a store nobody has open.
fn epoch_row(path: &Path) -> Option<Vec<u8>> {
    let db = redb::Builder::new().open_read_only(path).unwrap();
    let txn = db.begin_read().unwrap();
    let meta = txn.open_table(tables::META).unwrap();
    meta.get(tables::META_EPOCH).unwrap().map(|v| v.value().to_vec())
}

fn backup_of(engine: &Engine) -> Vec<u8> {
    let mut buf = Vec::new();
    engine.backup_to(&mut buf, WalkScope::Request).unwrap();
    buf
}

#[test]
fn a_first_open_mints_an_epoch_and_a_restart_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let (epoch, stamp) = {
        let engine = open(&path);
        let epoch = engine.epoch();
        assert_eq!(engine.origin(), Origin::new(engine.node_id(), epoch));
        assert!(epoch.start_hlc() > Hlc::ZERO, "the start HLC is the open's: {epoch:?}");
        let coll = engine.create_collection("shop", "orders").unwrap();
        engine.insert(&coll, doc! { "_id": 1 }).unwrap();
        let stamp = engine.read_oplog_from(Hlc::ZERO, 1, WalkScope::Request).unwrap()[0].stamp;
        (epoch, stamp)
    };
    assert_eq!(epoch_row(&path).as_deref(), Some(epoch.0.as_slice()), "the row is the epoch");
    assert_eq!(stamp.epoch, epoch, "a local write is stamped with the member's epoch");
    assert!(epoch.start_hlc() <= stamp.hlc, "no stamp of the epoch is older than its start");

    let again = open(&path);
    assert_eq!(again.epoch(), epoch, "a restart keeps its epoch");
    assert_eq!(again.version_vector().unwrap().get(again.origin()), {
        again
            .read_oplog_from(Hlc::ZERO, usize::MAX, WalkScope::Request)
            .unwrap()
            .iter()
            .map(|e| e.stamp.hlc)
            .max()
            .unwrap()
    });
}

/// An epoch minted at T, then an open on a clock behind T (a data directory moved to a host whose
/// clock is behind, a boot before the clock was stepped forward, or `kimmyd restore` and then a
/// start on such a host): nothing of the epoch was written yet, so the oplog tail and the vectors
/// are all below T. A peer that never witnessed this origin asks for it from T (`behind`'s
/// start-HLC rule), so a write of this origin stamped below T would never be asked for.
#[test]
fn an_open_on_a_clock_behind_its_epochs_start_stamps_every_write_at_or_above_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    drop(open(&path));
    let start = Hlc::new(crate::engine::physical_now_ms() + 3_600_000, 0);
    let epoch = Epoch::new(start, [7; 8]);
    {
        let db = redb::Database::open(&path).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(tables::META)
            .unwrap()
            .insert(tables::META_EPOCH, epoch.0.as_slice())
            .unwrap();
        txn.commit().unwrap();
    }
    let engine = open(&path);
    assert_eq!(engine.epoch(), epoch, "the open keeps the stored epoch");
    let coll = engine.create_collection("shop", "orders").unwrap();
    engine.insert(&coll, doc! { "_id": 1 }).unwrap();
    let stamps: Vec<_> = engine
        .read_oplog_from(Hlc::ZERO, usize::MAX, WalkScope::Request)
        .unwrap()
        .into_iter()
        .map(|e| e.stamp)
        .collect();
    assert_eq!(stamps.len(), 2, "the collection and the insert: {stamps:?}");
    for stamp in stamps {
        assert_eq!(stamp.epoch, epoch);
        assert!(stamp.hlc >= start, "{:?} is below the epoch's start {start:?}", stamp.hlc);
    }
}

#[test]
fn an_open_that_finds_no_row_mints_at_or_above_every_stamp_the_store_holds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let first = {
        let engine = open(&path);
        let coll = engine.create_collection("shop", "orders").unwrap();
        for i in 0..5 {
            engine.insert(&coll, doc! { "_id": i }).unwrap();
        }
        engine.epoch()
    };
    let top = {
        let engine = open(&path);
        engine
            .read_oplog_from(Hlc::ZERO, usize::MAX, WalkScope::Request)
            .unwrap()
            .iter()
            .map(|e| e.stamp.hlc)
            .max()
            .unwrap()
    };
    // the row goes, as it does in a restore
    {
        let db = redb::Database::open(&path).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(tables::META).unwrap().remove(tables::META_EPOCH).unwrap();
        txn.commit().unwrap();
    }
    let engine = open(&path);
    let second = engine.epoch();
    assert_ne!(second, first);
    assert!(second > first, "a later start sorts later");
    assert!(second.start_hlc() >= top, "{:?} is below the store's top {top:?}", second.start_hlc());
    // the entries written under the first epoch are untouched, and still under it
    let stamps: Vec<_> = engine
        .read_oplog_from(Hlc::ZERO, usize::MAX, WalkScope::Request)
        .unwrap()
        .into_iter()
        .map(|e| e.stamp)
        .collect();
    assert!(stamps.iter().all(|s| s.epoch == first));
    // and the next write is under the new one
    let coll = engine.get_collection("shop", "orders").unwrap();
    engine.insert(&coll, doc! { "_id": 99 }).unwrap();
    let last = engine.read_oplog_from(top, usize::MAX, WalkScope::Request).unwrap();
    assert_eq!(last.last().unwrap().stamp.epoch, second);
}

/// `kimmyd restore` opens the restored store, which mints the epoch, and writes the floor from
/// that open; the first real start opens it again and finds the row. Nothing has been written
/// under the new epoch, so that start lost no write of it, and what it may have lost sits under
/// the old epochs, as at the mint: it asks from the same position, whether or not the minting
/// open's floor reached the file (a crash between the mint and the floor's write).
#[test]
fn a_reopen_before_the_first_write_of_a_minted_epoch_asks_from_the_old_epochs() {
    let dir = tempfile::tempdir().unwrap();
    let source = open(&dir.path().join("source.redb"));
    let coll = source.create_collection("shop", "orders").unwrap();
    source.insert(&coll, doc! { "_id": 1 }).unwrap();
    let backed_up = source.version_vector().unwrap().get(source.origin());
    let backup = backup_of(&source);
    let path = dir.path().join("restored.redb");
    restore(&path, &mut backup.as_slice()).unwrap();

    let minted = {
        let engine = open(&path);
        assert_eq!(engine.own_position_at_open(), backed_up, "the minting open asks from B");
        engine.epoch()
    };
    let engine = open(&path);
    assert_eq!(engine.epoch(), minted, "premise: the reopen keeps the epoch");
    assert_eq!(
        engine.own_position_at_open(),
        backed_up,
        "the first start after the restore asks from the backup's position"
    );
    let coll = engine.get_collection("shop", "orders").unwrap();
    engine.insert(&coll, doc! { "_id": 2 }).unwrap();
    let written = engine.version_vector().unwrap().get(engine.origin());
    drop(engine);
    assert_eq!(
        open(&path).own_position_at_open(),
        written,
        "once it has written under the epoch, a start asks from its own position under it"
    );
}

/// A store that minted its first epoch and wrote nothing has no old epoch to ask from: a start
/// asks from the epoch's start, since no entry of it is older.
#[test]
fn a_reopen_of_a_new_store_that_wrote_nothing_asks_from_its_epochs_start() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let epoch = open(&path).epoch();
    assert_eq!(open(&path).own_position_at_open(), epoch.start_hlc());
}

#[test]
fn a_restore_does_not_carry_the_epoch_so_each_restore_mints_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let source = open(&dir.path().join("source.redb"));
    let coll = source.create_collection("shop", "orders").unwrap();
    source.insert(&coll, doc! { "_id": 1 }).unwrap();
    let source_epoch = source.epoch();
    let backup = backup_of(&source);
    // the backup copies META whole, so the row is in the stream: the restore must skip it
    assert!(
        backup.windows(Epoch::LEN).any(|w| w == source_epoch.0.as_slice()),
        "premise: the backup stream holds the source's epoch row"
    );

    let (one, two) = (dir.path().join("one.redb"), dir.path().join("two.redb"));
    restore(&one, &mut backup.as_slice()).unwrap();
    restore(&two, &mut backup.as_slice()).unwrap();
    assert_eq!(epoch_row(&one), None, "a restored store has no epoch until it opens");
    assert_eq!(epoch_row(&two), None);

    let (a, b) = (open(&one), open(&two));
    assert_eq!(a.node_id(), source.node_id(), "the identity is restored, the incarnation is not");
    assert_eq!(b.node_id(), source.node_id());
    assert_ne!(a.epoch(), source_epoch);
    assert_ne!(b.epoch(), source_epoch);
    assert_ne!(a.epoch(), b.epoch(), "restoring one backup twice gives two incarnations");
    // each keeps the old epoch's history as it was
    for restored in [&a, &b] {
        let stamps = restored.read_oplog_from(Hlc::ZERO, usize::MAX, WalkScope::Request).unwrap();
        assert!(stamps.iter().all(|e| e.stamp.epoch == source_epoch));
    }
}

#[test]
fn a_rewind_clears_the_epoch_and_the_next_open_mints_a_new_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let (old, kept) = {
        let engine = open(&path);
        let coll = engine.create_collection("shop", "orders").unwrap();
        engine.insert(&coll, doc! { "_id": 1 }).unwrap();
        let kept = engine.read_oplog_from(Hlc::ZERO, usize::MAX, WalkScope::Request).unwrap();
        let until = kept.last().unwrap().stamp.hlc;
        std::thread::sleep(std::time::Duration::from_millis(5));
        engine.insert(&coll, doc! { "_id": 2 }).unwrap();
        engine.rewind_to(until).unwrap();
        // this engine keeps its epoch until it is dropped, as the doc comment says
        let txn = engine.db().begin_read().unwrap();
        let meta = txn.open_table(tables::META).unwrap();
        assert!(meta.get(tables::META_EPOCH).unwrap().is_none(), "the row is gone in the rewind");
        drop(meta);
        drop(txn);
        (engine.epoch(), kept)
    };
    let engine = open(&path);
    assert_ne!(engine.epoch(), old, "the reopen minted");
    assert!(engine.epoch() > old);
    let after = engine.read_oplog_from(Hlc::ZERO, usize::MAX, WalkScope::Request).unwrap();
    assert_eq!(after, kept, "the entries the rewind kept are unchanged, under the old epoch");
    assert!(after.iter().all(|e| e.stamp.epoch == old));
}

/// What a restore does to the member's position and to the vectors: the writes it threw away
/// stay owed under the old epoch, and the new ones cannot be covered by anything a peer holds
/// of the old one (D13, L26).
#[test]
fn a_restored_members_new_writes_are_a_new_origin_and_its_old_writes_stay_owed() {
    let dir = tempfile::tempdir().unwrap();
    let a = open(&dir.path().join("a.redb"));
    let coll = a.create_collection("shop", "orders").unwrap();
    for i in 0..3 {
        a.insert(&coll, doc! { "_id": i }).unwrap();
    }
    let backup = backup_of(&a);
    let at_backup = a.version_vector().unwrap().get(a.origin());
    std::thread::sleep(std::time::Duration::from_millis(5));
    for i in 3..5 {
        a.insert(&coll, doc! { "_id": i }).unwrap();
    }
    let old = a.origin();
    let held_by_peer = a.version_vector().unwrap().get(old);
    assert!(held_by_peer > at_backup);

    // A is restored from the backup that predates its last two writes
    let path = dir.path().join("restored.redb");
    restore(&path, &mut backup.as_slice()).unwrap();
    let r = open(&path);
    assert_eq!(r.node_id(), a.node_id());
    assert_ne!(r.origin(), old);
    // the lost writes are owed from the backup's position under the old epoch
    assert_eq!(r.own_position_at_open(), at_backup, "the replay asks from the lowest old position");
    let rcoll = r.get_collection("shop", "orders").unwrap();
    r.insert(&rcoll, doc! { "_id": 99 }).unwrap();

    let mine = r.version_vector().unwrap();
    assert_eq!(mine.get(old), at_backup, "the old epoch stands where the backup left it");
    assert!(mine.get(r.origin()) > Hlc::ZERO, "the new writes are under the new epoch");
    assert_eq!(mine.len(), 2, "the node is two origins now: {mine:?}");

    // the peer A holds everything of the old epoch, and none of the new: its coverage of the old
    // epoch does not cover the restored member's new write
    let peer = a.witnessed_vector().unwrap();
    assert!(peer.behind(&mine).is_some(), "the new epoch's write is not covered at the peer");
    assert!(
        peer.get(old) >= mine.get(old) && peer.get(r.origin()) == Hlc::ZERO,
        "covered on the old epoch, not on the new"
    );
    // and the restored member lacks what the peer holds of the old epoch
    let theirs: VersionVector = a.version_vector().unwrap();
    assert!(r.witnessed_vector().unwrap().behind(&theirs).is_some());
}

/// A `state_held` row is keyed by the origin, in `META`, and a backup carries it.
#[test]
fn a_state_held_row_is_keyed_by_the_origin_and_survives_a_restore() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(&dir.path().join("source.redb"));
    let coll = engine.create_collection("shop", "orders").unwrap();
    engine.insert(&coll, doc! { "_id": 1 }).unwrap();
    // a grant above everything held of a foreign origin
    let foreign = Origin::new(kimmy_core::NodeId::from_bytes([9; 16]), Epoch::from_u64(5));
    let mut granted = VersionVector::new();
    granted.insert(foreign, Hlc::new(crate::engine::physical_now_ms() + 1_000, 0));
    engine.absorb_version_vector(&granted).unwrap();
    let held = engine.state_held().unwrap();
    assert_eq!(held.len(), 1);
    assert_eq!(held.get(foreign), granted.get(foreign));
    {
        let txn = engine.db().begin_read().unwrap();
        let meta = txn.open_table(tables::META).unwrap();
        let key = format!("{}{foreign}", tables::META_STATE_HELD_PREFIX);
        assert!(meta.get(key.as_str()).unwrap().is_some(), "keyed by {key}");
    }
    let backup = backup_of(&engine);
    let path = dir.path().join("restored.redb");
    restore(&path, &mut backup.as_slice()).unwrap();
    let restored = open(&path);
    assert_eq!(restored.state_held().unwrap().get(foreign), granted.get(foreign));
}

/// No table key is built from a stamp's node alone: a lookup, insert or key that takes `.node`
/// of a stamp or an origin and turns it into bytes or a string for a table is the V4m1 silent
/// miss, a row that an origin-keyed reader never finds. Keys are built from `Origin` (or the
/// whole stamp). A source guard in the style of `verified.rs`.
#[test]
fn no_table_key_is_built_from_a_nodes_bytes_alone() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    for entry in std::fs::read_dir(&src).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if path.extension().is_none_or(|e| e != "rs")
            || name.ends_with("_tests.rs")
            || name == "tests.rs"
        {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        // production code only: up to the file's test module
        let production = text.split("#[cfg(test)]\nmod ").next().unwrap();
        for (n, line) in production.lines().enumerate() {
            let code = line.split("//").next().unwrap();
            let by_node = code.contains(".node.to_bytes()")
                || code.contains("node_id().to_bytes()")
                || code.contains("stamp.node)") && code.contains("format!")
                || code.contains("META_STATE_HELD_PREFIX") && code.contains(".node");
            if by_node {
                offenders.push(format!("{name}:{}: {}", n + 1, line.trim()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these build a table key from a node alone; use the origin:\n{}",
        offenders.join("\n")
    );
}
