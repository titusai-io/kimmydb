//! D13 containment, the storage half (ADR-274): the table of newest epochs, the halt, and the floor
//! of a mint. The tests commit of unit 14a: every test is red on its designed assertion against the
//! scaffold in `containment.rs`, and the code commits turn it green. Test numbers are those of
//! `D13-prebuild.md`.

use std::path::Path;

use bson::doc;
use kimmy_core::{Epoch, Hlc, NodeId};

use crate::backup::restore;
use crate::{Engine, OpenOptions, WalkScope, physical_now_ms};

fn open(path: &Path) -> Engine {
    Engine::open(path).unwrap()
}

fn backup_of(engine: &Engine) -> Vec<u8> {
    let mut buf = Vec::new();
    engine.backup_to(&mut buf, WalkScope::Request).unwrap();
    buf
}

/// An epoch whose start is `ahead_ms` after the clock now: a stand-in for an epoch another host
/// minted.
fn an_epoch_ahead(ahead_ms: u64, random: u8) -> Epoch {
    Epoch::new(Hlc::new(physical_now_ms() + ahead_ms, 0), [random; 8])
}

// -- the mint floor (tests 4, 12; findings 1, 11)
// ----------------------------------------------------------------------

/// A restore into a halted host's directory mints strictly above the epoch the marker names, and
/// the told epoch starts ABOVE the host's own clock, or the floor would not be binding and a mint
/// from the clock alone would pass for the wrong reason (the tester's D).
#[test]
fn a_mint_with_a_floor_starts_strictly_above_it() {
    let dir = tempfile::tempdir().unwrap();
    let source = open(&dir.path().join("source.redb"));
    let coll = source.create_collection("shop", "orders").unwrap();
    source.insert(&coll, doc! { "_id": 1 }).unwrap();
    let backup = backup_of(&source);

    let told = an_epoch_ahead(60_000, 9);
    assert!(
        told.start_hlc() > Hlc::new(physical_now_ms(), u16::MAX),
        "premise: the told epoch starts above this host's clock, so the clock cannot pass the floor"
    );
    assert!(
        told.start_hlc().wall_ms
            <= physical_now_ms() + crate::clock_bound::DEFAULT_MAX_CLOCK_SKEW_MS,
        "premise: and at most the skew bound above it, or the tell is not acted on at all"
    );
    let path = dir.path().join("restored.redb");
    restore(&path, &mut backup.as_slice()).unwrap();
    let engine = Engine::open_with(&path, &OpenOptions::new().epoch_floor(told)).unwrap();
    assert!(
        engine.epoch() > told,
        "the mint is above the told epoch: {:?} vs {told:?}",
        engine.epoch()
    );
    assert!(
        engine.epoch().start_hlc() > told.start_hlc(),
        "STRICTLY above in the start HLC, so no tie is left to the random bytes (finding 11)"
    );
}

/// A floor that is below what the clock gives changes nothing: the mint is above the store's stamps
/// as before.
#[test]
fn a_floor_below_the_clock_does_not_change_the_mint() {
    let dir = tempfile::tempdir().unwrap();
    let source = open(&dir.path().join("source.redb"));
    source.create_collection("shop", "orders").unwrap();
    let backup = backup_of(&source);
    let below = Epoch::new(Hlc::new(1_000, 0), [1; 8]);
    let path = dir.path().join("restored.redb");
    restore(&path, &mut backup.as_slice()).unwrap();
    let engine = Engine::open_with(&path, &OpenOptions::new().epoch_floor(below)).unwrap();
    assert!(
        engine.epoch() > source.epoch(),
        "the mint is above the source's epoch as it was without a floor"
    );
    assert!(
        engine.epoch().start_hlc() > Hlc::new(1_000, 0),
        "and the floor was not the whole answer: {:?}",
        engine.epoch()
    );
}

// -- the table of newest epochs (tests 10, 11; findings 7, 9)
// -----------------------------------------------------------

#[test]
fn the_newest_epoch_of_a_node_is_recorded_and_only_ever_rises() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let other = NodeId::from_bytes([7; 16]);
    let (older, newer) = (an_epoch_ahead(1_000, 1), an_epoch_ahead(2_000, 1));
    {
        let engine = open(&path);
        assert_eq!(
            engine.newest_epoch(other).unwrap(),
            Epoch::ZERO,
            "premise: a node never heard of has none"
        );
        engine.note_epoch(other, newer).unwrap();
        assert_eq!(engine.newest_epoch(other).unwrap(), newer, "the table records the epoch");
        engine.note_epoch(other, older).unwrap();
        assert_eq!(
            engine.newest_epoch(other).unwrap(),
            newer,
            "an older epoch never lowers a row (grow-only)"
        );
        // A belief opens no transaction: the row is written by the next commit of any holder.
        engine.create_collection("shop", "orders").unwrap();
    }
    assert_eq!(
        open(&path).newest_epoch(other).unwrap(),
        newer,
        "the row is durable once a write has carried it"
    );
}

/// What the design accepts (the model row, with a crash): a member that restarts before its next
/// write forgets an epoch it had only heard in a handshake, and relearns it at its next contact.
#[test]
fn a_row_no_write_has_carried_is_forgotten_by_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let other = NodeId::from_bytes([7; 16]);
    let newer = an_epoch_ahead(2_000, 1);
    {
        let engine = open(&path);
        engine.note_epoch(other, newer).unwrap();
        assert_eq!(engine.newest_epoch(other).unwrap(), newer, "premise: held in memory");
    }
    assert_eq!(open(&path).newest_epoch(other).unwrap(), Epoch::ZERO);
}

/// The newest epoch of a node is also what the node's origins in the version vectors say, read at
/// each check (finding 7): a peer's entries of an epoch this member never saw in a handshake still
/// count.
#[test]
fn the_newest_epoch_reads_the_origins_in_the_vectors_as_well_as_the_row() {
    let dir = tempfile::tempdir().unwrap();
    let writer = open(&dir.path().join("writer.redb"));
    let coll = writer.create_collection("shop", "orders").unwrap();
    writer.insert(&coll, doc! { "_id": 1 }).unwrap();
    let reader = open(&dir.path().join("reader.redb"));
    let window = writer.entries_for_peer(Hlc::ZERO, 100, WalkScope::Request).unwrap();
    reader.apply_batch(&window.entries).unwrap();
    assert_eq!(
        reader.newest_epoch(writer.node_id()).unwrap(),
        writer.epoch(),
        "no handshake wrote a row, and the entries' origin names the epoch"
    );
}

/// A backup keeps the own node's row only (finding 9): staging clusters built from production
/// backups share node ids, and a backup that carried the whole table would carry production's
/// newest epochs into them.
#[test]
fn a_backup_carries_the_own_nodes_row_and_no_other() {
    let dir = tempfile::tempdir().unwrap();
    let source = open(&dir.path().join("source.redb"));
    let (other, third) = (NodeId::from_bytes([7; 16]), NodeId::from_bytes([8; 16]));
    let (e_other, e_third) = (an_epoch_ahead(1_000, 2), an_epoch_ahead(2_000, 3));
    source.note_epoch(other, e_other).unwrap();
    source.note_epoch(third, e_third).unwrap();
    assert_eq!(source.newest_epoch(other).unwrap(), e_other, "premise: the table records");
    let backup = backup_of(&source);

    let path = dir.path().join("restored.redb");
    restore(&path, &mut backup.as_slice()).unwrap();
    let restored = open(&path);
    assert_eq!(
        restored.newest_epoch(other).unwrap(),
        Epoch::ZERO,
        "another node's row is not carried"
    );
    assert_eq!(restored.newest_epoch(third).unwrap(), Epoch::ZERO);
    assert!(
        restored.newest_epoch(source.node_id()).unwrap() >= source.epoch(),
        "the own node's row is carried, and the restore's own epoch is above it"
    );
}

/// The rows a member relays on its vectors are the newest by epoch start, at most the cap, and the
/// cap's cut is reported: a relay of everything it holds would grow with every node id ever seen.
#[test]
fn the_rows_a_member_relays_are_the_newest_by_start_and_at_most_the_cap() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(&dir.path().join("kimmy.redb"));
    let nodes: Vec<NodeId> = (0..10u8).map(|i| NodeId::from_bytes([0x40 + i; 16])).collect();
    for (i, node) in nodes.iter().enumerate() {
        engine.note_epoch(*node, an_epoch_ahead(1_000 + 1_000 * i as u64, i as u8)).unwrap();
    }

    let (all, cut) = engine.newest_epoch_rows(100);
    assert!(!cut, "nothing is cut under the cap");
    assert_eq!(all.len(), 11, "ten rows noted, and this member's own: {all:?}");
    assert!(all.iter().any(|(node, epoch)| *node == engine.node_id() && *epoch == engine.epoch()));
    for pair in all.windows(2) {
        assert!(
            pair[0].1.start_hlc() >= pair[1].1.start_hlc(),
            "the newest by start first: {all:?}"
        );
    }

    let (few, cut) = engine.newest_epoch_rows(4);
    assert!(cut, "the cap cut rows, and says so");
    let held: Vec<NodeId> = few.iter().map(|(node, _)| *node).collect();
    assert_eq!(held, vec![nodes[9], nodes[8], nodes[7], nodes[6]], "the four newest, in order");
}

// -- the halt (tests 1, 2, 7; findings 8a, 8c)
// --------------------------------------------------------------------------

/// The flush leader that was asleep through the halt finds the flag and commits nothing (ADR-274,
/// the refusal site the halt's order protects: mutant "the flush does not read the flag"). A
/// coalesced commit has landed and is leading its barrier, asleep for the window, when the host
/// halts. The halt's own transaction is a flush: it carries that commit to the disk and does the
/// barrier's bookkeeping. The leader then wakes and flushes, finds the flag, and writes nothing:
/// the count of flushes does not move after the halt, and the commit is acknowledged and durable.
#[test]
fn a_flush_leader_asleep_through_the_halt_finds_it_and_commits_nothing() {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::DurabilityClass;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let engine = Arc::new(open(&path));
    engine.set_durability(DurabilityClass::Coalesced, Duration::from_millis(800));
    let orders = engine.create_collection("shop", "orders").unwrap();

    let writer = {
        let (engine, orders) = (Arc::clone(&engine), orders.clone());
        std::thread::spawn(move || engine.insert(&orders, doc! { "_id": 1 }))
    };
    // The commit is visible once it has landed, and it then leads its barrier and sleeps the
    // window: halt in the middle of it.
    let id = kimmy_core::DocId::Int64(1);
    let deadline = Instant::now() + Duration::from_secs(10);
    while engine.get(&orders, &id).unwrap().is_none() {
        assert!(Instant::now() < deadline, "the commit never landed");
        std::thread::sleep(Duration::from_millis(1));
    }
    std::thread::sleep(Duration::from_millis(100));
    engine.halt(an_epoch_ahead(1_000, 6)).unwrap();
    let flushes_at_the_halt = engine.fsyncs();

    writer.join().unwrap().expect("the commit is acknowledged");
    assert!(engine.halted().is_some(), "premise: the host is halted");
    assert_eq!(
        engine.fsyncs(),
        flushes_at_the_halt,
        "the leader that slept through the halt found it and committed nothing"
    );
    drop(engine);
    let again = open(&path);
    let orders = again.get_collection("shop", "orders").unwrap();
    assert!(again.get(&orders, &id).unwrap().is_some(), "the halt's flush carried the commit");
}

/// A told host refuses every write from then on, and the refusal commits nothing: the chokepoint's
/// own count does not move.
#[test]
fn a_halted_host_commits_nothing_after_the_halt() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(&dir.path().join("kimmy.redb"));
    let coll = engine.create_collection("shop", "orders").unwrap();
    engine.insert(&coll, doc! { "_id": 1 }).unwrap();
    assert!(engine.halted().is_none(), "premise: not halted");

    let told = an_epoch_ahead(1_000, 4);
    engine.halt(told).unwrap();
    assert!(engine.halted().is_some(), "the halt is in force");

    let commits = engine.commits();
    assert!(engine.insert(&coll, doc! { "_id": 2 }).is_err(), "a local write is refused");
    assert!(engine.create_collection("shop", "more").is_err(), "so is a schema change");
    assert_eq!(engine.commits(), commits, "and nothing was committed by the refusal");
}

/// The halt row names the epoch it halts, and the epoch it was told of.
#[test]
fn a_halt_names_its_own_epoch_and_the_one_it_was_told_of() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(&dir.path().join("kimmy.redb"));
    let told = an_epoch_ahead(1_000, 5);
    engine.halt(told).unwrap();
    let halt = engine.halted().expect("the halt is in force");
    assert_eq!(halt.halted, engine.epoch(), "it halts its own epoch");
    assert_eq!(halt.told, Some(told));
}

/// Durable: a restart of the store is still halted (test 2, the store's half; the marker's half is
/// the daemon's).
#[test]
fn a_halt_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let (epoch, told) = {
        let engine = open(&path);
        let told = an_epoch_ahead(1_000, 6);
        engine.halt(told).unwrap();
        (engine.epoch(), told)
    };
    let again = open(&path);
    assert_eq!(again.epoch(), epoch, "premise: a restart keeps the epoch");
    assert_eq!(again.halted().map(|h| h.told), Some(Some(told)), "and the halt");
    assert!(
        again.create_collection("shop", "orders").is_err(),
        "and refuses writes from its first moment"
    );
}

/// What wakes a stream that is open at the halt (6e): the engine hands out a watch on the halt,
/// which reads `false` until the halt and `true` from then on, and a subscriber is told of the
/// change.
#[test]
fn a_halt_is_heard_by_a_subscriber() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(&dir.path().join("kimmy.redb"));
    let mut heard = engine.halt_watch();
    assert!(!*heard.borrow_and_update(), "premise: not halted");
    engine.halt(an_epoch_ahead(1_000, 7)).unwrap();
    assert!(heard.has_changed().unwrap(), "the subscriber is told of the halt");
    assert!(*heard.borrow_and_update(), "and reads halted");
}

/// A store that starts halted reads halted to a subscriber from the first moment, so a stream
/// opened on it is closed by the pump's own look after subscribing.
#[test]
fn a_store_that_starts_halted_reads_halted_to_a_subscriber() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let engine = open(&path);
        engine.halt(an_epoch_ahead(1_000, 8)).unwrap();
    }
    let again = open(&path);
    assert!(*again.halt_watch().borrow(), "halted at the open");
}

/// A backup of a halted host, restored, is a new epoch: the row names the epoch it halts, so the
/// restored host is not halted (the review of the plan, B).
#[test]
fn a_restore_of_a_halted_hosts_backup_is_not_halted() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(&dir.path().join("kimmy.redb"));
    engine.create_collection("shop", "orders").unwrap();
    engine.halt(an_epoch_ahead(1_000, 7)).unwrap();
    assert!(engine.halted().is_some(), "premise: halted");
    // A halted host serves nothing, but a backup is a read of the store.
    let backup = backup_of(&engine);
    let path = dir.path().join("restored.redb");
    restore(&path, &mut backup.as_slice()).unwrap();
    let restored = open(&path);
    assert_ne!(restored.epoch(), engine.epoch(), "premise: the restore minted its own epoch");
    assert!(restored.halted().is_none(), "the halt belongs to the epoch it named");
    restored.create_collection("shop", "more").unwrap();
}

// -- the bit that tells a fresh epoch from a joined one (the guard)
// ----------------------------------

/// A fresh epoch has met no peer; noting one is remembered, and written durably from outside a
/// contact, it survives a restart.
#[test]
fn the_bit_that_a_peer_was_met_is_held_and_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let engine = open(&path);
        assert!(!engine.has_met_a_peer(), "premise: a fresh epoch has met nobody");
        engine.note_met_a_peer();
        assert!(engine.has_met_a_peer(), "noted at once, with no writer");
        engine.persist_met_a_peer().unwrap();
    }
    assert!(open(&path).has_met_a_peer(), "the bit is durable once written");
}

/// A note that was never written is lost with the process, and the guard runs again: harmless.
#[test]
fn a_bit_that_was_never_written_is_forgotten_by_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let engine = open(&path);
        engine.note_met_a_peer();
        assert!(engine.has_met_a_peer(), "premise: held in memory");
    }
    assert!(!open(&path).has_met_a_peer());
}

/// The bit belongs to the epoch: a restore of the store's backup mints a new one, and starts
/// without it.
#[test]
fn a_restore_starts_without_the_bit() {
    let dir = tempfile::tempdir().unwrap();
    let source = open(&dir.path().join("source.redb"));
    source.note_met_a_peer();
    source.persist_met_a_peer().unwrap();
    assert!(source.has_met_a_peer(), "premise: the source has met a peer");
    let backup = backup_of(&source);
    let path = dir.path().join("restored.redb");
    restore(&path, &mut backup.as_slice()).unwrap();
    assert!(!open(&path).has_met_a_peer(), "a new epoch has met nobody");
}
