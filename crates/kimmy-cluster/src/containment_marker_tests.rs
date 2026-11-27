//! D13 containment, the containment file (ADR-274, unit 14a): what is written, read and cleared in
//! the data directory, and the floor a restore takes from it. Each test is red on its designed
//! assertion against the scaffold in `containment.rs`, whose file functions do nothing.

use kimmy_core::{Epoch, Hlc, NodeId};

use crate::containment::*;

fn epoch_starting(wall_ms: u64) -> Epoch {
    Epoch::new(Hlc::new(wall_ms, 0), [6; 8])
}

fn node(n: u8) -> NodeId {
    NodeId::from_bytes([n; 16])
}

fn a_marker() -> Marker {
    Marker { node: node(1), floor: epoch_starting(2_000), guard: None }
}

fn a_guard_halt() -> GuardHalt {
    GuardHalt { epoch: epoch_starting(1_000), address: "10.1.2.3:7900".parse().unwrap() }
}

#[test]
fn the_containment_file_is_not_named_for_a_halt() {
    assert!(
        !MARKER_FILE.contains("halted"),
        "it lives beside a healthy host as well: {MARKER_FILE}"
    );
    assert_ne!(MARKER_FILE, "kimmy.halted");
}

#[test]
fn a_directory_with_no_file_reads_as_an_empty_one() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(read_marker_file(dir.path()).unwrap(), MarkerFile::default());
    assert_eq!(restore_floor(dir.path(), node(1)).unwrap(), None);
}

#[test]
fn a_marker_and_a_replace_flag_are_written_and_read_back() {
    let dir = tempfile::tempdir().unwrap();
    let file = MarkerFile { marker: Some(a_marker()), replace: Some(epoch_starting(3_000)) };
    write_marker_file(dir.path(), &file).unwrap();
    assert!(dir.path().join(MARKER_FILE).exists(), "the file is in the data directory");
    assert_eq!(read_marker_file(dir.path()).unwrap(), file);
    let guard_only = MarkerFile {
        marker: Some(Marker { guard: Some(a_guard_halt()), ..a_marker() }),
        replace: None,
    };
    write_marker_file(dir.path(), &guard_only).unwrap();
    assert_eq!(
        read_marker_file(dir.path()).unwrap(),
        guard_only,
        "a rewrite replaces the whole file"
    );
}

/// A crash leaves the old file or the new one, never half of one, and nothing else is left beside
/// it: the write goes through a temporary name that is renamed over the file.
#[test]
fn a_write_leaves_the_one_file_and_no_temporary() {
    let dir = tempfile::tempdir().unwrap();
    write_marker_file(dir.path(), &MarkerFile { marker: Some(a_marker()), replace: None }).unwrap();
    let names: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names, [MARKER_FILE.to_string()]);
}

#[test]
fn clearing_removes_the_file_and_clearing_an_absent_one_is_not_an_error() {
    let dir = tempfile::tempdir().unwrap();
    write_marker_file(dir.path(), &MarkerFile { marker: Some(a_marker()), replace: None }).unwrap();
    assert!(dir.path().join(MARKER_FILE).exists(), "premise: the file is written");
    clear_marker_file(dir.path()).unwrap();
    assert!(!dir.path().join(MARKER_FILE).exists());
    assert_eq!(read_marker_file(dir.path()).unwrap(), MarkerFile::default());
    clear_marker_file(dir.path()).unwrap();
}

#[test]
fn a_restore_floor_is_the_newer_epoch_the_marker_names() {
    let dir = tempfile::tempdir().unwrap();
    write_marker_file(dir.path(), &MarkerFile { marker: Some(a_marker()), replace: None }).unwrap();
    assert_eq!(restore_floor(dir.path(), node(1)).unwrap(), Some(epoch_starting(2_000)));
}

#[test]
fn a_guard_only_marker_has_no_floor_and_a_guard_marker_keeps_the_floor_it_had() {
    let dir = tempfile::tempdir().unwrap();
    let guard_only = Marker { floor: Epoch::ZERO, guard: Some(a_guard_halt()), ..a_marker() };
    write_marker_file(dir.path(), &MarkerFile { marker: Some(guard_only), replace: None }).unwrap();
    assert_eq!(restore_floor(dir.path(), node(1)).unwrap(), None, "no tell, no floor");
    let both = Marker { guard: Some(a_guard_halt()), ..a_marker() };
    write_marker_file(dir.path(), &MarkerFile { marker: Some(both), replace: None }).unwrap();
    assert_eq!(restore_floor(dir.path(), node(1)).unwrap(), Some(epoch_starting(2_000)));
}

/// A marker of another node, and a file with only a `--replace` flag, set no floor: a restore of a
/// different node's backup into the directory is not constrained by a marker that is not its own.
#[test]
fn a_marker_of_another_node_or_a_bare_replace_flag_sets_no_floor() {
    let dir = tempfile::tempdir().unwrap();
    write_marker_file(dir.path(), &MarkerFile { marker: Some(a_marker()), replace: None }).unwrap();
    assert_eq!(restore_floor(dir.path(), node(2)).unwrap(), None, "another node");
    write_marker_file(dir.path(), &MarkerFile { marker: None, replace: Some(epoch_starting(9)) })
        .unwrap();
    assert_eq!(restore_floor(dir.path(), node(1)).unwrap(), None, "no marker");
}

// -- two writers at once: the file is read, merged and written (ADR-274)
// -------------------------------------------------------------------

/// One writer of a directory's containment file, held between its read and its write while a second runs, and then let go. The
/// second is given two seconds to finish before the first is released, and is not waited for: where each writer's read and write
/// are one step, it cannot finish until the first has written.
fn with_the_first_held(
    dir: &std::path::Path,
    first: impl FnOnce() + Send,
    second: impl FnOnce() + Send,
) {
    use std::sync::mpsc;
    use std::time::Duration;
    let (at_the_hold, held) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    between_read_and_write::hold_the_next(dir, move || {
        at_the_hold.send(()).unwrap();
        released.recv_timeout(Duration::from_secs(30)).expect("the held writer is released");
    });
    std::thread::scope(|scope| {
        let first = scope.spawn(first);
        held.recv_timeout(Duration::from_secs(30)).expect("the first writer reached its write");
        let (finished, second_finished) = mpsc::channel();
        let second = scope.spawn(move || {
            second();
            let _ = finished.send(());
        });
        let _ = second_finished.recv_timeout(Duration::from_secs(2));
        release.send(()).unwrap();
        first.join().unwrap();
        second.join().unwrap();
    });
}

fn a_store(dir: &std::path::Path) -> kimmy_storage::Engine {
    kimmy_storage::Engine::open(&dir.join("kimmy.redb")).unwrap()
}

fn the_marker(dir: &std::path::Path) -> Marker {
    read_marker_file(dir).unwrap().marker.expect("a marker")
}

/// Two tells of two newer epochs at once, the lower one's writer reading first and writing last: the floor, which a recovery
/// restore mints above, is the higher.
#[test]
fn two_tells_at_once_leave_the_higher_floor() {
    let dir = tempfile::tempdir().unwrap();
    let engine = a_store(dir.path());
    let me = engine.node_id();
    let (lower, higher) = (epoch_starting(2_000), epoch_starting(3_000));
    with_the_first_held(
        dir.path(),
        || record_the_tell(&engine, me, lower).expect("the lower tell is recorded"),
        || record_the_tell(&engine, me, higher).expect("the higher tell is recorded"),
    );
    assert_eq!(
        the_marker(dir.path()).floor,
        higher,
        "the floor is the higher of the two, whichever wrote last"
    );
}

/// The guard's own halt is the other writer: a tell that lands while it records keeps its floor, and the guard's halt stays.
#[test]
fn a_tell_while_the_guard_records_its_halt_keeps_its_floor_and_the_halt() {
    let dir = tempfile::tempdir().unwrap();
    let engine = a_store(dir.path());
    let me = engine.node_id();
    let told = epoch_starting(3_000);
    let address = a_guard_halt().address;
    with_the_first_held(
        dir.path(),
        || halt_by_guard(&engine, address),
        || record_the_tell(&engine, me, told).expect("the tell is recorded"),
    );
    let marker = the_marker(dir.path());
    assert_eq!(marker.floor, told, "the tell's floor stays under the guard's halt");
    assert_eq!(
        marker.guard.map(|guard| guard.address),
        Some(address),
        "and the halt under the tell"
    );
}

/// Many tells at once: each is recorded, the file parses at every read, and the floor ends at the highest.
#[test]
fn tells_from_many_writers_at_once_all_land_and_the_file_always_parses() {
    use std::sync::atomic::{AtomicBool, Ordering};
    const WRITERS: u64 = 4;
    const EACH: u64 = 100;
    let dir = tempfile::tempdir().unwrap();
    let engine = a_store(dir.path());
    let me = engine.node_id();
    let done = AtomicBool::new(false);
    let (writers, unread) = std::thread::scope(|scope| {
        let reader = scope.spawn(|| {
            let mut unread = Vec::new();
            while !done.load(Ordering::Relaxed) {
                if let Err(e) = read_marker_file(dir.path()) {
                    unread.push(e.to_string());
                }
            }
            unread
        });
        let (engine, writers) = (&engine, 0..WRITERS);
        let writers: Vec<_> = writers
            .map(|writer| {
                scope.spawn(move || {
                    (0..EACH)
                        .filter_map(|i| {
                            let epoch = epoch_starting(1_000 + i * WRITERS + writer);
                            record_the_tell(engine, me, epoch).err().map(|e| e.to_string())
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        // Every writer is joined and the reader told to stop before anything is asserted, or a failure would leave it reading.
        let writers: Vec<_> = writers.into_iter().map(|writer| writer.join()).collect();
        done.store(true, Ordering::Relaxed);
        (writers, reader.join())
    });
    let unrecorded: Vec<String> = writers.into_iter().flat_map(|w| w.unwrap()).collect();
    assert!(
        unrecorded.is_empty(),
        "{} tells were not recorded, the first: {:?}",
        unrecorded.len(),
        unrecorded.first()
    );
    assert_eq!(unread.unwrap(), Vec::<String>::new(), "the file parses at every read");
    assert_eq!(the_marker(dir.path()).floor, epoch_starting(1_000 + EACH * WRITERS - 1));
}
