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
    Marker { node: node(1), halted: epoch_starting(1_000), told: Some(epoch_starting(2_000)) }
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
    let guard_only =
        MarkerFile { marker: Some(Marker { told: None, ..a_marker() }), replace: None };
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
fn the_floor_of_a_guard_only_marker_is_the_epoch_it_halted() {
    let dir = tempfile::tempdir().unwrap();
    let marker = Marker { told: None, ..a_marker() };
    write_marker_file(dir.path(), &MarkerFile { marker: Some(marker), replace: None }).unwrap();
    assert_eq!(restore_floor(dir.path(), node(1)).unwrap(), Some(epoch_starting(1_000)));
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
