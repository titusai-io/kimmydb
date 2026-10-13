//! The open's rebuild of the live counts and the digests with the values it
//! ships with: the stop looked for every 256 rows, the chunk cut at 50,000
//! rows or 64 MiB. The unit tests of `live_walk` look at every row and cut a
//! chunk at a handful of rows, so they cannot tell these from other values;
//! this runs the library as shipped, with only the stop placed by a hook. It
//! lives in `kimmyd`'s tests because those build the storage crate with
//! `test-hooks`, as every test that needs a hook does.

use std::cell::RefCell;
use std::io::Write;

use bson::doc;
use kimmy_storage::{Engine, OpenOptions, StorageError};

thread_local! {
    static CAPTURE: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
}

struct Sink;

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        CAPTURE.with(|c| {
            if let Some(out) = c.borrow_mut().as_mut() {
                out.extend_from_slice(buf);
            }
        });
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// What `body` logs on this thread.
fn logs_of(body: impl FnOnce()) -> String {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt().with_writer(|| Sink).with_ansi(false).finish(),
        )
        .unwrap();
    });
    CAPTURE.with(|c| *c.borrow_mut() = Some(Vec::new()));
    body();
    CAPTURE.with(|c| String::from_utf8(c.borrow_mut().take().unwrap_or_default()).unwrap())
}

/// The value of `field=` in the one log line that contains `line`.
fn field(logs: &str, line: &str, field: &str) -> u64 {
    let line = logs
        .lines()
        .find(|l| l.contains(line))
        .unwrap_or_else(|| panic!("no line {line:?} in:\n{logs}"));
    let (_, rest) = line.split_once(&format!("{field}=")).expect("the field");
    rest.split(|c: char| !c.is_ascii_digit()).next().unwrap().parse().unwrap()
}

fn a_store(
    docs: impl FnOnce(&Engine, &kimmy_storage::CollectionMeta),
) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let engine = Engine::open(&path).unwrap();
    let coll = engine.create_collection("app", "docs").unwrap();
    docs(&engine, &coll);
    (dir, path)
}

#[test]
fn the_stop_is_looked_for_every_256_rows_inside_a_chunk() {
    let (_dir, path) = a_store(|engine, coll| {
        engine.insert_many(coll, (0..3_000i64).map(|i| doc! { "_id": i }).collect()).unwrap();
    });
    // Asked for once 1,000 rows are processed: the first look at or after it
    // is at row 1,024, the next multiple of 256.
    kimmy_storage::stop_open_after_rows_for_test(1_000);
    let mut stopped = false;
    let logs = logs_of(|| {
        stopped = matches!(
            Engine::open_with(&path, &OpenOptions::new().force_rebuild("a test")),
            Err(StorageError::Stopping(_))
        );
    });
    assert!(stopped, "the rebuild ignored the stop:\n{logs}");
    assert_eq!(field(&logs, "stopped the rebuild", "records"), 1_024, "{logs}");
}

#[test]
fn a_chunk_is_cut_at_fifty_thousand_rows() {
    let (_dir, path) = a_store(|engine, coll| {
        engine.insert_many(coll, (0..50_300i64).map(|i| doc! { "_id": i }).collect()).unwrap();
    });
    let mut opened = false;
    let logs = logs_of(|| {
        opened = Engine::open_with(&path, &OpenOptions::new().force_rebuild("a test")).is_ok();
    });
    assert!(opened, "{logs}");
    assert_eq!(field(&logs, "rebuilt the live document counts", "records"), 50_300, "{logs}");
    assert_eq!(field(&logs, "rebuilt the live document counts", "chunks"), 2, "{logs}");
}

#[test]
fn a_chunk_is_cut_at_64_mib() {
    let (_dir, path) = a_store(|engine, coll| {
        let body = "x".repeat(1024 * 1024);
        for batch in 0..9 {
            engine
                .insert_many(
                    coll,
                    (0..8i64)
                        .map(|i| doc! { "_id": batch * 8 + i, "body": body.as_str() })
                        .collect(),
                )
                .unwrap();
        }
    });
    let mut opened = false;
    let logs = logs_of(|| {
        opened = Engine::open_with(&path, &OpenOptions::new().force_rebuild("a test")).is_ok();
    });
    assert!(opened, "{logs}");
    assert_eq!(field(&logs, "rebuilt the live document counts", "records"), 72, "{logs}");
    assert_eq!(field(&logs, "rebuilt the live document counts", "chunks"), 2, "{logs}");
}
