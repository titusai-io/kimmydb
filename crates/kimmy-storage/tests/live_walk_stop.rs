//! The open's rebuild of the live counts and the digests ends at a stop with
//! the production values, which the unit tests of `live_walk` cannot reach:
//! under `cfg(test)` the check is made at every row and the chunks are a few
//! rows. This runs the library as shipped.
//!
//! The stop flag is process-wide and cannot be cleared, so the test is the only
//! one in this binary; resuming is covered in the unit tests, where the stop
//! can be placed and cleared.

use std::io::Write;
use std::sync::{Arc, Mutex};

use bson::doc;
use kimmy_storage::{Engine, OpenOptions, StorageError, request_open_stop};

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_stop_asked_for_before_a_forced_rebuild_ends_it_at_its_first_check() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let engine = Engine::open(&path).unwrap();
        let coll = engine.create_collection("app", "docs").unwrap();
        engine.insert_many(&coll, (0..50i64).map(|i| doc! { "_id": i }).collect()).unwrap();
    }
    let logs = Buffer::default();
    let writer = logs.clone();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt().with_writer(move || writer.clone()).with_ansi(false).finish(),
    )
    .unwrap();

    request_open_stop();
    let opened = Engine::open_with(&path, &OpenOptions::new().force_rebuild("a test"));
    assert!(
        matches!(opened, Err(StorageError::Stopping(_))),
        "a forced rebuild ignored the stop: {:?}",
        opened.as_ref().err()
    );
    let logged = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(
        logged.contains("stopped the rebuild of the live document counts"),
        "the stop was not the rebuild's own: {logged}"
    );
}
