//! A backup, spilled to a file beside the database and then streamed (ADR-170).
//!
//! ADR-041's rule is that the backup's read transaction is closed before the
//! first byte goes to the client, so a slow client cannot pin redb's MVCC pages.
//! That rule stands. What this module changes is where the finished backup
//! waits while the client reads: an unlinked temporary file in the data
//! directory, not a `Vec` in the heap. The heap copy cost a member one backup's
//! worth of resident memory, and up to twice that while the `Vec` grew, for as
//! long as the client took to read it.

use std::io::{self, BufWriter, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use kimmy_storage::backup::BackupInfo;
use kimmy_storage::{Engine, StorageError};

/// A backup written out in full, with its read transaction already closed.
pub(crate) struct Spilled<F> {
    pub info: BackupInfo,
    /// Positioned at the start, ready to be read out.
    pub file: F,
    /// The file's length: what the response's `Content-Length` says.
    pub len: u64,
    /// The walk and the write together, which is what a backup costs the node.
    pub elapsed: Duration,
}

/// A backup that could not be written, and the directory it was written into.
///
/// The directory rides with the error because the likeliest cause is that
/// directory's disk: a backup needs free space equal to one backup beside the
/// store, and an operator reading the log line needs to know which volume to
/// look at.
#[derive(Debug)]
pub(crate) struct SpillFailed {
    pub dir: PathBuf,
    pub error: StorageError,
}

impl std::fmt::Display for SpillFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "spilling a backup into {}: {}", self.dir.display(), self.error)
    }
}

/// Where a backup spills: the directory holding the database file, which is
/// the volume an operator has already sized for the store.
pub(crate) fn spill_dir(engine: &Engine) -> PathBuf {
    match engine.path().parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// Take a backup of `engine` into an unlinked temporary file in `dir`.
///
/// Unlinked from the moment it is created, so a crash, a client that
/// disconnects or a panic cannot leave a backup-sized file behind: the space
/// comes back when the last descriptor closes, and there is nothing to sweep
/// at start-up.
///
/// Synchronous and long: the whole store is walked. The caller runs it off the
/// async runtime.
pub(crate) fn spill(engine: &Engine, dir: &Path) -> Result<Spilled<std::fs::File>, SpillFailed> {
    spill_with(engine, dir, unlinked_file_in)
}

/// A new file in `dir` that no name reaches: created under a name nothing else
/// holds, readable only by this user, and unlinked while it is held open.
///
/// Written out rather than taken from `tempfile`, whose Linux build brings
/// `linux-raw-sys` into the default dependency graph for one call (ADR-170).
fn unlinked_file_in(dir: &Path) -> io::Result<std::fs::File> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    loop {
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!(".kimmy-backup-{}-{n}.spill", std::process::id()));
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true).create_new(true);
        // Every document on the node, users' password hashes included: no
        // other account reads it in the instant it has a name.
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        match options.open(&path) {
            Ok(file) => {
                std::fs::remove_file(&path)?;
                return Ok(file);
            }
            // A name a crashed process left behind: take the next one.
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
}

/// [`spill`], with the file supplied by `open` — so a test can hand it a file
/// whose disk is full.
pub(crate) fn spill_with<F: Write + Seek>(
    engine: &Engine,
    dir: &Path,
    open: impl FnOnce(&Path) -> io::Result<F>,
) -> Result<Spilled<F>, SpillFailed> {
    let failed = |error| SpillFailed { dir: dir.to_path_buf(), error };
    let started = Instant::now();

    let file = open(dir).map_err(|e| {
        failed(StorageError::Database(format!("creating the file a backup spills into: {e}")))
    })?;
    // Buffered, because `backup_to` writes a record at a time and each would
    // otherwise be a system call.
    let mut out = BufWriter::with_capacity(1 << 16, file);
    // The route already runs this on a blocking thread, where the wrapper
    // simply runs the walk; it is here so that no other caller can put a walk
    // of the whole store on an async worker (ADR-153).
    let info = kimmy_storage::blocking(|| engine.backup_to(&mut out)).map_err(failed)?;
    let mut file = out
        .into_inner()
        .map_err(|e| failed(StorageError::Database(format!("writing a backup: {}", e.error()))))?;
    let len = file
        .stream_position()
        .and_then(|len| file.rewind().map(|()| len))
        .map_err(|e| failed(StorageError::Database(format!("rewinding a backup: {e}"))))?;

    Ok(Spilled { info, file, len, elapsed: started.elapsed() })
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    fn engine() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let coll = engine.create_collection("shop", "orders").unwrap();
        engine.insert(&coll, bson::doc! { "_id": 1, "v": "present" }).unwrap();
        (engine, dir)
    }

    /// A file on a disk with no room left: every write fails the way the
    /// kernel fails it, `ENOSPC`.
    struct FullDisk;

    impl Write for FullDisk {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::from_raw_os_error(28))
        }
        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::from_raw_os_error(28))
        }
    }

    impl Seek for FullDisk {
        fn seek(&mut self, _: io::SeekFrom) -> io::Result<u64> {
            Ok(0)
        }
    }

    #[test]
    fn a_spilled_backup_is_the_backup_from_its_first_byte() {
        let (engine, dir) = engine();
        let mut spilled = spill(&engine, dir.path()).unwrap();

        let mut bytes = Vec::new();
        spilled.file.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes.len() as u64, spilled.len, "the length is the whole file");
        assert_eq!(spilled.len, spilled.info.bytes as u64, "and what the walk wrote");
        assert!(bytes.starts_with(b"KIMMYBK1"), "read from the start, not from the end");

        let restored = tempfile::tempdir().unwrap();
        let path = restored.path().join("kimmy.redb");
        kimmy_storage::backup::restore(&path, &mut bytes.as_slice()).unwrap();
        let engine = Engine::open(&path).unwrap();
        let coll = engine.get_collection("shop", "orders").unwrap();
        assert_eq!(engine.count(&coll).unwrap(), 1);
    }

    #[test]
    fn a_spill_leaves_no_file_in_the_directory() {
        let (engine, dir) = engine();
        let before: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        let spilled = spill(&engine, dir.path()).unwrap();
        let during: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert_eq!(before.len(), during.len(), "the spill file is unlinked while it is open");
        drop(spilled);
    }

    /// The failure an operator is likeliest to meet: the data directory's disk
    /// has no room for one more backup. What is logged names the directory and
    /// keeps the kernel's own words for the cause.
    #[test]
    fn a_full_disk_names_the_directory_and_the_os_error() {
        let (engine, dir) = engine();
        let failure = spill_with(&engine, dir.path(), |_| Ok(FullDisk)).err().expect("a failure");
        let message = failure.to_string();
        assert!(message.contains(&dir.path().display().to_string()), "{message}");
        assert!(message.contains(&io::Error::from_raw_os_error(28).to_string()), "{message}");
    }

    #[test]
    fn a_directory_that_cannot_hold_the_file_is_named() {
        let (engine, dir) = engine();
        let missing = dir.path().join("not-here");
        let failure = spill(&engine, &missing).err().expect("a failure");
        let message = failure.to_string();
        assert!(message.contains(&missing.display().to_string()), "{message}");
        assert!(message.contains(&io::Error::from_raw_os_error(2).to_string()), "{message}");
    }
}
