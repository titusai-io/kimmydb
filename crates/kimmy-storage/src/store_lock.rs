//! The lock on `kimmy.redb`, taken before ADR-190's check confirms what it read
//! and before anything is written, and held until redb closes the store.
//!
//! redb 4.1 locked the file when its `FileBackend` was built, which is what
//! the engine's open relied on: the check's reads were confirmed under that
//! lock, and only then was the sidecar written. redb 4.3 takes no lock there.
//! It locks from inside its open, through lock methods on the storage backend,
//! and a backend that does not implement them opens with no lock at all,
//! silently. Left alone, the bump let a second process open a store a first
//! one was writing.
//!
//! So the engine takes the lock itself, first, as two locks on the same open
//! file description:
//!
//! - **redb's whole-storage range lock**, through the `FileBackend`. It
//!   excludes every redb 4.3 opener, read-write or read-only, in this process
//!   or another.
//! - **On Linux, a `flock`**, which is what redb 4.1 took, and so what excludes
//!   a 0.36.x node on the same store. On Linux the two are separate lock
//!   namespaces; on macOS they are one, and the range lock alone excludes a
//!   `flock` holder, so the `flock` is not taken there (taking it would
//!   conflict with our own range lock).
//!
//! redb then asks the backend for its lock. The backend answers as the
//! trait's documented second level, which supports only a whole-storage lock:
//! redb asks for its own ranges first, is told they are unsupported, falls back
//! to the whole storage, and is told it holds it, which it does. Only redb's
//! `ExclusiveWriter` mode works at that level, and it is the only one the
//! engine uses.
//!
//! **What may not happen is an open with no lock.** redb opens unlocked when a
//! backend answers every request with "unsupported", so the whole-storage
//! request is never answered that way, anything redb asks for after it is
//! granted is an error, and the engine refuses a store redb opened without
//! having asked ([`LockGrant`]). A redb that changes what it asks for fails
//! `redb_asks_for_the_whole_storage_lock_after_its_own_ranges`.

use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use redb::{BackendError, StorageBackend};

/// Whether redb, opening through the backend, asked for the whole-storage
/// lock and was told it holds it.
#[derive(Clone, Debug)]
pub(crate) struct LockGrant(Arc<AtomicBool>);

impl LockGrant {
    pub(crate) fn granted(&self) -> bool {
        #[cfg(test)]
        if test_hooks::PRETEND_NOT_GRANTED.with(|p| p.get()) {
            return false;
        }
        self.0.load(Ordering::SeqCst)
    }
}

/// The store's lock taken again by [`StoreLock::relock`], released on drop.
pub(crate) struct Relocked {
    backend: redb::backends::FileBackend,
    lock: StoreLock,
}

impl Drop for Relocked {
    fn drop(&mut self) {
        let _ = self.backend.close();
        self.lock.release();
    }
}

/// The two locks, held for as long as the backend lives.
#[derive(Debug)]
pub(crate) struct StoreLock {
    /// The `flock`, on a descriptor sharing the backend's open file
    /// description. Released on close, or when this is dropped.
    #[cfg(target_os = "linux")]
    flock: std::sync::Mutex<Option<std::fs::File>>,
    granted: Arc<AtomicBool>,
}

fn is_whole_storage(start: Bound<u64>, end: Bound<u64>) -> bool {
    matches!(start, Bound::Unbounded | Bound::Included(0)) && end == Bound::Unbounded
}

impl StoreLock {
    /// Take both locks. `file` shares its open file description with the
    /// file `backend` was built on. `DatabaseAlreadyOpen` when either lock is
    /// held elsewhere.
    pub(crate) fn take(
        file: &std::fs::File,
        backend: &redb::backends::FileBackend,
    ) -> Result<Self, redb::DatabaseError> {
        #[cfg(target_os = "linux")]
        let flock = {
            let clone = file.try_clone()?;
            match clone.try_lock() {
                Ok(()) => Some(clone),
                Err(std::fs::TryLockError::WouldBlock) => {
                    return Err(redb::DatabaseError::DatabaseAlreadyOpen);
                }
                // Where `flock` cannot lock, redb 4.1 could not have either,
                // so no 0.36.x node can be holding the store by one.
                Err(std::fs::TryLockError::Error(e))
                    if e.kind() == std::io::ErrorKind::Unsupported =>
                {
                    None
                }
                Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
            }
        };
        #[cfg(not(target_os = "linux"))]
        let _ = file;
        match backend.try_lock_range(Bound::Unbounded, Bound::Unbounded) {
            Ok(true) => {}
            Ok(false) => return Err(redb::DatabaseError::DatabaseAlreadyOpen),
            Err(BackendError::Unsupported) => {
                return Err(std::io::Error::other(
                    "this platform cannot lock the database file, and a store is never opened \
                     for writing unlocked",
                )
                .into());
            }
            Err(BackendError::Io(e)) => return Err(e.into()),
            Err(other) => {
                return Err(
                    std::io::Error::other(format!("locking the database: {other:?}")).into()
                );
            }
        }
        Ok(StoreLock {
            #[cfg(target_os = "linux")]
            flock: std::sync::Mutex::new(flock),
            granted: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Take the lock again on a store redb has closed, for as long as the
    /// returned pair lives: `None` if anything else holds it, or it cannot be
    /// opened.
    pub(crate) fn relock(database: &std::path::Path) -> Option<Relocked> {
        let file = std::fs::OpenOptions::new().read(true).write(true).open(database).ok()?;
        let clone = file.try_clone().ok()?;
        let backend = redb::backends::FileBackend::new(file).ok()?;
        let lock = StoreLock::take(&clone, &backend).ok()?;
        Some(Relocked { backend, lock })
    }

    pub(crate) fn grant(&self) -> LockGrant {
        LockGrant(Arc::clone(&self.granted))
    }

    /// redb asking for a lock. Its own ranges are unsupported until it asks for
    /// the whole storage, which it is told it holds; after that, any request is
    /// one this backend was not built for, and an error.
    pub(crate) fn request(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
        shared: bool,
    ) -> Result<bool, BackendError> {
        #[cfg(test)]
        test_hooks::record((start, end), shared);
        if shared {
            return Err(unexpected("a shared lock", start, end));
        }
        if is_whole_storage(start, end) {
            self.granted.store(true, Ordering::SeqCst);
            return Ok(true);
        }
        if self.granted.load(Ordering::SeqCst) {
            return Err(unexpected("a range lock after the whole storage", start, end));
        }
        Err(BackendError::Unsupported)
    }

    /// redb releasing a lock. The whole-storage lock is held until the backend
    /// closes, so releasing it early is a no-op; nothing else was granted.
    pub(crate) fn unlock(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        if is_whole_storage(start, end) {
            Ok(())
        } else {
            Err(unexpected("the release of a range it was never granted", start, end))
        }
    }

    /// The `flock`'s release; the range lock goes with the `FileBackend`'s
    /// close.
    pub(crate) fn release(&self) {
        #[cfg(target_os = "linux")]
        if let Some(file) = self.flock.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = file.unlock();
        }
    }
}

/// A lock request this backend was not built for.
pub(crate) fn unexpected(what: &str, start: Bound<u64>, end: Bound<u64>) -> BackendError {
    let message = format!(
        "redb asked the storage backend for {what} ({start:?}..{end:?}); it holds the whole \
         file and answers nothing else (ADR-190), so a redb that locks differently needs the \
         backend changed first"
    );
    tracing::error!("{message}");
    BackendError::Io(std::io::Error::other(message))
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::cell::{Cell, RefCell};
    use std::ops::Bound;

    type Request = ((Bound<u64>, Bound<u64>), bool);

    type Probe = Box<dyn FnMut(&std::path::Path)>;

    thread_local! {
        /// Every lock request redb made on this thread, and whether it was shared.
        pub static REQUESTS: RefCell<Vec<Request>> = const { RefCell::new(Vec::new()) };
        /// Report redb's open as not having asked for the lock.
        pub static PRETEND_NOT_GRANTED: Cell<bool> = const { Cell::new(false) };
        /// Run by the engine's open just before it writes the sidecar.
        pub static BEFORE_SIDECAR_WRITE: RefCell<Option<Probe>> = const { RefCell::new(None) };
        /// Run after a failed open, just before the lock is taken again to put
        /// the sidecar back.
        pub static BEFORE_PUT_BACK: RefCell<Option<Probe>> = const { RefCell::new(None) };
    }

    pub fn before_put_back(path: &std::path::Path) {
        BEFORE_PUT_BACK.with(|p| {
            if let Some(probe) = p.borrow_mut().as_mut() {
                probe(path);
            }
        });
    }

    pub fn before_sidecar_write(path: &std::path::Path) {
        BEFORE_SIDECAR_WRITE.with(|p| {
            if let Some(probe) = p.borrow_mut().as_mut() {
                probe(path);
            }
        });
    }

    pub fn record(range: (Bound<u64>, Bound<u64>), shared: bool) {
        REQUESTS.with(|r| r.borrow_mut().push((range, shared)));
    }

    pub fn take() -> Vec<Request> {
        REQUESTS.with(|r| std::mem::take(&mut *r.borrow_mut()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a_lock() -> (tempfile::TempDir, redb::backends::FileBackend, StoreLock) {
        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::File::create(dir.path().join("kimmy.redb")).unwrap();
        let clone = file.try_clone().unwrap();
        let backend = redb::backends::FileBackend::new(file).unwrap();
        let lock = StoreLock::take(&clone, &backend).unwrap();
        (dir, backend, lock)
    }

    /// Before the grant, redb's own ranges are unsupported, so it falls back;
    /// the whole storage is granted; after that nothing else is, and a shared
    /// lock never is: none of them answers "unsupported", which would open
    /// unlocked.
    #[test]
    fn only_the_whole_storage_is_granted_and_nothing_after_it_is_unsupported() {
        let (_dir, _backend, lock) = a_lock();
        let suffix = (Bound::Included(1 << 62), Bound::Unbounded);
        assert!(matches!(lock.request(suffix.0, suffix.1, false), Err(BackendError::Unsupported)));
        assert!(!lock.grant().granted());
        assert!(matches!(lock.request(Bound::Unbounded, Bound::Unbounded, false), Ok(true)));
        assert!(lock.grant().granted());
        assert!(matches!(lock.request(suffix.0, suffix.1, false), Err(BackendError::Io(_))));
        assert!(matches!(
            lock.request(Bound::Unbounded, Bound::Unbounded, true),
            Err(BackendError::Io(_))
        ));
        assert!(matches!(lock.unlock(suffix.0, suffix.1), Err(BackendError::Io(_))));
        assert!(lock.unlock(Bound::Included(0), Bound::Unbounded).is_ok());
    }
}
