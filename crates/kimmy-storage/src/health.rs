//! A storage engine that has hit an I/O error, and the one reaction to it
//! (ADR-188).
//!
//! redb latches the first error any backend call returns, of any kind, and
//! answers every later read and write with `PreviousIo` until the database is
//! closed and reopened. Measured against redb 4.1.0, one failed `write`,
//! `sync_data`, `set_len` or `read` does it, with ENOSPC or EIO, even when the
//! disk is healthy again at the next call. redb 4.3's `CheckedBackend` latches
//! the same way. So there is no transient case: the
//! first error is the moment the engine stops being able to serve, and it is
//! recorded here, at the one backend every engine byte passes through.
//!
//! Nothing here touches the database. The reaction runs on the thread that hit
//! the error, possibly inside a write transaction that holds the writer, and
//! anything that took an engine lock or opened a transaction there could
//! deadlock.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Once, OnceLock};

/// The first I/O error the storage backend returned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageFailure {
    /// The backend call that failed: `read`, `write`, `sync_data`, `set_len`
    /// or `len`.
    pub call: &'static str,
    /// The error's kind, such as `StorageFull`.
    pub kind: std::io::ErrorKind,
    /// The error as the operating system described it.
    pub error: String,
}

type Reaction = Box<dyn Fn(&StorageFailure) + Send + Sync>;

/// Whether this engine's storage has failed, shared by the engine and its
/// backend.
pub(crate) struct StorageHealth {
    failed: OnceLock<StorageFailure>,
    reaction: OnceLock<Reaction>,
    /// The reaction runs once, whichever of a failure and the installation of
    /// the reaction comes second, and however many threads fail together.
    reacted: Once,
    /// `KIMMY_TEST_FAIL_STORAGE`: the call to fail once, or 0 for none.
    armed: AtomicU8,
    /// Set with `call@write`: the armed call fails only inside the commit of
    /// a client write (`WriterHolder::Write`), so a test fails the write it
    /// is watching and not a background writer's commit that came first.
    armed_for_write: AtomicBool,
    /// Whether a `sync_data` has been attempted since the current commit
    /// began. Commits are serialized by the writer, so this belongs to the one
    /// commit in progress: a commit that fails with it set may have reached
    /// the disk, and its outcome is unknown (`StorageError::OutcomeUnknown`).
    /// Set on the attempt, not the success: a failed fsync is the case.
    synced: AtomicBool,
    /// Test builds only, `call@after-sync`: the armed call fails only once
    /// the commit in progress has attempted its fsync, to reach the steps
    /// redb takes after it.
    #[cfg(test)]
    armed_after_sync: AtomicBool,
}

thread_local! {
    /// Whether this thread is inside the commit of a client write, for the
    /// test switch's `@write` target.
    static IN_WRITE_COMMIT: Cell<bool> = const { Cell::new(false) };
}

/// Run `commit` marked as the commit of a client write when `is_write`, for
/// the test switch's `@write` target. Nothing else reads the mark.
pub(crate) fn committing<T>(is_write: bool, commit: impl FnOnce() -> T) -> T {
    let before = IN_WRITE_COMMIT.with(|c| c.replace(is_write));
    let out = commit();
    IN_WRITE_COMMIT.with(|c| c.set(before));
    out
}

impl Default for StorageHealth {
    fn default() -> Self {
        Self {
            failed: OnceLock::new(),
            reaction: OnceLock::new(),
            reacted: Once::new(),
            armed: AtomicU8::new(0),
            armed_for_write: AtomicBool::new(false),
            synced: AtomicBool::new(false),
            #[cfg(test)]
            armed_after_sync: AtomicBool::new(false),
        }
    }
}

impl std::fmt::Debug for StorageHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageHealth").field("failed", &self.failed.get()).finish()
    }
}

/// The backend calls, as the test switch names them.
const CALLS: [&str; 5] = ["read", "write", "sync_data", "set_len", "len"];

impl StorageHealth {
    /// Record `error` from `call`. Only the first is kept, and only the first
    /// runs the reaction: a later caller returns its own error normally.
    pub(crate) fn record(&self, call: &'static str, error: &std::io::Error) {
        let first = StorageFailure { call, kind: error.kind(), error: error.to_string() };
        if self.failed.set(first).is_ok() {
            self.react();
        }
    }

    pub(crate) fn failed(&self) -> Option<&StorageFailure> {
        self.failed.get()
    }

    /// Install the reaction; the first call wins. Runs it at once if the
    /// storage has already failed.
    pub(crate) fn on_failure(&self, reaction: Reaction) -> bool {
        let installed = self.reaction.set(reaction).is_ok();
        self.react();
        installed
    }

    fn react(&self) {
        if let (Some(failure), Some(reaction)) = (self.failed.get(), self.reaction.get()) {
            self.reacted.call_once(|| reaction(failure));
        }
    }

    /// A commit is starting, or has just been classified: no `sync_data`
    /// has been attempted in the commit in progress.
    pub(crate) fn begin_commit(&self) {
        self.synced.store(false, Ordering::SeqCst);
    }

    /// A `sync_data` is about to be attempted, in whichever commit is in
    /// progress.
    pub(crate) fn sync_attempted(&self) {
        self.synced.store(true, Ordering::SeqCst);
    }

    /// Whether the commit in progress has attempted a `sync_data`.
    pub(crate) fn synced(&self) -> bool {
        self.synced.load(Ordering::SeqCst)
    }

    /// Arm the test switch to fail `call` once: a backend call's name, or
    /// `call@write` to fail it only inside the commit of a client write.
    /// Returns whether `call` names one of the backend's calls.
    pub(crate) fn arm(&self, call: &str) -> bool {
        #[cfg(test)]
        let (call, after_sync) = match call.strip_suffix("@after-sync") {
            Some(call) => (call, true),
            None => (call, false),
        };
        let (call, for_write) = match call.split_once('@') {
            Some((call, "write")) => (call, true),
            Some(_) => return false,
            None => (call, false),
        };
        match CALLS.iter().position(|c| *c == call) {
            Some(i) => {
                #[cfg(test)]
                self.armed_after_sync.store(after_sync, Ordering::SeqCst);
                self.armed_for_write.store(for_write, Ordering::SeqCst);
                self.armed.store(i as u8 + 1, Ordering::SeqCst);
                true
            }
            None => false,
        }
    }

    /// The error the test switch injects into `call`, once.
    ///
    /// On every backend call, so the unarmed case, which is every call outside
    /// a test, is one relaxed load.
    pub(crate) fn injected(&self, call: &'static str) -> Option<std::io::Error> {
        if self.armed.load(Ordering::Relaxed) == 0 {
            return None;
        }
        if self.armed_for_write.load(Ordering::SeqCst) && !IN_WRITE_COMMIT.with(Cell::get) {
            return None;
        }
        #[cfg(test)]
        if self.armed_after_sync.load(Ordering::SeqCst) && !self.synced() {
            return None;
        }
        let i = CALLS.iter().position(|c| *c == call)? as u8 + 1;
        self.armed
            .compare_exchange(i, 0, Ordering::SeqCst, Ordering::SeqCst)
            .ok()
            .map(|_| std::io::Error::from_raw_os_error(5))
    }
}
