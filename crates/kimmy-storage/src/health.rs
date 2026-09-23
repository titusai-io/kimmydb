//! A storage engine that has hit an I/O error, and the one reaction to it
//! (ADR-188).
//!
//! redb latches the first error any backend call returns, of any kind, and
//! answers every later read and write with `PreviousIo` until the database is
//! closed and reopened. Measured against redb 4.1.0, one failed `write`,
//! `sync_data`, `set_len` or `read` does it, with ENOSPC or EIO, even when the
//! disk is healthy again at the next call. So there is no transient case: the
//! first error is the moment the engine stops being able to serve, and it is
//! recorded here, at the one backend every engine byte passes through.
//!
//! Nothing here touches the database. The reaction runs on the thread that hit
//! the error, possibly inside a write transaction that holds the writer, and
//! anything that took an engine lock or opened a transaction there could
//! deadlock.

use std::sync::atomic::{AtomicU8, Ordering};
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
}

impl Default for StorageHealth {
    fn default() -> Self {
        Self {
            failed: OnceLock::new(),
            reaction: OnceLock::new(),
            reacted: Once::new(),
            armed: AtomicU8::new(0),
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

    /// Arm the test switch to fail `call` once. Returns whether `call` names
    /// one of the backend's calls.
    pub(crate) fn arm(&self, call: &str) -> bool {
        match CALLS.iter().position(|c| *c == call) {
            Some(i) => {
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
        let i = CALLS.iter().position(|c| *c == call)? as u8 + 1;
        self.armed
            .compare_exchange(i, 0, Ordering::SeqCst, Ordering::SeqCst)
            .ok()
            .map(|_| std::io::Error::from_raw_os_error(5))
    }
}
