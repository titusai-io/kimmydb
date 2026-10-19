//! This member's SWIM incarnation, kept between processes (ADR-242).
//!
//! A member's identity is its address, its node id and an incarnation, and a
//! peer that holds one identity for an address refuses every other with a lower
//! incarnation, answering it with a `TurnUndead` that makes the member renew
//! one step (`Identity::renew`). A process that started at incarnation 0 every
//! time therefore walked, one renewal and one dropped set of peer blocks per
//! step, past whatever its last process had reached, and the walk grew by one
//! at every restart its peers noticed. So the last incarnation is kept in the
//! data directory, and a process starts **one above it**: above every
//! identity of this member a peer can hold, in one step.
//!
//! The file is written, fsynced and renamed into place, with its directory
//! fsynced, **before the identity it names is sent**: the first one before the
//! membership task starts, and each renewal before the datagrams of the input
//! that renewed it are dispatched. A file that is missing or cannot be read, or
//! holds a value no member reaches ([`TOO_LARGE`]), starts the member at 0,
//! which walks once, as every start did before; a write that fails is said, and
//! costs the next start at most the renewals it lost.

use std::path::{Path, PathBuf};

use tracing::{info, warn};

/// The file's name, in the data directory.
pub const FILE: &str = "kimmy.swim-incarnation";

/// The least recorded value taken as one the file cannot hold: no count of
/// starts and renewals reaches it, and a start above it could come to the top
/// of the range, where `Identity::renew` wraps to 0, which loses to the identity
/// it renews, so the member could never be taken again. Such a file is read as
/// one that does not hold a number.
pub(crate) const TOO_LARGE: u64 = 1 << 63;

/// The first incarnation of a process whose data directory recorded `last`:
/// one above it, and 0 when nothing was recorded.
pub(crate) fn first_after(last: Option<u64>) -> u64 {
    last.map_or(0, |last| last.saturating_add(1))
}

/// Where this member keeps its incarnation, and the one it has now.
#[derive(Debug)]
pub struct IncarnationFile {
    path: PathBuf,
    current: u64,
}

impl IncarnationFile {
    /// Read what `data_dir` recorded, choose this process's first incarnation
    /// from it ([`first_after`]), and record that durably before answering.
    /// Never fails: a file that cannot be read starts at 0, and one that cannot
    /// be written is said and the member starts all the same.
    pub fn begin(data_dir: &Path) -> Self {
        let path = data_dir.join(FILE);
        let last = read(&path);
        let file = Self { path, current: first_after(last) };
        file.write();
        info!(incarnation = file.current, previous = ?last, "this member's SWIM incarnation");
        file
    }

    /// The incarnation this member's identity has now.
    pub fn current(&self) -> u64 {
        self.current
    }

    /// The identity was renewed to `incarnation` in this process: record it,
    /// durably, before anything carries it.
    pub(crate) fn record(&mut self, incarnation: u64) {
        if incarnation == self.current {
            return;
        }
        self.current = incarnation;
        self.write();
    }

    /// The write and its two fsyncs run through [`kimmy_storage::blocking`]:
    /// both callers are on the runtime (the start in `spawn_cluster`, a renewal
    /// in the membership loop), whose worker this tells to move its other tasks
    /// elsewhere while it waits on the disk.
    fn write(&self) {
        let path = &self.path;
        let text = format!("{}\n", self.current);
        if let Err(e) = kimmy_storage::blocking(|| crate::catchup::write_atomic(path, &text)) {
            warn!(
                error = %e,
                path = %path.display(),
                incarnation = self.current,
                "the SWIM incarnation could not be recorded; the next start may renew its \
                 identity several times before its peers take it"
            );
        }
    }
}

/// What the file says, `None` when it is missing, unreadable or not a number.
fn read(path: &Path) -> Option<u64> {
    let text = match kimmy_storage::blocking(|| std::fs::read_to_string(path)) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            warn!(
                error = %e,
                path = %path.display(),
                "the SWIM incarnation file could not be read; this member starts at incarnation \
                 0 and may renew its identity several times before its peers take it"
            );
            return None;
        }
    };
    match text.trim().parse::<u64>() {
        Ok(last) if last < TOO_LARGE => Some(last),
        Ok(last) => {
            warn!(
                path = %path.display(),
                recorded = last,
                "the SWIM incarnation file holds a value no member reaches; this member starts at \
                 incarnation 0 and may renew its identity several times before its peers take it"
            );
            None
        }
        Err(_) => {
            warn!(
                path = %path.display(),
                "the SWIM incarnation file does not hold a number; this member starts at \
                 incarnation 0 and may renew its identity several times before its peers take it"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn on_disk(dir: &Path) -> String {
        std::fs::read_to_string(dir.join(FILE)).unwrap()
    }

    #[test]
    fn a_start_is_one_above_the_last_recorded_and_zero_with_none() {
        assert_eq!(first_after(None), 0);
        assert_eq!(first_after(Some(0)), 1);
        assert_eq!(first_after(Some(41)), 42);
        assert_eq!(first_after(Some(u64::MAX)), u64::MAX, "saturates, never wraps to 0");
    }

    #[test]
    fn a_first_start_records_zero() {
        let dir = tempfile::tempdir().unwrap();
        let file = IncarnationFile::begin(dir.path());
        assert_eq!(file.current(), 0);
        assert_eq!(on_disk(dir.path()), "0\n", "recorded before the member announces it");
    }

    #[test]
    fn each_start_takes_one_above_the_last() {
        let dir = tempfile::tempdir().unwrap();
        for expected in 0..5 {
            let file = IncarnationFile::begin(dir.path());
            assert_eq!(file.current(), expected);
            assert_eq!(on_disk(dir.path()), format!("{expected}\n"));
        }
    }

    #[test]
    fn a_renewal_is_recorded_and_the_next_start_is_above_it() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(FILE), "7\n").unwrap();
        let mut file = IncarnationFile::begin(dir.path());
        assert_eq!(file.current(), 8);
        file.record(9);
        assert_eq!(file.current(), 9);
        assert_eq!(on_disk(dir.path()), "9\n");
        assert_eq!(IncarnationFile::begin(dir.path()).current(), 10);
    }

    #[test]
    fn a_file_that_is_not_a_number_starts_at_zero_and_is_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(FILE), "not a number").unwrap();
        let file = IncarnationFile::begin(dir.path());
        assert_eq!(file.current(), 0);
        assert_eq!(on_disk(dir.path()), "0\n");
    }

    #[test]
    fn a_value_no_member_reaches_starts_at_zero_and_is_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        for value in [TOO_LARGE, u64::MAX - 1, u64::MAX] {
            std::fs::write(dir.path().join(FILE), format!("{value}\n")).unwrap();
            let file = IncarnationFile::begin(dir.path());
            assert_eq!(file.current(), 0, "{value} is not taken");
            assert_eq!(on_disk(dir.path()), "0\n");
        }
        std::fs::write(dir.path().join(FILE), format!("{}\n", TOO_LARGE - 1)).unwrap();
        assert_eq!(IncarnationFile::begin(dir.path()).current(), TOO_LARGE, "the last value taken");
    }

    /// A file that is there but cannot be read (here a directory stands at its
    /// path, which no write can replace either) starts at 0 and never refuses
    /// the start.
    #[test]
    fn a_file_that_cannot_be_read_starts_at_zero() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(FILE)).unwrap();
        let mut file = IncarnationFile::begin(dir.path());
        assert_eq!(file.current(), 0);
        file.record(1);
        assert_eq!(file.current(), 1, "the member goes on with the renewed identity");
        assert!(dir.path().join(FILE).is_dir(), "nothing was written over it");
    }

    #[test]
    fn a_directory_that_cannot_take_the_file_still_starts() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("no-such-directory");
        let mut file = IncarnationFile::begin(&missing);
        assert_eq!(file.current(), 0);
        file.record(1);
        assert_eq!(file.current(), 1, "the member goes on with the renewed identity");
        assert!(!missing.exists());
    }
}
