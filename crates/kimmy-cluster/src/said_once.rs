//! A warning said once per interval per subject, from a table that cannot grow.

use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// The subjects remembered at once. A flood of different subjects can only push
/// the oldest out, which says one of them again earlier than its interval.
const REMEMBERED: usize = 64;

/// When each subject was last said, by a hash of it: a subject is a line's
/// identifying text (a refused row's name), however long.
pub(crate) struct SaidOnce {
    said: Mutex<BTreeMap<u64, Instant>>,
}

impl SaidOnce {
    pub(crate) const fn new() -> Self {
        Self { said: Mutex::new(BTreeMap::new()) }
    }

    /// Whether `subject` is due to be said at `now`: it was not said within
    /// `every`. Answering yes counts it as said.
    pub(crate) fn due(&self, subject: &str, now: Instant, every: Duration) -> bool {
        let mut hasher = DefaultHasher::new();
        subject.hash(&mut hasher);
        let key = hasher.finish();
        let mut said = self.said.lock();
        if said.get(&key).is_some_and(|at| now.saturating_duration_since(*at) < every) {
            return false;
        }
        if said.len() >= REMEMBERED && !said.contains_key(&key) {
            said.retain(|_, at| now.saturating_duration_since(*at) < every);
            if said.len() >= REMEMBERED
                && let Some(oldest) = said.iter().min_by_key(|(_, at)| **at).map(|(key, _)| *key)
            {
                said.remove(&oldest);
            }
        }
        said.insert(key, now);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVERY: Duration = Duration::from_secs(300);

    /// A subject is due once, then not again until its interval has passed; another
    /// subject is due at once.
    #[test]
    fn a_subject_is_said_once_per_interval() {
        let said = SaidOnce::new();
        let t0 = Instant::now();
        assert!(said.due("document a", t0, EVERY));
        assert!(!said.due("document a", t0 + Duration::from_secs(299), EVERY));
        assert!(said.due("document b", t0 + Duration::from_secs(1), EVERY));
        assert!(said.due("document a", t0 + EVERY, EVERY));
        assert!(!said.due("document a", t0 + EVERY + Duration::from_secs(1), EVERY));
    }

    /// The table holds at most [`REMEMBERED`] subjects, whatever is said, and
    /// forgets the oldest first.
    #[test]
    fn the_table_stays_bounded_and_forgets_the_oldest() {
        let said = SaidOnce::new();
        let t0 = Instant::now();
        for i in 0..1_000u64 {
            assert!(said.due(&format!("document {i}"), t0 + Duration::from_millis(i), EVERY));
            assert!(said.said.lock().len() <= REMEMBERED);
        }
        assert_eq!(said.said.lock().len(), REMEMBERED);
        let at = t0 + Duration::from_secs(1);
        assert!(!said.due("document 999", at, EVERY), "the newest is remembered");
        assert!(said.due("document 0", at, EVERY), "the oldest was pushed out");
    }
}
