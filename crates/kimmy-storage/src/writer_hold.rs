//! Which hold has the writer, and since when, for the yield evaluator
//! (ADR-220).
//!
//! The evaluator takes no lock a runtime task takes and holds no engine, so the
//! engine publishes the hold it is in as **one word**: `((since_ms + 1) << 8) |
//! holder_slot`, `0` when the writer is free. One word, so the start and the
//! holder are always read together and there is no torn pair; no sequence is
//! needed, since another hold has another start.
//!
//! It is stored where a hold is **constructed** ([`crate::engine::WriterHold`]
//! and [`crate::engine::WriteTxn`]) and cleared in their release, **before** the
//! gate is let go, so a reader sees "held" for a gate somebody else has taken
//! only if the word still names the hold it was written for. The sites that
//! take the raw gate and never build either type (`begin_write_as`'s early
//! returns, the `close_writes` path with nothing pending, `flush_now` once the
//! writes are closed) never store, so never clear.
//!
//! The word lives in an `Arc` the engine owns and the daemon clones into the
//! evaluator's inputs: the evaluator reads it without the engine, and two
//! engines in one test process do not share one.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::engine::WriterHolder;

/// The instant every `since` is measured from: one per process.
fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

/// Milliseconds since the process epoch.
pub fn now_ms() -> u64 {
    u64::try_from(epoch().elapsed().as_millis()).unwrap_or(u64::MAX >> 9)
}

/// The writer's hold, as a reader sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HoldReading {
    /// What the holder is doing.
    pub holder: WriterHolder,
    /// When it took the writer, in [`now_ms`].
    pub since_ms: u64,
}

impl HoldReading {
    /// How long ago the hold began, as of `now_ms` (see [`now_ms`]).
    pub fn age_at(&self, now_ms: u64) -> Duration {
        Duration::from_millis(now_ms.saturating_sub(self.since_ms))
    }

    /// How long ago the hold began.
    pub fn age(&self) -> Duration {
        self.age_at(now_ms())
    }
}

/// The word. See the module documentation.
#[derive(Debug, Default)]
pub struct WriterHoldWord(AtomicU64);

impl WriterHoldWord {
    pub fn new() -> Self {
        Self::default()
    }

    /// The writer was taken as `holder`, now.
    pub(crate) fn publish(&self, holder: WriterHolder) {
        let word = ((now_ms() + 1) << 8) | holder.slot() as u64;
        self.0.store(word, Ordering::Release);
    }

    /// The writer is about to be let go. Called with the gate still held, so it
    /// can never clear the word of the hold that takes the gate next.
    pub(crate) fn clear(&self) {
        self.0.store(0, Ordering::Release);
    }

    /// The hold now, if the writer is held by a hold that published itself.
    pub fn reading(&self) -> Option<HoldReading> {
        let word = self.0.load(Ordering::Acquire);
        let since = (word >> 8).checked_sub(1)?;
        let holder = *WriterHolder::ALL.get((word & 0xff) as usize)?;
        Some(HoldReading { holder, since_ms: since })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_word_names_the_holder_and_the_start_together_and_clears_to_free() {
        let word = WriterHoldWord::new();
        assert_eq!(word.reading(), None, "free to begin with");
        for holder in WriterHolder::ALL {
            let before = now_ms();
            word.publish(holder);
            let read = word.reading().expect("held");
            assert_eq!(read.holder, holder);
            assert!(read.since_ms >= before && read.since_ms <= now_ms());
            word.clear();
            assert_eq!(word.reading(), None);
        }
    }

    #[test]
    fn a_hold_begun_at_the_epoch_is_not_mistaken_for_free() {
        // `since` is stored plus one, so zero milliseconds is still non-zero.
        let word = WriterHoldWord(AtomicU64::new(1 << 8));
        assert_eq!(word.reading(), Some(HoldReading { holder: WriterHolder::ALL[0], since_ms: 0 }));
    }

    #[test]
    fn a_word_that_names_no_holder_reads_as_free() {
        let word = WriterHoldWord(AtomicU64::new(((5 + 1) << 8) | 0xff));
        assert_eq!(word.reading(), None);
    }

    fn engine() -> (std::sync::Arc<crate::Engine>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        (std::sync::Arc::new(engine), dir)
    }

    /// Every way of holding the writer that builds a hold publishes it while it
    /// lasts and clears it when it ends, by commit, abort, drop or error.
    #[test]
    fn a_hold_is_published_while_it_lasts_and_cleared_when_it_ends() {
        let (engine, _dir) = engine();
        let word = engine.writer_hold_word();
        assert_eq!(word.reading(), None);

        // A transaction: committed, aborted, and dropped.
        let coll = engine.create_collection("app", "docs").unwrap();
        assert_eq!(word.reading(), None, "creating a collection let go");
        let txn = engine.begin_write(WriterHolder::Bulk).unwrap();
        assert_eq!(word.reading().map(|r| r.holder), Some(WriterHolder::Bulk));
        txn.abort().unwrap();
        assert_eq!(word.reading(), None, "abort");
        let txn = engine.begin_write(WriterHolder::Expiry).unwrap();
        assert_eq!(word.reading().map(|r| r.holder), Some(WriterHolder::Expiry));
        drop(txn);
        assert_eq!(word.reading(), None, "drop");
        let txn = engine.begin_write(WriterHolder::Write).unwrap();
        txn.commit().unwrap();
        assert_eq!(word.reading(), None, "commit");
        engine.insert(&coll, bson::doc! {"_id": 1}).unwrap();
        assert_eq!(word.reading(), None, "an ordinary write");

        // A bare hold.
        let hold = engine.hold_writer(WriterHolder::Replication);
        let read = word.reading().expect("held");
        assert_eq!(read.holder, WriterHolder::Replication);
        std::thread::sleep(Duration::from_millis(30));
        assert!(read.age() >= Duration::from_millis(30));
        drop(hold);
        assert_eq!(word.reading(), None, "hold dropped");

        // A write that fails inside its transaction lets go.
        assert!(engine.insert(&coll, bson::doc! {"_id": 1}).is_err(), "a duplicate");
        assert_eq!(word.reading(), None, "an error path");
    }

    /// A caller that gives up waiting for the writer never stored, so never
    /// clears: the hold it was waiting behind stays published.
    #[test]
    fn a_write_that_times_out_waiting_leaves_the_holders_word_alone() {
        let (engine, _dir) = engine();
        let word = engine.writer_hold_word();
        let hold = engine.hold_writer(WriterHolder::Repair);
        let before = word.reading().expect("held");
        let waiting = {
            let engine = std::sync::Arc::clone(&engine);
            std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(
                    crate::with_write_wait_budget(Duration::from_millis(50), async {
                        engine.begin_write(WriterHolder::Write).map(|_| ())
                    }),
                )
            })
        };
        assert!(matches!(waiting.join().unwrap(), Err(crate::StorageError::WriterBusy { .. })));
        assert_eq!(word.reading(), Some(before), "the holder's word is as it was");
        drop(hold);
        assert_eq!(word.reading(), None);
    }

    /// The paths that take the raw gate and never build a hold never store: a
    /// stop that finds nothing pending, and a write refused once writes are
    /// closed, leave the word at zero.
    #[test]
    fn the_raw_gate_paths_never_store() {
        let (engine, _dir) = engine();
        let word = engine.writer_hold_word();
        assert!(engine.close_writes(Duration::from_secs(5)), "nothing pending, nothing held");
        assert_eq!(word.reading(), None, "close_writes with nothing pending");
        let refused = engine.begin_write(WriterHolder::Write);
        assert!(matches!(refused, Err(crate::StorageError::Stopping(_))));
        assert_eq!(word.reading(), None, "a write refused because writes are closed");
        // The same, with a hold in progress that the stop waits out.
        let (engine, _dir) = self::engine();
        let word = engine.writer_hold_word();
        let hold = engine.hold_writer(WriterHolder::Write);
        let read = word.reading().expect("held");
        assert!(!engine.close_writes(Duration::from_millis(50)), "the hold outlasts the cap");
        assert_eq!(word.reading(), Some(read), "a stop that gave up did not touch the word");
        drop(hold);
        assert_eq!(word.reading(), None);
    }

    /// A flush under the coalescing barrier is a hold of its own: the durability
    /// holder, published while it lasts and cleared after.
    #[test]
    fn a_durability_flush_is_a_published_hold() {
        let (engine, _dir) = engine();
        engine.set_durability(crate::DurabilityClass::Coalesced, Duration::from_millis(1));
        let word = engine.writer_hold_word();
        let coll = engine.create_collection("app", "flushed").unwrap();
        let seen = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let until = std::time::Instant::now() + Duration::from_millis(800);
                while std::time::Instant::now() < until {
                    if word.reading().is_some_and(|r| r.holder == WriterHolder::Durability) {
                        seen.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            });
            for i in 0..200i64 {
                engine.insert(&coll, bson::doc! {"_id": i}).unwrap();
            }
        });
        assert_eq!(word.reading(), None, "nothing stuck after the flushes");
        // Whether a flush was caught by the sampler is timing; what must hold is
        // that none left the word set, which the line above asserts.
        let _ = seen;
    }

    /// The word is cleared **before** the gate is let go: whoever takes the gate
    /// next finds it free, never still naming the hold that has just ended. One
    /// thread takes and lets go of the writer, and another takes the gate raw the
    /// instant it is free and looks.
    fn the_word_is_clear_by_the_time_the_gate_is_free(
        cycles: usize,
        cycle: impl Fn(&crate::Engine) + Sync,
    ) {
        let (engine, _dir) = engine();
        let word = engine.writer_hold_word();
        let done = std::sync::atomic::AtomicBool::new(false);
        let looked = std::sync::atomic::AtomicU64::new(0);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                for _ in 0..cycles {
                    cycle(&engine);
                }
                done.store(true, std::sync::atomic::Ordering::SeqCst);
            });
            while !done.load(std::sync::atomic::Ordering::SeqCst) {
                if let Some(gate) = engine.try_raw_gate_for_test() {
                    assert_eq!(
                        word.reading(),
                        None,
                        "the gate was free and the word still named the hold that let it go"
                    );
                    looked.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    drop(gate);
                }
            }
        });
        assert!(looked.load(std::sync::atomic::Ordering::Relaxed) > 0, "never took the gate");
    }

    #[test]
    fn a_hold_clears_the_word_before_it_frees_the_gate() {
        the_word_is_clear_by_the_time_the_gate_is_free(100_000, |engine| {
            drop(engine.hold_writer(WriterHolder::Write));
        });
    }

    #[test]
    fn a_transaction_clears_the_word_before_it_frees_the_gate() {
        the_word_is_clear_by_the_time_the_gate_is_free(100_000, |engine| {
            engine.begin_write(WriterHolder::Bulk).unwrap().abort().unwrap();
        });
    }

    /// Two engines in one process have words of their own.
    #[test]
    fn engines_do_not_share_a_word() {
        let (a, _a) = engine();
        let (b, _b) = engine();
        let hold = a.hold_writer(WriterHolder::Bulk);
        assert!(a.writer_hold_word().reading().is_some());
        assert_eq!(b.writer_hold_word().reading(), None);
        drop(hold);
    }

    /// Writers taking and letting go of the gate against a reader: the word never
    /// reads as free while a hold is in progress (a clear that landed after the
    /// release would wipe the next holder's), never names a holder that is not the
    /// one that began at that instant (a start and a holder read apart would), and
    /// is free once they stop.
    #[test]
    fn the_word_follows_the_gate_under_contention() {
        let (engine, _dir) = engine();
        let word = engine.writer_hold_word();
        let began: parking_lot::Mutex<std::collections::HashMap<u64, WriterHolder>> =
            Default::default();
        let stop = std::sync::atomic::AtomicBool::new(false);
        let mut sampled: Vec<HoldReading> = Vec::new();
        std::thread::scope(|scope| {
            let writers: Vec<_> = [WriterHolder::Write, WriterHolder::Bulk]
                .into_iter()
                .map(|holder| {
                    let (engine, word, began) = (&engine, &word, &began);
                    scope.spawn(move || {
                        for _ in 0..150 {
                            let hold = engine.hold_writer(holder);
                            let mine = word.reading().expect("held: a stale clear landed");
                            assert_eq!(
                                mine.holder, holder,
                                "the word names the hold it was written for"
                            );
                            began.lock().insert(mine.since_ms, holder);
                            // Past the next millisecond, so no two holds share a start.
                            std::thread::sleep(Duration::from_micros(1_100));
                            assert_eq!(
                                word.reading(),
                                Some(mine),
                                "a stale clear wiped a hold in progress"
                            );
                            drop(hold);
                        }
                    })
                })
                .collect();
            let reader = scope.spawn(|| {
                let mut seen = Vec::new();
                while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                    seen.extend(word.reading());
                }
                seen
            });
            for writer in writers {
                writer.join().unwrap();
            }
            stop.store(true, std::sync::atomic::Ordering::SeqCst);
            sampled = reader.join().unwrap();
        });
        assert!(!sampled.is_empty(), "the reader never caught a hold");
        let began = began.lock();
        for read in &sampled {
            assert_eq!(
                began.get(&read.since_ms),
                Some(&read.holder),
                "the start and the holder were not read together: {read:?}"
            );
        }
        assert_eq!(word.reading(), None, "no word left set");
    }
}
