//! The open's walk: the live counts and the cut digests rebuilt in one pass
//! over `DOCS`, in chunks that commit and honour a stop (B3 design 2.1 and 5).
//!
//! **One walk, two marks.** If either mark is missing or behind, or the start
//! decided the previous run cannot be shown to have left them current, one walk
//! rebuilds both tables.
//!
//! **The marks go before the first rewritten row.** The first commit of a walk
//! deletes both marks in the transaction that writes its first rows and the
//! cursor, so no commit ever holds a rewritten row beside a mark. A stopped,
//! crashed or failed walk therefore leaves the store saying "rebuild
//! required", whatever any exit marker says. When the start decided the
//! rebuild is forced, the open deleted the marks even earlier, in its first
//! write ([`force_marks_deleted`]).
//!
//! **Chunks.** The walk is ordered by `(collection, key)`. It commits when a
//! chunk reaches [`CHUNK_BYTES`] or [`CHUNK_ROWS`], each commit holding the
//! partial counts and digests of the collections it has passed (a collection
//! split across chunks keeps a partial row) and a cursor. Bytes are the unit
//! that matters: a `DOCS` value carries its body, and on the largest store
//! measured the heaviest 50,000-row chunk was 128.8 MiB against 17 MiB on
//! average, so a row cap alone is seven times off. At the cold rate measured
//! on a spinning disk (about 10 MB/s) a 64 MiB chunk is some seconds, so what
//! keeps a stop inside the stop budget is the check every [`STOP_EVERY`] rows
//! inside the chunk, not the chunk size.
//!
//! **Stop.** At the check, the chunk so far is committed with the cursor
//! marked `stopped_cleanly`, and the open ends with the stop. A crash leaves
//! the cursor as the last commit made it, not stopped cleanly.
//!
//! **Resume.** Only if both marks are absent (so the first commit of a walk
//! ran), the cursor's build, schema and digest format are this build's, and it
//! was left by a cooperative stop. Both marks absent also shows that no older
//! build has served since the walk began: an older build that completed an
//! open restored the counts' mark (`live_count`'s rebuild), and one that did
//! not changed no document record at open (B3 design 8.9). Anything else
//! discards the cursor and walks from the start, so a crash mid-walk costs a
//! restart, as the design accepts.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::time::Instant;

use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, WriteTransaction};
use tracing::{info, warn};

use crate::codec;
use crate::error::{Result, StopReason, StorageError};
use crate::live_count::{THROUGH, decode_arrival, mark_of};
use crate::live_digest::{self, DigestRow};
use crate::tables;

/// A chunk commits when it has read this many bytes of key and value.
pub(crate) const CHUNK_BYTES: u64 = 64 * 1024 * 1024;

/// A chunk commits when it has read this many rows: the secondary guard.
pub(crate) const CHUNK_ROWS: u64 = 50_000;

/// The stop is looked for every this many rows inside a chunk.
pub(crate) const STOP_EVERY: u64 = 256;

/// What a walk did, for the line it logs.
pub(crate) struct Rebuilt {
    pub collections: usize,
    pub records: u64,
    pub bytes: u64,
    pub chunks: u32,
    pub previous_mark: Option<u64>,
    pub arrival: u64,
    pub resumed: bool,
    pub why: &'static str,
}

/// The open walk's cursor: where it stands, and what it was taken by.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Cursor {
    pub build: String,
    pub schema: u8,
    pub format: u8,
    pub stopped_cleanly: bool,
    /// The last record processed: `(collection, key)`. `None` before any.
    pub after: Option<(u64, Vec<u8>)>,
    pub rows: u64,
    pub bytes: u64,
}

impl Cursor {
    const VERSION: u8 = 1;

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = vec![Self::VERSION];
        out.extend_from_slice(&(self.build.len() as u16).to_be_bytes());
        out.extend_from_slice(self.build.as_bytes());
        out.push(self.schema);
        out.push(self.format);
        out.push(u8::from(self.stopped_cleanly));
        match &self.after {
            Some((collection, key)) => {
                out.push(1);
                out.extend_from_slice(&collection.to_be_bytes());
                out.extend_from_slice(&(key.len() as u32).to_be_bytes());
                out.extend_from_slice(key);
            }
            None => out.push(0),
        }
        out.extend_from_slice(&self.rows.to_be_bytes());
        out.extend_from_slice(&self.bytes.to_be_bytes());
        out
    }

    /// `None` for anything this build cannot read: a cursor nobody can trust is
    /// no cursor.
    pub(crate) fn decode(mut input: &[u8]) -> Option<Self> {
        fn take<'a>(input: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
            if input.len() < n {
                return None;
            }
            let (head, tail) = input.split_at(n);
            *input = tail;
            Some(head)
        }
        if take(&mut input, 1)?[0] != Self::VERSION {
            return None;
        }
        let build_len = u16::from_be_bytes(take(&mut input, 2)?.try_into().ok()?) as usize;
        let build = String::from_utf8(take(&mut input, build_len)?.to_vec()).ok()?;
        let schema = take(&mut input, 1)?[0];
        let format = take(&mut input, 1)?[0];
        let stopped_cleanly = take(&mut input, 1)?[0] == 1;
        let after = match take(&mut input, 1)?[0] {
            0 => None,
            1 => {
                let collection = u64::from_be_bytes(take(&mut input, 8)?.try_into().ok()?);
                let len = u32::from_be_bytes(take(&mut input, 4)?.try_into().ok()?) as usize;
                Some((collection, take(&mut input, len)?.to_vec()))
            }
            _ => return None,
        };
        let rows = u64::from_be_bytes(take(&mut input, 8)?.try_into().ok()?);
        let bytes = u64::from_be_bytes(take(&mut input, 8)?.try_into().ok()?);
        input.is_empty().then_some(Self {
            build,
            schema,
            format,
            stopped_cleanly,
            after,
            rows,
            bytes,
        })
    }

    /// Whether this build may resume what the cursor describes.
    fn is_ours(&self, schema: u8) -> bool {
        self.build == kimmy_core::build::VERSION
            && self.schema == schema
            && self.format == live_digest::FORMAT
    }
}

/// The very first write of a start that decided the rebuild is forced
/// (B3 design 2.1, layer 2): delete both marks, and the cursor if any mark was
/// present. Run in the open's ensure-tables transaction, before the migration
/// and before the walk, so from then on the store says "rebuild required"
/// whatever any exit marker says.
///
/// A cursor is kept when both marks were already absent: that is a walk this
/// build stopped, which the resume rule decides about on its own evidence.
pub(crate) fn force_marks_deleted(txn: &WriteTransaction) -> Result<()> {
    let mut counts = txn.open_table(tables::LIVE_COUNTS_THROUGH)?;
    let mut digests = txn.open_table(tables::LIVE_DIGESTS_THROUGH)?;
    let any_mark =
        counts.remove(THROUGH)?.is_some() | digests.remove(live_digest::THROUGH)?.is_some();
    if any_mark {
        digests.remove(live_digest::WALK)?;
    }
    Ok(())
}

/// Rebuild the counts and the digests if they may be stale, in chunks.
///
/// `forced` names why the start decided the rebuild must happen whatever the
/// marks say, for the log; the marks themselves were already deleted by
/// [`force_marks_deleted`]. `Ok(None)` when nothing was stale.
pub(crate) fn run(db: &Database, forced: Option<&'static str>) -> Result<Option<Rebuilt>> {
    let started = Instant::now();
    #[cfg(test)]
    if test_hooks::fails_before_the_walk() {
        return Err(StorageError::Corrupt("a failure injected before the walk".into()));
    }
    let schema = crate::migrate::stored_version(db)?.unwrap_or(0);

    let (mark, previous_mark, counts_current, digests_current, marks_absent, cursor, total) = {
        let txn = db.begin_read()?;
        let mark =
            mark_of(&txn.open_table(tables::OPLOG_ARRIVAL)?, &txn.open_table(tables::OPLOG)?)?;
        let counts = txn.open_table(tables::LIVE_COUNTS_THROUGH)?;
        let counts_mark = counts.get(THROUGH)?.map(|m| m.value().to_vec());
        let digests = txn.open_table(tables::LIVE_DIGESTS_THROUGH)?;
        let digest_mark = digests.get(live_digest::THROUGH)?.map(|m| m.value().to_vec());
        let cursor = digests.get(live_digest::WALK)?.map(|c| Cursor::decode(c.value()));
        (
            mark.clone(),
            counts_mark.as_deref().map(decode_arrival),
            counts_mark.as_deref() == Some(mark.as_slice()),
            live_digest::mark_matches(&digests, &mark)?,
            counts_mark.is_none() && digest_mark.is_none(),
            cursor,
            txn.open_table(tables::DOCS)?.len()?,
        )
    };

    if counts_current && digests_current && forced.is_none() {
        // Nothing stale. A cursor left beside two current marks is a leftover
        // (the completing commit deletes it), and is removed so that it can
        // never be resumed over rows it was not taken for.
        if cursor.is_some() {
            let txn = db.begin_write()?;
            txn.open_table(tables::LIVE_DIGESTS_THROUGH)?.remove(live_digest::WALK)?;
            txn.commit()?;
        }
        return Ok(None);
    }

    // Resume only on the store's own evidence (module docs).
    let resumed_from = match cursor {
        Some(Some(cursor)) if marks_absent && cursor.stopped_cleanly && cursor.is_ours(schema) => {
            Some(cursor)
        }
        _ => None,
    };
    let resumed = resumed_from.is_some();
    let why = match (resumed, forced) {
        (true, _) => "resumed a walk this build stopped",
        (false, Some(why)) => why,
        (false, None) => "a mark was missing or behind",
    };

    crate::open_progress::set_open_phase_of(crate::open_progress::OpenPhase::Counting, total);
    let mut cursor = match resumed_from {
        Some(cursor) => Cursor { stopped_cleanly: false, ..cursor },
        None => Cursor {
            build: kimmy_core::build::VERSION.to_string(),
            schema,
            format: live_digest::FORMAT,
            stopped_cleanly: false,
            after: None,
            rows: 0,
            bytes: 0,
        },
    };
    crate::open_progress::set_open_done(cursor.rows);
    let mut chunks = 0u32;
    let mut touched = std::collections::BTreeSet::new();

    loop {
        let txn = db.begin_write()?;
        if chunks == 0 && !resumed {
            // The first commit of a fresh walk: both marks go, and the
            // derived tables start empty, in the transaction that writes the
            // first rows and the cursor.
            txn.open_table(tables::LIVE_COUNTS_THROUGH)?.remove(THROUGH)?;
            txn.open_table(tables::LIVE_DIGESTS_THROUGH)?.remove(live_digest::THROUGH)?;
            txn.open_table(tables::LIVE_COUNTS)?.retain(|_, _| false)?;
            txn.open_table(tables::LIVE_DIGESTS)?.retain(|_, _| false)?;
        }

        let mut counts: BTreeMap<u64, u64> = BTreeMap::new();
        let mut digests: BTreeMap<u64, DigestRow> = BTreeMap::new();
        let (mut chunk_rows, mut chunk_bytes) = (0u64, 0u64);
        let mut last = cursor.after.clone();
        let mut finished = true;
        let mut stopped = false;
        {
            let docs = txn.open_table(tables::DOCS)?;
            let range = match &cursor.after {
                Some((collection, key)) => docs.range::<(u64, &[u8])>((
                    Bound::Excluded((*collection, key.as_slice())),
                    Bound::Unbounded,
                ))?,
                None => docs.range::<(u64, &[u8])>(..)?,
            };
            for row in range {
                if chunk_rows % stop_every() == 0 && stop_requested(cursor.rows + chunk_rows) {
                    stopped = true;
                    finished = false;
                    break;
                }
                let (key, value) = row?;
                let (collection, doc_key) = key.value();
                let value = value.value();
                chunk_rows += 1;
                chunk_bytes += (doc_key.len() + value.len()) as u64;
                match codec::doc_record_header(value) {
                    Ok((stamp, live)) => {
                        if live {
                            *counts.entry(collection).or_default() += 1;
                        }
                        live_digest::fold_walked(&mut digests, collection, doc_key, &stamp, live);
                    }
                    // Not counted, as no reader can return it either; refusing
                    // to open over one unreadable record would be worse than a
                    // count one short.
                    Err(e) => warn!(error = %e, "unreadable document record not counted"),
                }
                last = Some((collection, doc_key.to_vec()));
                if chunk_bytes >= chunk_bytes_limit() || chunk_rows >= chunk_rows_limit() {
                    finished = false;
                    break;
                }
            }
        }

        // The collections this chunk passed, added to what earlier chunks
        // left: a collection split across chunks keeps a partial row.
        if !counts.is_empty() {
            let mut table = txn.open_table(tables::LIVE_COUNTS)?;
            for (id, n) in &counts {
                let current = table.get(*id)?.map_or(0, |c| c.value());
                table.insert(*id, current + n)?;
            }
        }
        if !digests.is_empty() {
            let mut table = txn.open_table(tables::LIVE_DIGESTS)?;
            for (id, delta) in &digests {
                let mut row = match table.get(*id)? {
                    Some(raw) => DigestRow::decode(raw.value())?,
                    None => DigestRow::default(),
                };
                row.digest ^= delta.digest;
                for (node, hlc) in &delta.maxw {
                    row.raise_maxw(&kimmy_core::Stamp::new(*hlc, *node));
                }
                for (node, hlc) in &delta.bare_max {
                    row.raise_bare(&kimmy_core::Stamp::new(*hlc, *node));
                }
                table.insert(*id, row.encode().as_slice())?;
            }
        }
        touched.extend(counts.keys().chain(digests.keys()).copied());

        cursor.after = last;
        cursor.rows += chunk_rows;
        cursor.bytes += chunk_bytes;
        let mut through = txn.open_table(tables::LIVE_DIGESTS_THROUGH)?;
        if finished {
            // The completing commit: the cursor goes, and both marks come
            // back, for the state the walk read (the oplog and the arrival
            // index did not move during the open).
            through.remove(live_digest::WALK)?;
            drop(through);
            crate::live_count::mark_through(&txn, &mark)?;
            live_digest::mark_through(&txn, &mark)?;
        } else {
            cursor.stopped_cleanly = stopped;
            through.insert(live_digest::WALK, cursor.encode().as_slice())?;
            drop(through);
        }
        txn.commit()?;
        chunks += 1;
        crate::open_progress::set_open_done(cursor.rows);

        if stopped {
            return Err(StorageError::Stopping(StopReason::Shutdown));
        }
        if finished {
            break;
        }
        #[cfg(test)]
        if test_hooks::fail_after_chunk(chunks) {
            return Err(StorageError::Corrupt("a failure injected after a chunk committed".into()));
        }
    }

    let rebuilt = Rebuilt {
        collections: touched.len(),
        records: cursor.rows,
        bytes: cursor.bytes,
        chunks,
        previous_mark,
        arrival: decode_arrival(&mark),
        resumed,
        why,
    };
    #[cfg(test)]
    test_hooks::record(&rebuilt);
    info!(
        collections = rebuilt.collections,
        records = rebuilt.records,
        bytes = rebuilt.bytes,
        chunks = rebuilt.chunks,
        elapsed_ms = started.elapsed().as_millis() as u64,
        previous_mark = ?rebuilt.previous_mark,
        arrival = rebuilt.arrival,
        resumed = rebuilt.resumed,
        why = rebuilt.why,
        "rebuilt the live document counts before serving"
    );
    Ok(Some(rebuilt))
}

fn stop_requested(rows_so_far: u64) -> bool {
    #[cfg(test)]
    if test_hooks::stop_after(rows_so_far) {
        return true;
    }
    let _ = rows_so_far;
    crate::open_progress::open_stop_requested()
}

/// How often the stop is looked for. Every row under test, so that a stop can
/// be placed at an exact row; [`STOP_EVERY`] otherwise.
fn stop_every() -> u64 {
    #[cfg(test)]
    return 1;
    #[cfg(not(test))]
    STOP_EVERY
}

fn chunk_bytes_limit() -> u64 {
    #[cfg(test)]
    if let Some(bytes) = test_hooks::CHUNK_BYTES.with(std::cell::Cell::get) {
        return bytes;
    }
    CHUNK_BYTES
}

fn chunk_rows_limit() -> u64 {
    #[cfg(test)]
    if let Some(rows) = test_hooks::CHUNK_ROWS.with(std::cell::Cell::get) {
        return rows;
    }
    CHUNK_ROWS
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::cell::Cell;

    thread_local! {
        /// Commit a chunk at this many rows, on this thread.
        pub(crate) static CHUNK_ROWS: Cell<Option<u64>> = const { Cell::new(None) };
        pub(crate) static CHUNK_BYTES: Cell<Option<u64>> = const { Cell::new(None) };
        /// Report a stop once this many rows have been processed in all, on
        /// this thread.
        static STOP_AFTER_ROWS: Cell<Option<u64>> = const { Cell::new(None) };
        /// Fail, as a crash would leave things, after this many chunks have
        /// committed, on this thread.
        static FAIL_AFTER_CHUNKS: Cell<Option<u32>> = const { Cell::new(None) };
        /// Fail the open just before the walk starts: after everything the
        /// open does ahead of it, and so after a forced start's first write.
        static FAIL_BEFORE_THE_WALK: Cell<bool> = const { Cell::new(false) };
        /// What the last walk on this thread did: `(resumed, chunks, records)`.
        static LAST: Cell<Option<(bool, u32, u64)>> = const { Cell::new(None) };
    }

    pub(crate) fn fail_before_the_walk() {
        FAIL_BEFORE_THE_WALK.with(|c| c.set(true));
    }

    pub(crate) fn fails_before_the_walk() -> bool {
        FAIL_BEFORE_THE_WALK.with(|c| c.replace(false))
    }

    pub(crate) fn record(rebuilt: &super::Rebuilt) {
        LAST.with(|c| c.set(Some((rebuilt.resumed, rebuilt.chunks, rebuilt.records))));
    }

    /// `(resumed, chunks, records)` of the last walk on this thread, taken.
    pub(crate) fn last() -> Option<(bool, u32, u64)> {
        LAST.with(Cell::take)
    }

    /// Run `f` with the walk's chunks this small on this thread.
    pub(crate) fn chunked_by<T>(rows: u64, f: impl FnOnce() -> T) -> T {
        CHUNK_ROWS.with(|c| c.set(Some(rows)));
        let out = f();
        CHUNK_ROWS.with(|c| c.set(None));
        out
    }

    pub(crate) fn stop_after_rows(rows: u64) {
        STOP_AFTER_ROWS.with(|c| c.set(Some(rows)));
    }

    pub(crate) fn clear_stop() {
        STOP_AFTER_ROWS.with(|c| c.set(None));
    }

    pub(crate) fn stop_after(rows_so_far: u64) -> bool {
        STOP_AFTER_ROWS.with(|c| c.get().is_some_and(|n| rows_so_far >= n))
    }

    pub(crate) fn fail_after_chunks(chunks: u32) {
        FAIL_AFTER_CHUNKS.with(|c| c.set(Some(chunks)));
    }

    pub(crate) fn fail_after_chunk(chunks: u32) -> bool {
        FAIL_AFTER_CHUNKS.with(|c| match c.get() {
            Some(n) if chunks >= n => {
                c.set(None);
                true
            }
            _ => false,
        })
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use kimmy_core::DocId;
    use redb::ReadableDatabase;

    use super::*;
    use crate::live_count::test_support::{assert_counts_exact, assert_digests_exact};
    use crate::{Engine, OpenOptions};

    const COLLECTIONS: [&str; 3] = ["alpha", "beta", "gamma"];

    /// A store with documents across three collections, some deleted, closed.
    fn a_store() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let engine = Engine::open(&path).unwrap();
        for (n, name) in COLLECTIONS.iter().enumerate() {
            let coll = engine.create_collection("app", name).unwrap();
            for i in 0..(7 + n as i64) {
                engine.insert(&coll, doc! { "_id": i, "n": i }).unwrap();
            }
            engine.delete(&coll, &DocId::Int64(1)).unwrap();
        }
        drop(engine);
        (dir, path)
    }

    /// What a start finds with no mark: neither, and the derived tables wrong.
    fn lose_the_marks(path: &std::path::Path) {
        let db = Database::open(path).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(tables::LIVE_COUNTS_THROUGH).unwrap().remove(THROUGH).unwrap();
        txn.open_table(tables::LIVE_DIGESTS_THROUGH).unwrap().remove(live_digest::THROUGH).unwrap();
        txn.commit().unwrap();
    }

    fn marks_present(path: &std::path::Path) -> (bool, bool) {
        let db = Database::open(path).unwrap();
        let txn = db.begin_read().unwrap();
        (
            txn.open_table(tables::LIVE_COUNTS_THROUGH).unwrap().get(THROUGH).unwrap().is_some(),
            txn.open_table(tables::LIVE_DIGESTS_THROUGH)
                .unwrap()
                .get(live_digest::THROUGH)
                .unwrap()
                .is_some(),
        )
    }

    fn cursor_of(path: &std::path::Path) -> Option<Cursor> {
        let db = Database::open(path).unwrap();
        let txn = db.begin_read().unwrap();
        let through = txn.open_table(tables::LIVE_DIGESTS_THROUGH).unwrap();
        through
            .get(live_digest::WALK)
            .unwrap()
            .map(|c| Cursor::decode(c.value()).expect("a cursor"))
    }

    fn stopped(result: Result<Engine>) -> bool {
        matches!(result, Err(StorageError::Stopping(_)))
    }

    #[test]
    fn a_chunked_walk_equals_a_fresh_one_and_the_completing_commit_deletes_the_cursor() {
        let (_dir, path) = a_store();
        lose_the_marks(&path);
        test_hooks::chunked_by(4, || {
            let engine = Engine::open(&path).unwrap();
            assert_counts_exact(&engine, "a walk in chunks of four");
            let (resumed, chunks, records) = test_hooks::last().expect("a walk ran");
            assert!(!resumed);
            assert!(chunks > 1, "{chunks} chunks");
            assert_eq!(records, 7 + 8 + 9, "a delete is a tombstone in place");
        });
        assert_eq!(marks_present(&path), (true, true));
        assert_eq!(cursor_of(&path), None, "the completing commit deletes the cursor");
    }

    #[test]
    fn a_cooperative_stop_resumes_and_the_resumed_walk_equals_a_fresh_one() {
        let (_dir, path) = a_store();
        lose_the_marks(&path);
        test_hooks::chunked_by(4, || {
            test_hooks::stop_after_rows(10);
            assert!(stopped(Engine::open(&path)), "the stop ends the open");
            test_hooks::clear_stop();
        });
        assert_eq!(marks_present(&path), (false, false), "a stopped walk leaves both marks out");
        let cursor = cursor_of(&path).expect("the stop left a cursor");
        assert!(cursor.stopped_cleanly);
        assert_eq!(cursor.rows, 10);
        test_hooks::chunked_by(4, || {
            let engine = Engine::open(&path).unwrap();
            let (resumed, _, records) = test_hooks::last().expect("a walk ran");
            assert!(resumed, "a cooperative stop resumes");
            assert_eq!(records, 7 + 8 + 9, "the whole store, counting what was read before");
            assert_counts_exact(&engine, "a resumed walk");
        });
        assert_eq!(cursor_of(&path), None);
    }

    #[test]
    fn a_crash_mid_walk_restarts_from_zero() {
        let (_dir, path) = a_store();
        lose_the_marks(&path);
        test_hooks::chunked_by(4, || {
            test_hooks::fail_after_chunks(2);
            assert!(Engine::open(&path).is_err());
        });
        assert_eq!(marks_present(&path), (false, false));
        let cursor = cursor_of(&path).expect("the commits made before the crash left a cursor");
        assert!(!cursor.stopped_cleanly, "a crash is not a stop");
        test_hooks::chunked_by(4, || {
            let engine = Engine::open(&path).unwrap();
            let (resumed, chunks, _) = test_hooks::last().unwrap();
            assert!(!resumed, "a walk that was not stopped cleanly is walked again from zero");
            assert!(chunks >= 2);
            assert_counts_exact(&engine, "a walk restarted after a crash");
        });
    }

    #[test]
    fn a_kill_at_every_chunk_commit_leaves_the_store_saying_rebuild_required() {
        let (_dir, path) = a_store();
        for after in 1..6 {
            lose_the_marks(&path);
            test_hooks::chunked_by(4, || {
                test_hooks::fail_after_chunks(after);
                assert!(Engine::open(&path).is_err(), "after chunk {after}");
            });
            assert_eq!(
                marks_present(&path),
                (false, false),
                "a mark stands beside rewritten rows after chunk {after}"
            );
            // Then it completes, whatever any marker says.
            let engine = Engine::open(&path).unwrap();
            assert_counts_exact(&engine, "the walk after the kill");
        }
    }

    /// The first commit of a walk that started on one stale mark deletes the
    /// other too: the first start of a build that keeps the digests finds the
    /// counts' mark matching (the last build kept it) and the digest mark
    /// missing, and rewrites the counts under it otherwise (B3 design 5, N2).
    #[test]
    fn a_walk_started_by_one_stale_mark_deletes_the_other_with_its_first_chunk() {
        for (which, remove) in [("the digest mark", "digest"), ("the counts mark", "counts")] {
            let (_dir, path) = a_store();
            {
                let db = Database::open(&path).unwrap();
                let txn = db.begin_write().unwrap();
                if remove == "digest" {
                    txn.open_table(tables::LIVE_DIGESTS_THROUGH)
                        .unwrap()
                        .remove(live_digest::THROUGH)
                        .unwrap();
                } else {
                    txn.open_table(tables::LIVE_COUNTS_THROUGH).unwrap().remove(THROUGH).unwrap();
                }
                txn.commit().unwrap();
            }
            let (counts, digests) = marks_present(&path);
            assert_eq!((counts, digests), (remove == "digest", remove == "counts"), "premise");
            test_hooks::chunked_by(4, || {
                test_hooks::fail_after_chunks(1);
                assert!(Engine::open(&path).is_err());
            });
            assert_eq!(
                marks_present(&path),
                (false, false),
                "with only {which} missing, a walk that stopped after its first chunk left a \
                 mark beside rewritten rows"
            );
        }
    }

    #[test]
    fn a_cursor_from_another_build_schema_or_format_is_discarded() {
        for tamper in ["build", "schema", "format"] {
            let (_dir, path) = a_store();
            lose_the_marks(&path);
            test_hooks::chunked_by(4, || {
                test_hooks::stop_after_rows(6);
                assert!(stopped(Engine::open(&path)));
                test_hooks::clear_stop();
            });
            {
                let mut cursor = cursor_of(&path).unwrap();
                match tamper {
                    "build" => cursor.build = "0.0.1".into(),
                    "schema" => cursor.schema = cursor.schema.wrapping_add(1),
                    _ => cursor.format = cursor.format.wrapping_add(1),
                }
                let db = Database::open(&path).unwrap();
                let txn = db.begin_write().unwrap();
                txn.open_table(tables::LIVE_DIGESTS_THROUGH)
                    .unwrap()
                    .insert(live_digest::WALK, cursor.encode().as_slice())
                    .unwrap();
                txn.commit().unwrap();
            }
            let engine = Engine::open(&path).unwrap();
            let (resumed, _, _) = test_hooks::last().unwrap();
            assert!(!resumed, "a cursor with another {tamper} was resumed");
            assert_counts_exact(&engine, "a walk that discarded the cursor");
        }
    }

    #[test]
    fn a_stopped_walk_then_an_older_build_that_completed_its_open_restarts_from_zero() {
        for forced in [true, false] {
            a_stopped_walk_then_an_older_build(forced);
        }
    }

    /// `forced`: the start decided the rebuild, so it deletes the stale cursor
    /// as its first write; not forced, the walk's own resume rule must refuse
    /// a cursor with a mark beside it.
    fn a_stopped_walk_then_an_older_build(forced: bool) {
        // Rollback-gate row 3, at the store: stop a forced walk mid-chunk, an
        // older build starts to ready (its open rebuilt the counts and wrote
        // their mark; it knows nothing of the digests), stops, and this build
        // starts again. Both marks are no longer absent, so the walk restarts.
        let (_dir, path) = a_store();
        lose_the_marks(&path);
        test_hooks::chunked_by(4, || {
            test_hooks::stop_after_rows(6);
            assert!(stopped(Engine::open_with(
                &path,
                &OpenOptions::new().force_rebuild("the previous run cannot be shown")
            )));
            test_hooks::clear_stop();
        });
        {
            let db = Database::open(&path).unwrap();
            let txn = db.begin_write().unwrap();
            let mark = {
                let arrival = txn.open_table(tables::OPLOG_ARRIVAL).unwrap();
                let oplog = txn.open_table(tables::OPLOG).unwrap();
                mark_of(&arrival, &oplog).unwrap()
            };
            // What 0.47.2's rebuild leaves: exact counts, and their mark.
            let mut counts: std::collections::BTreeMap<u64, u64> = Default::default();
            for row in txn.open_table(tables::DOCS).unwrap().iter().unwrap() {
                let (key, value) = row.unwrap();
                if codec::doc_record_is_live(value.value()).unwrap() {
                    *counts.entry(key.value().0).or_default() += 1;
                }
            }
            {
                let mut table = txn.open_table(tables::LIVE_COUNTS).unwrap();
                table.retain(|_, _| false).unwrap();
                for (id, n) in counts {
                    table.insert(id, n).unwrap();
                }
            }
            crate::live_count::mark_through(&txn, &mark).unwrap();
            txn.commit().unwrap();
        }
        assert_eq!(marks_present(&path), (true, false));
        test_hooks::chunked_by(4, || {
            let options = match forced {
                true => OpenOptions::new().force_rebuild("the previous run cannot be shown"),
                false => OpenOptions::new(),
            };
            let engine = Engine::open_with(&path, &options).unwrap();
            let (resumed, _, _) = test_hooks::last().unwrap();
            assert!(!resumed, "the cursor was resumed over rows an older build may have changed");
            assert_counts_exact(&engine, "a walk restarted after an older build");
        });
    }

    #[test]
    fn a_forced_start_repairs_what_the_marks_vouch_for_and_a_failure_before_the_walk_still_rebuilds()
     {
        // Marks present and matching, a digest row forged behind them.
        let (_dir, path) = a_store();
        {
            let engine = Engine::open(&path).unwrap();
            let db = engine.db();
            let txn = db.begin_write().unwrap();
            let mut rows = txn.open_table(tables::LIVE_DIGESTS).unwrap();
            let (id, mut row) = {
                let first = rows.iter().unwrap().next().unwrap().unwrap();
                (first.0.value(), DigestRow::decode(first.1.value()).unwrap())
            };
            row.digest ^= 0xfeed;
            rows.insert(id, row.encode().as_slice()).unwrap();
            drop(rows);
            txn.commit().unwrap();
        }
        // An unforced start trusts the marks; the forged row stands.
        {
            let _ = test_hooks::last();
            let engine = Engine::open(&path).unwrap();
            assert!(test_hooks::last().is_none(), "no walk");
            drop(engine);
        }
        // The forced start's first write deletes the marks, and the open
        // fails before the walk: the store alone must still force a rebuild.
        test_hooks::fail_before_the_walk();
        assert!(
            Engine::open_with(&path, &OpenOptions::new().force_rebuild("a test")).is_err(),
            "the injected failure ends the open"
        );
        assert_eq!(
            marks_present(&path),
            (false, false),
            "a forced start's first write deletes both marks"
        );
        let engine = Engine::open(&path).unwrap();
        assert!(
            test_hooks::last().is_some(),
            "an unforced start walks, because the marks are gone"
        );
        assert_digests_exact(&engine, "the walk after a forced start failed before its walk");
    }

    #[test]
    fn a_forced_start_keeps_a_cursor_its_own_build_stopped_and_drops_one_beside_a_mark() {
        let (_dir, path) = a_store();
        lose_the_marks(&path);
        test_hooks::chunked_by(4, || {
            test_hooks::stop_after_rows(6);
            assert!(stopped(Engine::open(&path)));
            test_hooks::clear_stop();
        });
        // Forced, both marks absent, a usable cursor: resumed, not restarted.
        test_hooks::chunked_by(4, || {
            let engine =
                Engine::open_with(&path, &OpenOptions::new().force_rebuild("a test")).unwrap();
            let (resumed, _, records) = test_hooks::last().expect("a walk ran");
            assert!(resumed, "the forced start discarded a cursor its own build left");
            assert_eq!(records, 7 + 8 + 9);
            assert_counts_exact(&engine, "a forced resume");
        });
        // The same again, stopped part-way, for the cursor beside a mark below.
        lose_the_marks(&path);
        test_hooks::chunked_by(4, || {
            test_hooks::stop_after_rows(6);
            assert!(stopped(Engine::open(&path)));
            test_hooks::clear_stop();
        });
        // A cursor beside a mark is stale: the forced start deletes it.
        {
            let db = Database::open(&path).unwrap();
            let txn = db.begin_write().unwrap();
            txn.open_table(tables::LIVE_COUNTS_THROUGH)
                .unwrap()
                .insert(THROUGH, &[0u8; 8][..])
                .unwrap();
            txn.commit().unwrap();
        }
        test_hooks::fail_before_the_walk();
        assert!(Engine::open_with(&path, &OpenOptions::new().force_rebuild("a test")).is_err());
        assert_eq!(
            cursor_of(&path),
            None,
            "the forced start's first write dropped the stale cursor"
        );
    }

    #[test]
    fn the_cursor_round_trips_and_refuses_what_it_cannot_read() {
        let cursor = Cursor {
            build: "0.48.0".into(),
            schema: 4,
            format: 1,
            stopped_cleanly: true,
            after: Some((7, b"key".to_vec())),
            rows: 12,
            bytes: 3_400,
        };
        assert_eq!(Cursor::decode(&cursor.encode()), Some(cursor.clone()));
        let none = Cursor { after: None, ..cursor.clone() };
        assert_eq!(Cursor::decode(&none.encode()), Some(none));
        let mut longer = cursor.encode();
        longer.push(0);
        assert_eq!(Cursor::decode(&longer), None);
        assert_eq!(Cursor::decode(&[]), None);
        assert_eq!(Cursor::decode(&cursor.encode()[..5]), None);
    }
}
