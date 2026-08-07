//! Change streams.
//!
//! A change stream is a durable oplog replay spliced onto a live broadcast. The
//! splice is the whole difficulty, and the order matters:
//!
//! 1. **Subscribe to the live channel first.** Anything committed from this
//!    moment on is buffered for us.
//! 2. **Then replay the oplog** from the resume point to its tail.
//! 3. **Then switch to live**, discarding anything already delivered during
//!    replay.
//!
//! Doing it the other way round — read the oplog, then subscribe — leaves a
//! window between the read and the subscription in which committed events reach
//! nobody. That gap is silent, intermittent, and load-dependent, so it gets a
//! dedicated test rather than a comment.
//!
//! This works on a single node with no peers, because the oplog exists whether
//! or not the node has ever seen one. That is the reason change streams here do
//! not require a replica set.

use std::collections::HashMap;
use std::sync::Arc;

use kimmy_core::{CollectionId, Hlc, OplogEntry, ResumeToken, Stamp};
use redb::{ReadableDatabase, ReadableTable};
use tokio::sync::broadcast;
use tracing::debug;

use crate::codec;
use crate::engine::Engine;
use crate::error::{Result, StorageError};
use crate::tables;

/// How many oplog entries are read per replay batch.
const REPLAY_BATCH: usize = 1024;

/// What a stream is watching.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchScope {
    /// Every change on this node.
    Cluster,
    /// Every change in one database.
    Database(String),
    Collection(CollectionId),
}

/// Where a stream starts.
#[derive(Clone, Debug, Default)]
pub struct WatchOptions {
    /// Resume immediately *after* this token.
    pub resume_after: Option<ResumeToken>,
    /// Start at this logical time, inclusive. Ignored if `resume_after` is set.
    pub start_at: Option<Hlc>,
}

/// One item from a change stream.
#[derive(Clone, Debug)]
pub enum ChangeEvent {
    Change {
        entry: Arc<OplogEntry>,
        token: ResumeToken,
    },
    /// The stream can no longer be trusted to be gap-free; the client must
    /// resubscribe. Delivered rather than silently dropping events.
    ///
    /// Currently unreachable, and deliberately so: falling behind the live
    /// buffer recovers from the oplog, and a resume point that predates the log
    /// is rejected up front by [`Engine::watch`] rather than mid-stream. This
    /// becomes reachable when oplog collection lands and a stream can have its
    /// replay range removed underneath it.
    Invalidate {
        reason: InvalidateReason,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InvalidateReason {
    /// The consumer fell behind and the oplog no longer covers the gap.
    ConsumerLagged,
    /// The resume point has been collected from the oplog.
    ResumeTokenExpired,
}

impl Engine {
    /// Open a change stream.
    ///
    /// Fails only if the resume token predates the retained oplog, which is a
    /// condition the caller must handle rather than silently skip past.
    pub fn watch(&self, scope: WatchScope, options: WatchOptions) -> Result<ChangeStream> {
        // Step 1: subscribe *before* reading the oplog. Reversing these two
        // lines reintroduces the gap this whole module exists to avoid.
        let rx = self.subscribe();

        let start = match (&options.resume_after, options.start_at) {
            // Resuming is exclusive of the token itself, so a client never sees
            // the last event it already acknowledged twice.
            (Some(token), _) => {
                self.check_resume_point(token)?;
                token.exclusive_start()
            }
            (None, Some(at)) => at,
            (None, None) => {
                // No resume point: start live, skipping all history.
                self.current_tail()?.map_or(Hlc::ZERO, Hlc::successor)
            }
        };

        Ok(ChangeStream {
            rx,
            scope,
            next_replay_from: Some(start),
            resume_floor: start,
            replay: Vec::new().into_iter(),
            last_delivered: None,
            db_of_collection: HashMap::new(),
            finished: false,
        })
    }

    /// Reject a resume token whose position has already been collected.
    ///
    /// A token that is simply *newer* than everything on disk is fine — it just
    /// means nothing has happened since — so only a token older than the oldest
    /// retained entry is expired.
    fn check_resume_point(&self, token: &ResumeToken) -> Result<()> {
        let txn = self.db().begin_read()?;
        let oplog = txn.open_table(tables::OPLOG)?;
        let Some((oldest_key, _)) = oplog.first()? else {
            // An empty oplog cannot have collected anything.
            return Ok(());
        };
        let oldest = codec::decode_oplog_key(oldest_key.value())?;
        if token.to_stamp() < oldest {
            return Err(StorageError::Core(kimmy_core::Error::ResumeTokenExpired));
        }
        Ok(())
    }

    fn current_tail(&self) -> Result<Option<Hlc>> {
        let txn = self.db().begin_read()?;
        let oplog = txn.open_table(tables::OPLOG)?;
        match oplog.last()? {
            Some((key, _)) => Ok(Some(codec::decode_oplog_key(key.value())?.hlc)),
            None => Ok(None),
        }
    }

    /// Read up to `limit` oplog entries at or after `from`.
    pub fn read_oplog_from(&self, from: Hlc, limit: usize) -> Result<Vec<OplogEntry>> {
        let txn = self.db().begin_read()?;
        let oplog = txn.open_table(tables::OPLOG)?;
        let lower = codec::oplog_key_lower_bound(from);

        let mut out = Vec::new();
        for entry in oplog.range(lower.as_slice()..)? {
            let (_, value) = entry?;
            out.push(codec::decode_oplog_entry(value.value())?);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
}

/// A gap-free stream of changes.
pub struct ChangeStream {
    rx: broadcast::Receiver<Arc<OplogEntry>>,
    scope: WatchScope,
    /// Where the next replay batch starts, or `None` once replay is exhausted.
    next_replay_from: Option<Hlc>,
    /// The earliest point this stream may ever rewind to. Recovering from lag
    /// must not resurrect history the client deliberately started after.
    resume_floor: Hlc,
    replay: std::vec::IntoIter<OplogEntry>,
    /// The highest stamp handed to the caller, used to discard live events that
    /// replay already covered.
    last_delivered: Option<Stamp>,
    /// Cache for database-scoped filtering.
    db_of_collection: HashMap<CollectionId, String>,
    finished: bool,
}

impl ChangeStream {
    /// The next change, or `None` once the engine is dropped.
    ///
    /// Cancel-safe with respect to replay: an abandoned call may drop a live
    /// event, so callers must not race this in a `select!` they intend to
    /// resume.
    pub async fn next(&mut self, engine: &Engine) -> Option<ChangeEvent> {
        if self.finished {
            return None;
        }

        loop {
            // Phase 1: drain the current replay batch.
            if let Some(entry) = self.replay.next() {
                if let Some(event) = self.accept(engine, Arc::new(entry)) {
                    return Some(event);
                }
                continue;
            }

            // Phase 2: refill from the oplog until it runs dry.
            if let Some(from) = self.next_replay_from {
                match engine.read_oplog_from(from, REPLAY_BATCH) {
                    Ok(batch) => {
                        // A short batch means we have caught up to the tail;
                        // everything after this arrives on the live channel.
                        self.next_replay_from = if batch.len() < REPLAY_BATCH {
                            None
                        } else {
                            batch.last().map(|e| e.stamp.hlc.successor())
                        };
                        self.replay = batch.into_iter();
                        continue;
                    }
                    Err(e) => {
                        debug!(error = %e, "oplog replay failed; ending stream");
                        self.finished = true;
                        return None;
                    }
                }
            }

            // Phase 3: live.
            match self.rx.recv().await {
                Ok(entry) => {
                    if let Some(event) = self.accept(engine, entry) {
                        return Some(event);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    // Falling behind the in-memory buffer is recoverable,
                    // because the same events are on disk. Rewind to just after
                    // the last one delivered and replay from the oplog instead
                    // of invalidating the client. Only oplog *collection* can
                    // make a gap unrecoverable, and that is caught at watch
                    // time by `check_resume_point`.
                    debug!(skipped, "consumer lagged; recovering from the oplog");
                    let from = self
                        .last_delivered
                        .map_or(self.resume_floor, |stamp| stamp.hlc.successor());
                    self.next_replay_from = Some(from);
                }
                Err(broadcast::error::RecvError::Closed) => {
                    self.finished = true;
                    return None;
                }
            }
        }
    }

    /// Decide whether an entry should be delivered.
    ///
    /// Filters by scope, and discards anything at or below the high-water mark
    /// so that the overlap between replay and live is not delivered twice.
    fn accept(&mut self, engine: &Engine, entry: Arc<OplogEntry>) -> Option<ChangeEvent> {
        if let Some(last) = self.last_delivered
            && entry.stamp <= last
        {
            return None;
        }
        if !self.in_scope(engine, &entry) {
            // Still advance the mark: a filtered-out entry is one we have
            // decided about, and not advancing would re-examine it forever.
            self.last_delivered = Some(entry.stamp);
            return None;
        }

        self.last_delivered = Some(entry.stamp);
        let token = entry.resume_token();
        Some(ChangeEvent::Change { entry, token })
    }

    fn in_scope(&mut self, engine: &Engine, entry: &OplogEntry) -> bool {
        match &self.scope {
            WatchScope::Cluster => true,
            WatchScope::Collection(id) => entry.collection == *id,
            WatchScope::Database(db) => {
                // Resolving the owning database needs a lookup, so cache it;
                // a collection's database never changes.
                if let Some(known) = self.db_of_collection.get(&entry.collection) {
                    return known == db;
                }
                match engine.database_of_collection(entry.collection) {
                    Ok(Some(owner)) => {
                        let matched = owner == *db;
                        self.db_of_collection.insert(entry.collection, owner);
                        matched
                    }
                    // A dropped collection has no owner; its trailing events
                    // are not attributable to a database.
                    _ => false,
                }
            }
        }
    }

    /// The token to resume from, reflecting everything delivered so far.
    pub fn resume_token(&self) -> Option<ResumeToken> {
        self.last_delivered.map(ResumeToken::from_stamp)
    }
}

impl Engine {
    /// Which database owns a collection id.
    pub(crate) fn database_of_collection(&self, id: CollectionId) -> Result<Option<String>> {
        let txn = self.db().begin_read()?;
        let collections = txn.open_table(tables::COLLECTIONS)?;
        for entry in collections.iter()? {
            let (_, value) = entry?;
            let meta: crate::meta::CollectionMeta = serde_json::from_slice(value.value())?;
            if meta.id == id {
                return Ok(Some(meta.db));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use kimmy_core::OpKind;

    use super::*;
    use crate::meta::CollectionMeta;

    fn setup() -> (Engine, CollectionMeta, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let coll = engine.create_collection("app", "docs").unwrap();
        (engine, coll, dir)
    }

    /// Collect the next `n` *document* changes, failing rather than hanging.
    ///
    /// Collection-level entries (create/drop) share the collection id and would
    /// otherwise be counted alongside document changes.
    async fn take(engine: &Engine, stream: &mut ChangeStream, n: usize) -> Vec<ChangeEvent> {
        let mut out = Vec::new();
        while out.len() < n {
            let event =
                tokio::time::timeout(std::time::Duration::from_secs(5), stream.next(engine))
                    .await
                    .unwrap_or_else(|_| panic!("timed out after {} of {n} events", out.len()))
                    .expect("stream ended early");
            if let ChangeEvent::Change { entry, .. } = &event
                && entry.kind == OpKind::Collection
            {
                continue;
            }
            out.push(event);
        }
        out
    }

    fn doc_ids(events: &[ChangeEvent]) -> Vec<i64> {
        events
            .iter()
            .filter_map(|e| match e {
                ChangeEvent::Change { entry, .. } => match entry.doc_id.as_ref() {
                    Some(kimmy_core::DocId::Int64(n)) => Some(*n),
                    _ => None,
                },
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn delivers_changes_as_they_happen() {
        let (engine, coll, _dir) = setup();
        let mut stream = engine.watch(WatchScope::Cluster, WatchOptions::default()).unwrap();

        engine.insert(&coll, doc! { "_id": 1i64 }).unwrap();
        engine.insert(&coll, doc! { "_id": 2i64 }).unwrap();

        let events = take(&engine, &mut stream, 2).await;
        assert_eq!(doc_ids(&events), vec![1, 2]);
    }

    #[tokio::test]
    async fn a_fresh_stream_skips_prior_history() {
        let (engine, coll, _dir) = setup();
        engine.insert(&coll, doc! { "_id": 1i64 }).unwrap();

        let mut stream = engine.watch(WatchScope::Cluster, WatchOptions::default()).unwrap();
        engine.insert(&coll, doc! { "_id": 2i64 }).unwrap();

        let events = take(&engine, &mut stream, 1).await;
        assert_eq!(doc_ids(&events), vec![2], "history predating the stream must not replay");
    }

    #[tokio::test]
    async fn start_at_replays_history() {
        let (engine, coll, _dir) = setup();
        engine.insert(&coll, doc! { "_id": 1i64 }).unwrap();
        engine.insert(&coll, doc! { "_id": 2i64 }).unwrap();

        let mut stream = engine
            .watch(
                WatchScope::Collection(coll.id),
                WatchOptions { start_at: Some(Hlc::ZERO), ..Default::default() },
            )
            .unwrap();

        let events = take(&engine, &mut stream, 2).await;
        assert_eq!(doc_ids(&events), vec![1, 2]);
    }

    #[tokio::test]
    async fn resuming_is_exclusive_of_the_token() {
        let (engine, coll, _dir) = setup();
        engine.insert(&coll, doc! { "_id": 1i64 }).unwrap();
        engine.insert(&coll, doc! { "_id": 2i64 }).unwrap();

        let mut first = engine
            .watch(
                WatchScope::Collection(coll.id),
                WatchOptions { start_at: Some(Hlc::ZERO), ..Default::default() },
            )
            .unwrap();
        let events = take(&engine, &mut first, 1).await;
        assert_eq!(doc_ids(&events), vec![1]);
        let token = first.resume_token().expect("a delivered event yields a token");

        // Resuming must not redeliver document 1.
        let mut resumed = engine
            .watch(
                WatchScope::Collection(coll.id),
                WatchOptions { resume_after: Some(token), ..Default::default() },
            )
            .unwrap();
        let events = take(&engine, &mut resumed, 1).await;
        assert_eq!(doc_ids(&events), vec![2]);
    }

    /// The reason this module is structured the way it is.
    ///
    /// A subscriber disconnects, writes continue during the gap, and it then
    /// resumes from its token. The delivered sequence must contain every write
    /// exactly once — no gap where the replay met the live feed, and no
    /// duplicate across the overlap.
    #[tokio::test]
    async fn resuming_under_continuous_writes_has_no_gaps_and_no_duplicates() {
        let (engine, coll, _dir) = setup();
        const TOTAL: i64 = 500;
        const DISCONNECT_AFTER: usize = 20;

        // redb permits only one handle per file, so the writer shares this one
        // rather than opening its own.
        let engine = std::sync::Arc::new(engine);
        let mut stream = engine.watch(WatchScope::Cluster, WatchOptions::default()).unwrap();

        // Writes run concurrently with reading, so the replay-to-live splice
        // happens under real contention rather than against a quiet log.
        let writer = std::thread::spawn({
            let engine = std::sync::Arc::clone(&engine);
            move || {
                for i in 0..TOTAL {
                    engine.insert(&coll, doc! { "_id": i }).unwrap();
                }
            }
        });

        // Read a prefix, then "disconnect".
        let mut seen: Vec<i64> = Vec::new();
        while seen.len() < DISCONNECT_AFTER {
            let events = take(&engine, &mut stream, 1).await;
            seen.extend(doc_ids(&events));
        }
        let token = stream.resume_token().expect("delivered events yield a token");
        drop(stream);

        writer.join().unwrap();

        // Resume from the token and drain the rest.
        let mut resumed = engine
            .watch(
                WatchScope::Cluster,
                WatchOptions { resume_after: Some(token), ..Default::default() },
            )
            .unwrap();

        while (seen.len() as i64) < TOTAL {
            let events = take(&engine, &mut resumed, 1).await;
            seen.extend(doc_ids(&events));
        }

        let expected: Vec<i64> = (0..TOTAL).collect();
        assert_eq!(seen.len(), expected.len(), "wrong number of events delivered");
        assert_eq!(seen, expected, "events must arrive exactly once, in order");
    }

    #[tokio::test]
    async fn collection_scope_filters_other_collections() {
        let (engine, coll, _dir) = setup();
        let other = engine.create_collection("app", "other").unwrap();

        let mut stream =
            engine.watch(WatchScope::Collection(coll.id), WatchOptions::default()).unwrap();

        engine.insert(&other, doc! { "_id": 99i64 }).unwrap();
        engine.insert(&coll, doc! { "_id": 1i64 }).unwrap();

        let events = take(&engine, &mut stream, 1).await;
        assert_eq!(doc_ids(&events), vec![1], "the other collection must be filtered out");
    }

    #[tokio::test]
    async fn database_scope_filters_other_databases() {
        let (engine, coll, _dir) = setup();
        let elsewhere = engine.create_collection("other", "docs").unwrap();

        let mut stream =
            engine.watch(WatchScope::Database("app".into()), WatchOptions::default()).unwrap();

        engine.insert(&elsewhere, doc! { "_id": 99i64 }).unwrap();
        engine.insert(&coll, doc! { "_id": 1i64 }).unwrap();

        let events = take(&engine, &mut stream, 1).await;
        assert_eq!(doc_ids(&events), vec![1]);
    }

    #[tokio::test]
    async fn deletes_and_replaces_appear_with_their_kind() {
        let (engine, coll, _dir) = setup();
        let mut stream =
            engine.watch(WatchScope::Collection(coll.id), WatchOptions::default()).unwrap();

        let id = engine.insert(&coll, doc! { "_id": 1i64 }).unwrap();
        engine.replace(&coll, &id, doc! { "v": 2 }, false).unwrap();
        engine.delete(&coll, &id).unwrap();

        let events = take(&engine, &mut stream, 3).await;
        let kinds: Vec<OpKind> = events
            .iter()
            .filter_map(|e| match e {
                ChangeEvent::Change { entry, .. } => Some(entry.kind),
                _ => None,
            })
            .collect();
        assert_eq!(kinds, vec![OpKind::Insert, OpKind::Replace, OpKind::Delete]);
    }

    #[tokio::test]
    async fn an_expired_resume_token_is_reported_rather_than_skipped() {
        let (engine, coll, _dir) = setup();
        engine.insert(&coll, doc! { "_id": 1i64 }).unwrap();

        // A token from before the oldest retained entry: silently starting from
        // the beginning would hide the fact that events were missed.
        let ancient = ResumeToken::new(Hlc::new(1, 0), engine.node_id());
        let result = engine.watch(
            WatchScope::Cluster,
            WatchOptions { resume_after: Some(ancient), ..Default::default() },
        );
        assert!(matches!(
            result.err(),
            Some(StorageError::Core(kimmy_core::Error::ResumeTokenExpired))
        ));
    }

    #[tokio::test]
    async fn a_token_newer_than_the_oplog_is_accepted() {
        // Nothing has happened since; that is not the same as expiry.
        let (engine, coll, _dir) = setup();
        engine.insert(&coll, doc! { "_id": 1i64 }).unwrap();

        let future = ResumeToken::new(Hlc::new(u64::MAX / 2, 0), engine.node_id());
        assert!(
            engine
                .watch(
                    WatchScope::Cluster,
                    WatchOptions { resume_after: Some(future), ..Default::default() }
                )
                .is_ok()
        );
    }

    /// A consumer that falls far behind the in-memory buffer must still receive
    /// every event, because the oplog can supply what the buffer dropped.
    #[tokio::test]
    async fn a_lagging_consumer_recovers_from_the_oplog_without_losing_events() {
        let (engine, coll, _dir) = setup();
        let mut stream = engine.watch(WatchScope::Cluster, WatchOptions::default()).unwrap();

        // Several times the live buffer, written without anyone reading.
        const TOTAL: i64 = 2_500;
        for i in 0..TOTAL {
            engine.insert(&coll, doc! { "_id": i }).unwrap();
        }

        let mut seen = Vec::new();
        while (seen.len() as i64) < TOTAL {
            match tokio::time::timeout(std::time::Duration::from_secs(5), stream.next(&engine))
                .await
                .expect("timed out")
            {
                Some(ChangeEvent::Change { entry, .. }) => {
                    if let Some(kimmy_core::DocId::Int64(n)) = entry.doc_id.as_ref() {
                        seen.push(*n);
                    }
                }
                Some(ChangeEvent::Invalidate { reason }) => {
                    panic!("lag should recover from the oplog, got {reason:?}")
                }
                None => panic!("stream ended early after {} events", seen.len()),
            }
        }

        assert_eq!(seen, (0..TOTAL).collect::<Vec<_>>(), "lag must not create a gap");
    }

    /// Recovery must not rewind past where the client asked to start.
    #[tokio::test]
    async fn lag_recovery_does_not_resurrect_skipped_history() {
        let (engine, coll, _dir) = setup();
        for i in 0..10i64 {
            engine.insert(&coll, doc! { "_id": i }).unwrap();
        }

        // A default stream deliberately skips everything above.
        let mut stream = engine.watch(WatchScope::Cluster, WatchOptions::default()).unwrap();
        for i in 100..2_500i64 {
            engine.insert(&coll, doc! { "_id": i }).unwrap();
        }

        let events = take(&engine, &mut stream, 1).await;
        assert_eq!(doc_ids(&events), vec![100], "must not replay pre-stream history");
    }
}
