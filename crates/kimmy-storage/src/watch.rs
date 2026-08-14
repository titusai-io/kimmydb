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

use kimmy_core::{CollectionId, Hlc, OpKind, OplogEntry, ResumeToken, Stamp};
use redb::{ReadableDatabase, ReadableTable};
use tokio::sync::broadcast;
use tracing::{debug, warn};

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
    /// This stream is over; the client must decide what to do rather than wait.
    /// Delivered rather than silently dropping events, or silently delivering
    /// none.
    ///
    /// Two shapes of reason, and they are different in kind. Retention
    /// collecting the range is a *gap*: events existed and cannot be read.
    /// The collection being dropped is an *end*: there is nothing further to
    /// watch. Falling behind the live channel is neither — that channel is
    /// only a wake-up, so a slow consumer re-reads from disk.
    Invalidate {
        reason: InvalidateReason,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InvalidateReason {
    /// The stream's position was collected before it could be read.
    ConsumerLagged,
    /// The resume point has been collected from the oplog.
    ResumeTokenExpired,
    /// The collection being watched was dropped.
    ///
    /// Only a stream scoped to *that* collection ends. A `Cluster` or
    /// `Database` stream keeps going, because for those a dropped collection
    /// is one of the things they are watching for rather than the end of what
    /// they watch.
    ///
    /// **Ids are derived from `(database, name)`** ([ADR-031]), so a
    /// collection recreated under the same name has the same id — and without
    /// this, a stream opened before the drop would silently resume delivering
    /// for the new collection, bridging two different collections that merely
    /// share a name. The stall was the visible half of that; this was the
    /// dangerous one.
    ///
    /// [ADR-031]: ../../../docs/decisions.md
    CollectionDropped,
}

impl InvalidateReason {
    /// The name on the wire.
    ///
    /// Written out rather than derived from `Debug`, which is what the HTTP
    /// layer used to format. A type that crosses a format boundary needs a
    /// *chosen* representation: `NodeId` and `CollectionId` have each cost a
    /// replication outage by inheriting one, and a `Debug` string means
    /// renaming a variant silently renames a value clients branch on.
    ///
    /// The two existing names are kept exactly as `Debug` rendered them, so
    /// this changes nothing already on the wire.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ConsumerLagged => "ConsumerLagged",
            Self::ResumeTokenExpired => "ResumeTokenExpired",
            Self::CollectionDropped => "CollectionDropped",
        }
    }
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

        // Streams follow *arrival* order, not stamp order. A replicated entry
        // keeps its origin stamp and so lands behind the local tail; following
        // stamp order would mean a subscriber already past that point never
        // saw it. See `tables::OPLOG_ARRIVAL`.
        let start = match (&options.resume_after, options.start_at) {
            // Resuming is exclusive of the token itself, so a client never sees
            // the last event it already acknowledged twice.
            (Some(token), _) => self.arrival_after(token)?,
            (None, Some(at)) => self.first_arrival_at_or_after(at)?,
            // No resume point: start live, skipping all history.
            (None, None) => self.next_arrival_seq()?,
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

    /// The arrival position just after the entry a token names.
    ///
    /// The token is a stamp, which is a public contract older than the arrival
    /// index, so it is translated here rather than changing what clients hold.
    fn arrival_after(&self, token: &ResumeToken) -> Result<u64> {
        let txn = self.db().begin_read()?;
        let by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ)?;
        let key = codec::oplog_key(&token.to_stamp());

        if let Some(seq) = by_stamp.get(key.as_slice())? {
            return Ok(seq.value() + 1);
        }

        // The entry is not in the log. Either it was collected — in which case
        // resuming would silently skip everything between — or it is newer than
        // anything we hold, which is fine and simply means nothing has happened
        // since. The oldest retained stamp distinguishes the two.
        drop(by_stamp);
        let collected = {
            let oplog = txn.open_table(tables::OPLOG)?;
            match oplog.first()? {
                Some((oldest, _)) => token.to_stamp() < codec::decode_oplog_key(oldest.value())?,
                // An empty log cannot have collected anything.
                None => false,
            }
        };

        if collected {
            return Err(StorageError::Core(kimmy_core::Error::ResumeTokenExpired));
        }
        drop(txn);
        self.next_arrival_seq()
    }

    /// The first arrival position whose entry is stamped at or after `at`.
    ///
    /// `start_at` is expressed in logical time, so it has to be resolved
    /// against arrival order. Scanning is acceptable because this runs once per
    /// stream and only for the explicit `start_at` form.
    fn first_arrival_at_or_after(&self, at: Hlc) -> Result<u64> {
        let txn = self.db().begin_read()?;
        let arrival = txn.open_table(tables::OPLOG_ARRIVAL)?;
        for row in arrival.iter()? {
            let (seq, key) = row?;
            if codec::decode_oplog_key(key.value())?.hlc >= at {
                return Ok(seq.value());
            }
        }
        drop(arrival);
        self.next_arrival_seq()
    }

    /// The position the next appended entry will take.
    fn next_arrival_seq(&self) -> Result<u64> {
        let txn = self.db().begin_read()?;
        let arrival = txn.open_table(tables::OPLOG_ARRIVAL)?;
        Ok(arrival.last()?.map_or(0, |(seq, _)| seq.value() + 1))
    }

    /// The oldest arrival position still retained.
    ///
    /// A stream whose next position is below this has had its replay range
    /// collected underneath it.
    fn oldest_arrival_seq(&self) -> Result<Option<u64>> {
        let txn = self.db().begin_read()?;
        let arrival = txn.open_table(tables::OPLOG_ARRIVAL)?;
        Ok(arrival.first()?.map(|(seq, _)| seq.value()))
    }

    /// Read up to `limit` entries in arrival order, starting at `from`.
    pub fn read_arrival_from(&self, from: u64, limit: usize) -> Result<Vec<OplogEntry>> {
        let txn = self.db().begin_read()?;
        let arrival = txn.open_table(tables::OPLOG_ARRIVAL)?;
        let oplog = txn.open_table(tables::OPLOG)?;

        let mut out = Vec::new();
        for row in arrival.range(from..)? {
            let (_, key) = row?;
            // The two tables are written in one transaction, so a missing entry
            // means the index outlived what it points at — a bug rather than a
            // race. Skipping keeps the stream serving; the count mismatch is
            // repaired on the next open.
            let Some(raw) = oplog.get(key.value())? else {
                warn!("arrival index points at a missing oplog entry");
                continue;
            };
            out.push(codec::decode_oplog_entry(raw.value())?);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
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

    /// Read up to `limit` oplog entries at or after `from`, in **stamp** order.
    ///
    /// This is the replication view, not the stream view: anti-entropy asks
    /// "what do you hold after this logical time", which is a question about
    /// origin stamps. Change streams use [`Self::read_arrival_from`] instead —
    /// see [`tables::OPLOG_ARRIVAL`] for why the two differ.
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
    /// Arrival position the next replay batch starts at, or `None` once replay
    /// is exhausted.
    next_replay_from: Option<u64>,
    /// The earliest position this stream may ever rewind to. A stream that
    /// deliberately started late must not be rewound into history it skipped.
    resume_floor: u64,
    replay: std::vec::IntoIter<OplogEntry>,
    /// The last stamp handed to the caller, which is what a resume token names.
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
    ///
    /// # Why the broadcast channel is only a wake-up
    ///
    /// Every entry delivered here is read from the arrival index, never from
    /// the channel — the channel's payload is discarded and only its *arrival*
    /// is used, as a signal that there may be more on disk.
    ///
    /// That is what makes ordering exact. Publication happens after the commit
    /// that assigned an arrival position, so two concurrent writers can publish
    /// in the opposite order from the one they committed in. A stream that
    /// trusted publication order would deliver those two events reversed, and a
    /// stream that de-duplicated by comparing stamps would drop the second one
    /// outright — which is exactly what a replicated entry looks like, since it
    /// carries an older stamp than the local tail.
    ///
    /// Reading from the index instead makes falling behind the channel buffer a
    /// non-event: the data is on disk either way, so a `Lagged` receiver is
    /// just a wake-up that arrived late.
    pub async fn next(&mut self, engine: &Engine) -> Option<ChangeEvent> {
        if self.finished {
            return None;
        }

        loop {
            // Phase 1: drain the batch already read.
            if let Some(entry) = self.replay.next() {
                if let Some(event) = self.accept(engine, Arc::new(entry)) {
                    return Some(event);
                }
                continue;
            }

            let from = self.next_replay_from.unwrap_or(self.resume_floor);

            // Phase 2: has retention collected the range we were about to read?
            // Detecting it here is what turns a silent gap into an event the
            // client can act on.
            match engine.oldest_arrival_seq() {
                Ok(Some(oldest)) if from < oldest => {
                    self.finished = true;
                    return Some(ChangeEvent::Invalidate {
                        reason: InvalidateReason::ConsumerLagged,
                    });
                }
                Err(e) => {
                    debug!(error = %e, "could not check the retained range; ending stream");
                    self.finished = true;
                    return None;
                }
                _ => {}
            }

            // Phase 3: read whatever has arrived since we last looked.
            match engine.read_arrival_from(from, REPLAY_BATCH) {
                Ok(batch) if !batch.is_empty() => {
                    self.next_replay_from = Some(from + batch.len() as u64);
                    self.replay = batch.into_iter();
                    continue;
                }
                Ok(_) => {}
                Err(e) => {
                    debug!(error = %e, "oplog replay failed; ending stream");
                    self.finished = true;
                    return None;
                }
            }

            // Phase 4: nothing new on disk. Wait to be told to look again.
            self.next_replay_from = Some(from);
            match self.rx.recv().await {
                // The payload is deliberately ignored; see the note above.
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    debug!(skipped, "wake-up channel lagged; re-reading from the arrival index");
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
    /// Only scope filtering: arrival positions are consumed strictly in order
    /// and never revisited, so there is no overlap to de-duplicate.
    fn accept(&mut self, engine: &Engine, entry: Arc<OplogEntry>) -> Option<ChangeEvent> {
        if !self.in_scope(engine, &entry) {
            return None;
        }

        // The collection this stream is watching has gone. Ending here rather
        // than at the edge that renders events, because *this* is where
        // `finished` lives: a consumer of `Engine::watch` that is not the HTTP
        // layer would otherwise keep a stream it believes is live.
        //
        // Scoped deliberately. A `Cluster` or `Database` stream sees drops of
        // collections it is not defined by, and ending those would take the
        // embedding worker down with the first dropped collection.
        if entry.kind == OpKind::DropCollection
            && matches!(self.scope, WatchScope::Collection(id) if id == entry.collection)
        {
            self.finished = true;
            return Some(ChangeEvent::Invalidate { reason: InvalidateReason::CollectionDropped });
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
    /// Schema-change entries share the collection id and would otherwise be
    /// counted alongside document changes.
    async fn take(engine: &Engine, stream: &mut ChangeStream, n: usize) -> Vec<ChangeEvent> {
        let mut out = Vec::new();
        while out.len() < n {
            let event =
                tokio::time::timeout(std::time::Duration::from_secs(5), stream.next(engine))
                    .await
                    .unwrap_or_else(|_| panic!("timed out after {} of {n} events", out.len()))
                    .expect("stream ended early");
            if let ChangeEvent::Change { entry, .. } = &event
                && !entry.kind.is_document()
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
    async fn dropping_the_collection_ends_a_stream_watching_it() {
        // Before this, the stream simply went quiet: no event, no end, nothing
        // to observe. Worse than the silence, ids are derived from
        // `(database, name)` — so a collection recreated under the same name
        // has the same id, and the stream would resume delivering for it,
        // bridging two different collections with nothing in between.
        let (engine, coll, _dir) = setup();
        engine.insert(&coll, doc! { "_id": 1i64 }).unwrap();

        let mut stream = engine
            .watch(
                WatchScope::Collection(coll.id),
                WatchOptions { start_at: Some(Hlc::ZERO), ..Default::default() },
            )
            .unwrap();
        assert_eq!(doc_ids(&take(&engine, &mut stream, 1).await), vec![1]);

        engine.drop_collection("app", "docs").unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next(&engine))
            .await
            .expect("a dropped collection must end the stream rather than stall it")
            .expect("an event");
        assert!(
            matches!(
                event,
                ChangeEvent::Invalidate { reason: InvalidateReason::CollectionDropped }
            ),
            "expected an invalidate, got {event:?}"
        );

        // And it stays ended: recreating the collection must not silently
        // adopt the stream that was watching the old one.
        let recreated = engine.create_collection("app", "docs").unwrap();
        assert_eq!(recreated.id, coll.id, "the id is derived from the name");
        engine.insert(&recreated, doc! { "_id": 2i64 }).unwrap();
        assert!(stream.next(&engine).await.is_none(), "the stream is over");
    }

    #[tokio::test]
    async fn dropping_one_collection_does_not_end_a_cluster_stream() {
        // The scope condition, stated as a test. A cluster-wide stream is the
        // embedding worker's, and ending it on the first dropped collection
        // anywhere would stop embedding for the whole node.
        let (engine, coll, _dir) = setup();
        let other = engine.create_collection("app", "other").unwrap();

        let mut stream = engine
            .watch(
                WatchScope::Cluster,
                WatchOptions { start_at: Some(Hlc::ZERO), ..Default::default() },
            )
            .unwrap();

        engine.drop_collection("app", "docs").unwrap();
        engine.insert(&other, doc! { "_id": 7i64 }).unwrap();

        // The drop is not delivered as a document change, and the stream is
        // still live enough to deliver the write that followed it.
        assert_eq!(doc_ids(&take(&engine, &mut stream, 1).await), vec![7]);
        let _ = coll;
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
    // -----------------------------------------------------------------------
    // Arrival order
    // -----------------------------------------------------------------------

    /// A remote entry stamped *behind* the local tail, as replication produces.
    fn remote_entry(coll: &CollectionMeta, id: &str, wall_ms: u64) -> OplogEntry {
        OplogEntry {
            stamp: Stamp::new(Hlc::new(wall_ms, 0), kimmy_core::NodeId::generate()),
            kind: OpKind::Insert,
            collection: coll.id,
            doc_id: Some(kimmy_core::DocId::String(id.into())),
            body: Some(bson::serialize_to_vec(&doc! { "_id": id }).unwrap()),
        }
    }

    /// The bug this whole index exists for.
    #[tokio::test]
    async fn a_replicated_entry_reaches_a_subscriber_already_past_its_stamp() {
        let (engine, coll, _dir) = setup();

        // Local history, so the tail is well above where the remote entry will
        // be stamped.
        for i in 0..5i64 {
            engine.insert(&coll, doc! { "_id": i }).unwrap();
        }

        // A live subscriber, caught up to the tail.
        let mut stream = engine.watch(WatchScope::Cluster, WatchOptions::default()).unwrap();

        // Now apply a remote write stamped in 1970 — far behind everything the
        // subscriber has already read past. Under stamp ordering it would land
        // behind the stream position and never be delivered.
        let entry = remote_entry(&coll, "from-a-peer", 1);
        engine.apply_remote(&coll, &entry).unwrap();

        let events = take(&engine, &mut stream, 1).await;
        match &events[0] {
            ChangeEvent::Change { entry, .. } => {
                assert_eq!(
                    entry.doc_id,
                    Some(kimmy_core::DocId::String("from-a-peer".into())),
                    "a replicated write must reach a live subscriber"
                );
            }
            other => panic!("expected a change, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_replicated_entry_is_delivered_after_a_resume() {
        // Same property across a disconnect: the token is a stamp, and the
        // entry sorts below it, so translating the token to an arrival position
        // is what keeps this working.
        let (engine, coll, _dir) = setup();
        for i in 0..5i64 {
            engine.insert(&coll, doc! { "_id": i }).unwrap();
        }

        let mut stream = engine
            .watch(
                WatchScope::Cluster,
                WatchOptions { start_at: Some(Hlc::ZERO), ..Default::default() },
            )
            .unwrap();
        let events = take(&engine, &mut stream, 5).await;
        let token = stream.resume_token().expect("a token after delivery");
        drop(stream);

        engine.apply_remote(&coll, &remote_entry(&coll, "late", 1)).unwrap();

        let mut resumed = engine
            .watch(
                WatchScope::Cluster,
                WatchOptions { resume_after: Some(token), ..Default::default() },
            )
            .unwrap();
        let after = take(&engine, &mut resumed, 1).await;

        assert_eq!(doc_ids(&events), vec![0, 1, 2, 3, 4]);
        match &after[0] {
            ChangeEvent::Change { entry, .. } => {
                assert_eq!(entry.doc_id, Some(kimmy_core::DocId::String("late".into())))
            }
            other => panic!("expected a change, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn re_applying_a_remote_entry_does_not_deliver_it_twice() {
        // Peers resend overlapping ranges routinely. A second append must not
        // take a second arrival position.
        let (engine, coll, _dir) = setup();
        let mut stream = engine.watch(WatchScope::Cluster, WatchOptions::default()).unwrap();

        let entry = remote_entry(&coll, "dup", 1);
        engine.apply_remote(&coll, &entry).unwrap();
        engine.apply_remote(&coll, &entry).unwrap();
        engine.insert(&coll, doc! { "_id": "after" }).unwrap();

        let events = take(&engine, &mut stream, 2).await;
        let ids = doc_ids_str(&events);
        assert_eq!(ids, vec!["dup", "after"], "the resend must not be redelivered");
    }

    #[test]
    fn the_arrival_index_is_rebuilt_when_it_does_not_cover_the_oplog() {
        // The index is derived state, so a database written without it — or by
        // a build that stopped maintaining it — is repaired rather than refused.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        let entries = {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("app", "docs").unwrap();
            for i in 0..10i64 {
                engine.insert(&coll, doc! { "_id": i }).unwrap();
            }
            engine.read_arrival_from(0, 1000).unwrap().len()
        };

        // Wipe the index, simulating a database that predates it.
        {
            let db = redb::Database::create(&path).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut arrival = txn.open_table(tables::OPLOG_ARRIVAL).unwrap();
                let mut by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ).unwrap();
                arrival.retain(|_, _| false).unwrap();
                by_stamp.retain(|_, _| false).unwrap();
            }
            txn.commit().unwrap();
        }

        let reopened = Engine::open(&path).unwrap();
        assert_eq!(
            reopened.read_arrival_from(0, 1000).unwrap().len(),
            entries,
            "reopening must rebuild the index from the oplog"
        );
    }

    #[tokio::test]
    async fn a_stream_whose_range_was_collected_is_invalidated() {
        // Retention can remove the range a live stream was about to read. That
        // used to be a silent gap; it is now an event the client can act on.
        use crate::gc::RetentionPolicy;

        let (engine, coll, _dir) = setup();
        let mut stream = engine
            .watch(
                WatchScope::Cluster,
                WatchOptions { start_at: Some(Hlc::ZERO), ..Default::default() },
            )
            .unwrap();

        for i in 0..10i64 {
            engine.insert(&coll, doc! { "_id": i }).unwrap();
        }
        engine
            .collect_garbage_at(
                crate::engine::physical_now_ms() + 1_000_000_000,
                RetentionPolicy::new(0, 0),
            )
            .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next(&engine))
            .await
            .expect("timed out")
            .expect("stream ended instead of invalidating");

        assert!(
            matches!(event, ChangeEvent::Invalidate { reason: InvalidateReason::ConsumerLagged }),
            "expected an invalidate, got {event:?}"
        );
    }

    /// String document ids from a batch of events.
    fn doc_ids_str(events: &[ChangeEvent]) -> Vec<String> {
        events
            .iter()
            .filter_map(|e| match e {
                ChangeEvent::Change { entry, .. } => match &entry.doc_id {
                    Some(kimmy_core::DocId::String(s)) => Some(s.clone()),
                    _ => None,
                },
                _ => None,
            })
            .collect()
    }
}
