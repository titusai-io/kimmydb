//! A drop that reaches a newer life of its collection (ADR-231).
//!
//! A collection's id derives from its name, so two members can create the same
//! name independently, and a drop made on one of them can reach the other after
//! that member has made a life of its own. [`aims_at_a_previous_incarnation`]
//! judges such a drop to be aimed at a life already over here and ignores it.
//! That was right for the collection and wrong for what the other life had put
//! in it: a document of the life before, stamped at or before the drop, that
//! arrived **before** the drop was known (entries arrive in stamp order, so the
//! document precedes the drop that destroys it) was applied into the newer
//! life, and nothing removed it. The member that made the drop had removed it
//! with the drop; the other kept it for good, the counts differed, and the
//! count half's repair ran again every few seconds without converging.
//!
//! The rule here is ADR-081's incarnation floor, extended to the late drop. A
//! collection whose `created` is above the drop and whose floor is below it
//! **owes a sweep**: it removes every record stamped at or before the drop, as
//! a replicated delete at the drop's stamp (no new entry: every member does the
//! same on learning of the drop), indexes and vector rows with them, in chunks
//! that look for a stop between two, and then **sets its floor at the drop**,
//! which is also what says the sweep was done. A record at or before the drop
//! in a life created after it can only be a straggler of the life before:
//! every write this member mints is above its `created`, and the creation was
//! stamped by a clock that had witnessed what it replicated.
//!
//! The floor is set last, so a sweep that was cut short owes again, to the
//! entry that comes round once more and to the start's reconcile
//! ([`Engine::reconcile_late_drops`]), which also heals a store that holds the
//! stragglers of a drop applied before this rule existed.

use kimmy_core::{CollectionId, Hlc, Stamp};
use redb::ReadableTable;
use tracing::{debug, info, warn};

use crate::codec;
use crate::engine::{Engine, WriterHolder, doc_range_after};
use crate::error::Result;
use crate::meta::CollectionMeta;
use crate::tables;

/// Records examined in one chunk, and removed at most: one transaction holds
/// the writer for no more than this.
const SWEEP_CHUNK: usize = 1_000;

/// And the bytes of the records examined in one chunk, since a thousand large
/// documents are a large transaction and a large `found`.
const SWEEP_CHUNK_BYTES: usize = 8 * 1024 * 1024;

/// What a sweep asks between two chunks: `Err(Stopping)` when it must end.
pub(crate) type StopCheck<'a> = &'a dyn Fn() -> Result<()>;

/// Test hooks: where a stop lands that a start's SIGTERM would.
#[cfg(test)]
pub(crate) mod hooks {
    use std::cell::Cell;

    thread_local! {
        /// How many drops on this thread took the survivors path.
        static SURVIVORS_PATHS: Cell<usize> = const { Cell::new(0) };
        /// Run once after the next chunk commits on this thread: where a write
        /// lands in the middle of a sweep.
        static AFTER_A_CHUNK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
            const { std::cell::RefCell::new(None) };
        /// Ask the open to stop (as a SIGTERM at a start does) once this many
        /// chunks have committed on this thread.
        static STOP_THE_OPEN_AFTER: Cell<Option<usize>> = const { Cell::new(None) };
    }

    pub(crate) fn stop_the_open_after_chunks(chunks: usize) {
        STOP_THE_OPEN_AFTER.with(|c| c.set(Some(chunks)));
    }

    pub(super) fn took_the_survivors_path() {
        SURVIVORS_PATHS.with(|c| c.set(c.get() + 1));
    }

    /// Drops that took the survivors path on this thread so far.
    pub(crate) fn survivors_paths() -> usize {
        SURVIVORS_PATHS.with(|c| c.get())
    }

    pub(crate) fn run_after_the_next_chunk(f: impl FnOnce() + 'static) {
        AFTER_A_CHUNK.with(|c| *c.borrow_mut() = Some(Box::new(f)));
    }

    pub(super) fn after_a_chunk() {
        if let Some(f) = AFTER_A_CHUNK.with(|c| c.borrow_mut().take()) {
            f();
        }
        STOP_THE_OPEN_AFTER.with(|c| match c.get() {
            Some(1) => {
                c.set(None);
                crate::open_progress::tests::THIS_THREAD_STOPS.with(|s| s.set(true));
            }
            Some(n) => c.set(Some(n - 1)),
            None => {}
        });
    }
}

/// How a sweep ended: what it removed, and whether the collection stands. A
/// re-dated life that the sweep leaves with nothing live, and no definition
/// stamped after the drop, is buried by the sweep in its last commit.
struct SweepEnd {
    removed: usize,
    survived: bool,
}

/// What one chunk of a sweep did.
struct Chunk {
    removed: usize,
    skipped: usize,
    last: Option<Vec<u8>>,
    exhausted: bool,
    /// The lowest arrival position among the entries of the records this chunk
    /// kept (stamped after the drop), for a re-dated life.
    first_arrival: Option<u64>,
}

/// Whether a collection standing here owes a sweep for a drop stamped
/// `dropped`: its life began after the drop (`created` above it) and its floor
/// has not been taken up to it.
pub(crate) fn owes_a_sweep(standing: &CollectionMeta, dropped: Hlc) -> bool {
    dropped < standing.created && standing.incarnation_floor.is_none_or(|floor| floor < dropped)
}

impl Engine {
    /// [`Self::sweep_late_drop_until`], stopped by the engine's own stop: what
    /// an apply and a snapshot page ask.
    pub(crate) fn sweep_late_drop(&self, db: &str, name: &str, dropped: Stamp) -> Result<usize> {
        self.sweep_late_drop_until(db, name, dropped, &|| {
            self.check_walk(crate::WalkScope::Background)
        })
    }

    /// Sweep the stragglers of the life before `dropped` out of the collection
    /// `db.name`, if it owes the sweep: the definitions that life brought
    /// (indexes created, and a vector configuration changed, at or before the
    /// drop), then the records, and the floor taken up to the drop in the
    /// commit of the last chunk. Returns how many records it removed.
    ///
    /// Called with the drop's tombstone already recorded, so a record that
    /// arrives while the sweep runs is refused as history. `stop` is asked
    /// before each chunk: an apply asks the engine's stop, a start the open's
    /// (a SIGTERM at a start sets only that one). A stop ends it with
    /// `Stopping`; the chunks that landed stand and the next call finds the
    /// rest, the floor being unset.
    ///
    /// A life that a drop re-dated (see [`Self::drop_keeping_later_writes`])
    /// and that the sweep leaves with nothing is buried in the commit of its
    /// last chunk.
    pub(crate) fn sweep_late_drop_until(
        &self,
        db: &str,
        name: &str,
        dropped: Stamp,
        stop: StopCheck<'_>,
    ) -> Result<usize> {
        Ok(self.sweep_until(db, name, dropped, stop)?.removed)
    }

    fn sweep_until(
        &self,
        db: &str,
        name: &str,
        dropped: Stamp,
        stop: StopCheck<'_>,
    ) -> Result<SweepEnd> {
        // A shadow is decided by its parent: the parent is swept first, and
        // buries the shadow with itself if it is left with nothing. Whatever
        // order a start, a replay or a drop's own path reaches them in.
        if let Some(parent) = kimmy_core::vector_meta::base_name(name)
            && let Ok(standing) = self.get_collection(db, parent)
            && owes_a_sweep(&standing, dropped.hlc)
        {
            self.sweep_until(db, parent, dropped, stop)?;
        }
        let mut survived = true;
        let mut removed = 0usize;
        let mut skipped = 0usize;
        let mut after: Option<Vec<u8>> = None;
        let mut started = false;
        let mut first_arrival: Option<u64> = None;
        loop {
            stop()?;
            if !started {
                // Definitions first, so the floor can be committed with the
                // last chunk of records.
                self.sweep_late_definitions(db, name, dropped)?;
            }
            let txn = self.begin_write(WriterHolder::Drop)?;
            let Some(mut standing) = read_collection(&txn, db, name)? else {
                txn.abort()?;
                return Ok(SweepEnd { removed, survived: false });
            };
            if !owes_a_sweep(&standing, dropped.hlc) {
                txn.abort()?;
                return Ok(SweepEnd { removed, survived: true });
            }
            if !started {
                started = true;
                debug!(
                    db,
                    collection = name,
                    stamp = ?dropped,
                    "a drop reached a newer life of the collection; removing what it holds of the \
                     life before"
                );
            }
            let chunk = self.remove_stragglers(&txn, &standing, dropped, after)?;
            removed += chunk.removed;
            skipped += chunk.skipped;
            first_arrival = match (first_arrival, chunk.first_arrival) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            let mut buried = None;
            if chunk.exhausted {
                // The floor, in the commit of the last chunk: it says the
                // sweep is done, and there is no instant at which the records
                // are gone and a straggler at the drop's own `Hlc` is refused
                // by neither the tombstone (strict) nor the floor.
                //
                // A life a drop re-dated that holds nothing live after the
                // sweep, and no definition stamped after the drop, has nothing
                // the drop kept: it is buried, in this commit, with its shadow.
                let live = txn
                    .open_table(tables::LIVE_COUNTS)?
                    .get(standing.id.0)?
                    .map_or(0, |count| count.value())
                    .saturating_sub(chunk.removed as u64);
                let redated = standing.is_redated();
                // A shadow is the parent's: while the parent stands with a vector
                // configuration the shadow stands too, empty if need be.
                let configured_parent = match kimmy_core::vector_meta::base_name(&standing.name) {
                    Some(base) => {
                        read_collection(&txn, db, base)?.is_some_and(|p| p.vector.is_some())
                    }
                    None => false,
                };
                if redated
                    && live == 0
                    && !configured_parent
                    && !holds_a_definition_stamped_after(&standing, dropped.hlc)
                {
                    survived = false;
                    buried = Some(self.bury_redated_in_txn(&txn, &standing, dropped)?);
                } else {
                    if redated && let Some(marker) = standing.redated.as_mut() {
                        marker.first_arrival = match (first_arrival, marker.arrivals_from) {
                            (Some(seen), Some(seed)) => Some(seen.min(seed)),
                            (seen, seed) => seen.or(seed),
                        };
                    }
                    standing.incarnation_floor = Some(dropped.hlc);
                    Engine::put_collection_meta(&txn, &standing)?;
                }
            }
            txn.commit()?;
            #[cfg(test)]
            hooks::after_a_chunk();
            if let Some(burial) = buried {
                let ids = self.finish_burial(db, name, burial);
                crate::database_tombstone::Notify::Each.buried(self, db, name, &ids);
            }
            if kimmy_core::vector_meta::is_shadow(&standing.name) && chunk.removed > 0 {
                self.bump_vector_generation(standing.id);
            }
            if chunk.exhausted {
                break;
            }
            after = chunk.last;
        }
        if skipped > 0 {
            warn!(
                db,
                collection = name,
                records = skipped,
                "records of the life before a drop have a header that does not decode and were \
                 left where they are"
            );
        }
        if removed > 0 {
            info!(
                db,
                collection = name,
                records = removed,
                "removed what a collection held of the life before a drop that reached it late"
            );
        }
        Ok(SweepEnd { removed, survived })
    }

    /// The definitions a newer life took in of the one before: the indexes
    /// created at or before the drop (their entries go with them, the drop's
    /// tombstone is recorded for each) and a vector configuration changed at or
    /// before it, as a peer's drop of each at the drop's stamp would have done.
    /// An index with no `created` stamp (one made before the stamp existed) is
    /// left: it cannot be told from the newer life's, and a newer life's
    /// indexes carry a stamp. The records come after.
    fn sweep_late_definitions(&self, db: &str, name: &str, dropped: Stamp) -> Result<()> {
        let Ok(standing) = self.get_collection(db, name) else { return Ok(()) };
        if !owes_a_sweep(&standing, dropped.hlc) {
            return Ok(());
        }
        for index in &standing.indexes {
            if index.created.is_some_and(|created| created.hlc <= dropped.hlc) {
                self.drop_index_inner(db, name, &index.name, Some(dropped))?;
            }
        }
        if standing.vector.is_some()
            && standing.vector_changed.is_some_and(|changed| changed.hlc <= dropped.hlc)
        {
            self.disable_vectors_inner(
                db,
                name,
                false,
                crate::vectors::Configured::FromEntry(dropped),
                &|_| false,
            )?;
        }
        Ok(())
    }

    /// One chunk: examine the collection's records after `after`, and replace
    /// each live one stamped at or before the drop with a tombstone at the
    /// drop's stamp.
    ///
    /// A tombstone with no oplog entry behind it, as a snapshot's carried
    /// delete is (ADR-228): the count, the digest (and its `bare_max`) and the
    /// index entries move in this transaction. A stream that saw the
    /// straggler's insert is not sent a delete: there is no entry to send. A
    /// record whose header does not decode is left (counted, warned by the
    /// caller); one whose body does not decode is tombstoned without index
    /// maintenance, as nothing can be said of the entries it made.
    fn remove_stragglers(
        &self,
        txn: &crate::engine::WriteTxn<'_>,
        standing: &CollectionMeta,
        dropped: Stamp,
        after: Option<Vec<u8>>,
    ) -> Result<Chunk> {
        // Bounded by SWEEP_CHUNK rows or SWEEP_CHUNK_BYTES examined; the stop
        // is looked for between chunks (walks_stop: a bounded read).
        let mut found: Vec<(Vec<u8>, Option<bson::Document>)> = Vec::new();
        let (mut examined, mut bytes, mut skipped) = (0usize, 0usize, 0usize);
        let mut last = after.clone();
        let mut exhausted = true;
        let mut first_arrival: Option<u64> = None;
        // A shadow row is kept only while its parent record is live: a chunk the
        // embedding worker stamped after the drop for a parent that was swept
        // must not keep the shadow (and with it a parentless row) alive. The
        // worker already refuses a swept parent in its own writer scope
        // (`document_stamp`); this is the same test for what it wrote before.
        let parent = kimmy_core::vector_meta::base_name(&standing.name)
            .map(|base| CollectionId::derive(&standing.db, base));
        // The entries of what the life keeps, by arrival, for a re-dated one: the
        // stream that opens without a token begins there and not at a walk of
        // the log. Tombstones stamped after the drop count: a stream that missed
        // a delete would show a document the life no longer has.
        let arrivals = match standing.is_redated() {
            true => Some(txn.open_table(tables::OPLOG_ARRIVAL_SEQ)?),
            false => None,
        };
        {
            let docs = txn.open_table(tables::DOCS)?;
            for row in docs.range(doc_range_after(standing.id, after.as_deref()))? {
                if examined == SWEEP_CHUNK || bytes >= SWEEP_CHUNK_BYTES {
                    exhausted = false;
                    break;
                }
                let (key, value) = row?;
                examined += 1;
                bytes += key.value().1.len() + value.value().len();
                last = Some(key.value().1.to_vec());
                let Ok((stamp, live)) = codec::doc_record_header(value.value()) else {
                    skipped += 1;
                    continue;
                };
                if stamp.hlc > dropped.hlc
                    && let Some(arrivals) = &arrivals
                    && let Some(seq) = arrivals.get(codec::oplog_key(&stamp).as_slice())?
                {
                    let seq = seq.value();
                    first_arrival = Some(first_arrival.map_or(seq, |first| first.min(seq)));
                }
                if !live {
                    continue;
                }
                let orphaned = stamp.hlc > dropped.hlc
                    && parent.is_some_and(|parent| {
                        crate::vectors::chunk_has_no_live_parent(
                            &docs,
                            parent,
                            dropped.hlc,
                            value.value(),
                        )
                    });
                if stamp.hlc <= dropped.hlc || orphaned {
                    let document = codec::decode_doc_record(value.value())
                        .and_then(|record| Ok(record.document()?))
                        .unwrap_or_else(|e| {
                            warn!(error = %e, "a record of the life before a drop does not decode; removed without its index entries");
                            None
                        });
                    found.push((key.value().1.to_vec(), document));
                }
            }
        }
        let tombstone = codec::encode_doc_record(&kimmy_core::DocRecord::tombstone(dropped));
        let mut docs = txn.open_table(tables::DOCS)?;
        for (key, _) in &found {
            crate::live_count::put_bare_tombstone(txn, &mut docs, standing.id.0, key, &tombstone)?;
        }
        drop(docs);
        for (key, previous) in &found {
            // A removal has no document to key and so no violation to report.
            crate::index::maintain_remote(self, txn, standing, previous.as_ref(), None, key)?;
        }
        Ok(Chunk { removed: found.len(), skipped, last, exhausted, first_arrival })
    }

    /// A replicated drop of the collection `db.name` has reached a member whose
    /// life began at or before it (ADR-231, extended; ruled by Chris,
    /// 2026-10-13): **keep what is stamped after the drop**.
    ///
    /// Only an explicit *collection* drop takes this path. A database's drop
    /// does not (ADR-229): a life **created at or before the database's own
    /// tombstone** is buried with what was written into it afterwards, whichever
    /// route the collection's drop came by, and a life created after it (a
    /// database dropped and made again) keeps like any other. That one rule is
    /// the guard on every path, and it is judged inside the transaction below.
    /// A database tombstone that arrives later finds a re-dated life by
    /// [`Redated::from`](crate::meta::Redated::from) and buries it the same way.
    ///
    /// **One write transaction decides** (D-2), the same that buries on the fast
    /// path, so a write stamped after the drop cannot land between the decision
    /// and the burial and be lost though acknowledged:
    ///
    /// - *Fast path.* The collection's kept digest row is present, every
    ///   origin's highest stamp (`maxw`, which includes tombstones and is never
    ///   lowered) is at or below the drop, and no definition (index, vector
    ///   configuration) is stamped after it. Buried and purged as ever
    ///   (ADR-158, ADR-189), nothing read. An absent row is *unknown*, not
    ///   "nothing above" (the walk of an empty collection is cheap). A stale
    ///   `maxw` errs toward the survivors path and is never wrong.
    /// - *Survivors path.* The first commit, in that transaction, records the
    ///   tombstones of the collection and its shadow and **re-dates both** to
    ///   just after the drop (D-1). The life is now what ADR-231 calls a newer
    ///   life that owes a sweep, so a stop, a crash or a redelivery of the drop
    ///   is resumed by what already resumes that: the start's reconcile and
    ///   `sweep_after_an_ignored_drop`. The sweep ([`Self::sweep_until`]) takes the
    ///   floor in its last commit, and buries a life left with nothing in the
    ///   same commit.
    ///
    /// Open streams on the collection end at the first commit, as at a burial.
    pub(crate) fn drop_keeping_later_writes(
        &self,
        db: &str,
        name: &str,
        dropped: Stamp,
        stop: StopCheck<'_>,
    ) -> Result<Dropped> {
        let shadow_name = kimmy_core::vector_meta::shadow_name(name);
        if kimmy_core::vector_meta::is_shadow(name) {
            return Ok(Dropped::Buried(self.drop_collection_inner(db, name, Some(dropped))?));
        }
        // Nothing to bury, answered without taking the writer. Only that: what is
        // buried is read again under the writer below.
        match self.get_collection(db, name) {
            Ok(_) => {}
            Err(crate::StorageError::Core(kimmy_core::Error::CollectionNotFound { .. })) => {
                return Ok(Dropped::Buried(false));
            }
            Err(e) => return Err(e),
        }
        #[cfg(test)]
        crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::Burial);
        let txn = self.begin_write_as(WriterHolder::Ddl, crate::engine::Continuing::No)?;
        let standing = read_collection(&txn, db, name)?
            .filter(|meta| !crate::sync::aims_at_a_previous_incarnation(meta, dropped.hlc));
        let Some(mut meta) = standing else {
            // Buried, or buried and recreated, before the drop took the writer:
            // nothing of what stands now is this drop's to bury.
            txn.abort()?;
            #[cfg(test)]
            crate::sync::race_hooks::absorbed(crate::sync::race_hooks::Race::Burial);
            return Ok(Dropped::Buried(false));
        };
        let mut shadow = read_collection(&txn, db, &shadow_name)?;
        let covered =
            database_tombstone_in(&txn, db)?.is_some_and(|stamp| meta.created <= stamp.hlc);
        if covered || self.nothing_is_stamped_after(&txn, &meta, dropped.hlc)? {
            let burial = self.bury_standing_in_txn(&txn, db, name, meta, shadow, Some(dropped))?;
            txn.commit()?;
            let ids = self.finish_burial(db, name, burial);
            crate::database_tombstone::Notify::Each.buried(self, db, name, &ids);
            return Ok(Dropped::Buried(true));
        }
        let ids: Vec<CollectionId> =
            [Some(meta.id), shadow.as_ref().map(|s| s.id)].into_iter().flatten().collect();
        {
            let mut tombstones = txn.open_table(tables::COLLECTIONS_DROPPED)?;
            for id in &ids {
                let newer = match tombstones.get(id.0)? {
                    Some(existing) => dropped > codec::decode_oplog_key(existing.value())?,
                    None => true,
                };
                if newer {
                    tombstones.insert(id.0, codec::oplog_key(&dropped).as_slice())?;
                }
            }
        }
        // Nothing written after this commit has its entry below this position,
        // whatever range of the collection the sweep has already passed.
        let next = next_arrival_in(&txn)?;
        redate(&mut meta, dropped.hlc);
        if let Some(marker) = meta.redated.as_mut() {
            marker.arrivals_from = Some(next);
        }
        Engine::put_collection_meta(&txn, &meta)?;
        if let Some(shadow) = shadow.as_mut() {
            redate(shadow, dropped.hlc);
            Engine::put_collection_meta(&txn, shadow)?;
        }
        txn.commit()?;
        #[cfg(test)]
        hooks::took_the_survivors_path();
        crate::database_tombstone::Notify::Each.buried(self, db, name, &ids);
        let end = self.sweep_until(db, name, dropped, stop)?;
        if end.survived && self.get_collection(db, &shadow_name).is_ok() {
            self.sweep_until(db, &shadow_name, dropped, stop)?;
        }
        info!(
            db,
            collection = name,
            stamp = ?dropped,
            kept = end.survived,
            "a drop reached a collection holding writes stamped after it; kept them"
        );
        Ok(if end.survived { Dropped::Kept } else { Dropped::Buried(true) })
    }

    /// Whether a collection holds nothing stamped after `dropped`, read through
    /// the transaction that would bury it.
    fn nothing_is_stamped_after(
        &self,
        txn: &crate::engine::WriteTxn<'_>,
        meta: &CollectionMeta,
        dropped: Hlc,
    ) -> Result<bool> {
        if holds_a_definition_stamped_after(meta, dropped) {
            return Ok(false);
        }
        let rows = txn.open_table(tables::LIVE_DIGESTS)?;
        let Some(row) = crate::live_digest::kept_row(&rows, meta.id.0)? else {
            return Ok(false);
        };
        Ok(row.maxw.values().all(|stamp| *stamp <= dropped))
    }

    /// Bury a re-dated life that the drop which re-dated it, or a database's
    /// drop that has reached it since, leaves nothing of: the collection and its
    /// shadow, in the caller's transaction. The incarnation rule that turns a
    /// drop away from a life created above it is the very thing being undone,
    /// so this is a mode of its own and not `bury_in_txn`.
    pub(crate) fn bury_redated_in_txn(
        &self,
        txn: &crate::engine::WriteTxn<'_>,
        standing: &CollectionMeta,
        dropped: Stamp,
    ) -> Result<crate::engine::Burial> {
        let shadow = match kimmy_core::vector_meta::is_shadow(&standing.name) {
            true => None,
            false => read_collection(
                txn,
                &standing.db,
                &kimmy_core::vector_meta::shadow_name(&standing.name),
            )?,
        };
        self.bury_standing_in_txn(
            txn,
            &standing.db,
            &standing.name,
            standing.clone(),
            shadow,
            Some(dropped),
        )
    }

    /// A database's tombstone `dropped` covers the re-dated life `standing` if
    /// the life was created at or before it before the drop re-dated it: it is
    /// buried with the database, the writes it kept with it (ADR-229). In its
    /// own transaction; `None` where the life was already buried or recreated.
    pub(crate) fn bury_redated_under_database(
        &self,
        db: &str,
        name: &str,
        dropped: Stamp,
    ) -> Result<Option<Vec<CollectionId>>> {
        let txn = self.begin_write_as(WriterHolder::Ddl, crate::engine::Continuing::No)?;
        let Some(standing) = read_collection(&txn, db, name)? else {
            txn.abort()?;
            return Ok(None);
        };
        if !standing.redated.is_some_and(|r| r.created == standing.created && r.from <= dropped.hlc)
        {
            txn.abort()?;
            return Ok(None);
        }
        let burial = self.bury_redated_in_txn(&txn, &standing, dropped)?;
        txn.commit()?;
        Ok(Some(self.finish_burial(db, name, burial)))
    }

    /// A drop was judged aimed at a life of `current` that has already ended
    /// ([`crate::sync::aims_at_a_previous_incarnation`]): sweep what the newer
    /// life holds of the one before, in the collection and in its vector shadow.
    pub(crate) fn sweep_after_an_ignored_drop(
        &self,
        current: &CollectionMeta,
        dropped: Stamp,
    ) -> Result<()> {
        self.sweep_late_drop(&current.db, &current.name, dropped)?;
        if !kimmy_core::vector_meta::is_shadow(&current.name) {
            let shadow = kimmy_core::vector_meta::shadow_name(&current.name);
            if self.get_collection(&current.db, &shadow).is_ok() {
                self.sweep_late_drop(&current.db, &shadow, dropped)?;
            }
        }
        Ok(())
    }

    /// At a start: sweep every collection that owes one for a tombstone this
    /// member holds, so a store that was left with stragglers (a drop applied
    /// before the sweep existed, or a stop in the middle of one) is healed
    /// before it serves. The floor each sweep sets is the marker that it was
    /// done, so a store with nothing owed pays one pass over the collection
    /// list. A stop ends it between two chunks and the next start continues.
    pub fn reconcile_late_drops(&self) -> Result<usize> {
        use crate::open_progress::{OpenPhase, open_stop_requested, set_open_phase};

        let tombstones: std::collections::HashMap<_, _> =
            self.collections_dropped()?.into_iter().collect();
        let owed: Vec<(CollectionMeta, Stamp)> = self
            .all_collections()?
            .into_iter()
            .filter_map(|standing| {
                let dropped = *tombstones.get(&standing.id)?;
                owes_a_sweep(&standing, dropped.hlc).then_some((standing, dropped))
            })
            .collect();
        if owed.is_empty() {
            return Ok(0);
        }
        let before = crate::open_progress::open_snapshot().phase;
        crate::open_progress::set_open_phase_of(OpenPhase::Reconciling, owed.len() as u64);
        let mut removed = 0usize;
        let result = (|| {
            for (n, (standing, dropped)) in owed.iter().enumerate() {
                if open_stop_requested() {
                    return Err(crate::error::StorageError::Stopping(crate::StopReason::Shutdown));
                }
                crate::open_progress::set_open_done(n as u64);
                removed +=
                    self.sweep_late_drop_until(&standing.db, &standing.name, *dropped, &|| {
                        // A SIGTERM at a start sets only the open's stop.
                        if open_stop_requested() {
                            return Err(crate::error::StorageError::Stopping(
                                crate::StopReason::Shutdown,
                            ));
                        }
                        self.check_walk(crate::WalkScope::Background)
                    })?;
            }
            Ok(())
        })();
        set_open_phase(before);
        result?;
        info!(
            collections = owed.len(),
            records = removed,
            "swept what newer lives held of the lives before their drops, before serving"
        );
        Ok(removed)
    }
}

/// The arrival position the next entry appended to this node's log will take.
fn next_arrival_in(txn: &redb::WriteTransaction) -> Result<u64> {
    let arrival = txn.open_table(tables::OPLOG_ARRIVAL)?;
    Ok(arrival.last()?.map_or(0, |(seq, _)| seq.value() + 1))
}

/// What a replicated collection drop did to the collection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Dropped {
    /// Buried (`true`), or nothing of the drop's to bury (`false`).
    Buried(bool),
    /// Writes stamped after the drop stand: the life was re-dated and swept.
    Kept,
}

/// Move a life that a drop met writes after to just above the drop, remembering
/// where it was and that it was re-dated. A second drop over a life already
/// re-dated keeps the first `from`: it is the creation a database's drop is
/// judged against.
fn redate(meta: &mut CollectionMeta, dropped: Hlc) {
    let from = meta.redated.filter(|r| r.created == meta.created).map_or(meta.created, |r| r.from);
    meta.created = dropped.successor();
    meta.redated = Some(crate::meta::Redated {
        from,
        created: meta.created,
        first_arrival: None,
        arrivals_from: None,
    });
}

/// Whether a definition of `standing` is stamped after `dropped`: an index, or
/// a vector configuration. One with no stamp counts as not after, the rule the
/// sweep applies to it.
fn holds_a_definition_stamped_after(standing: &CollectionMeta, dropped: Hlc) -> bool {
    standing.indexes.iter().any(|index| index.created.is_some_and(|c| c.hlc > dropped))
        || standing.vector_changed.is_some_and(|changed| changed.hlc > dropped)
}

/// The tombstone of `db`'s drop as `txn` sees it.
fn database_tombstone_in(txn: &redb::WriteTransaction, db: &str) -> Result<Option<Stamp>> {
    let tombstones = txn.open_table(tables::COLLECTIONS_DROPPED)?;
    match tombstones.get(kimmy_core::database_drop_id(db).0)? {
        Some(raw) => Ok(Some(codec::decode_oplog_key(raw.value())?)),
        None => Ok(None),
    }
}

/// The collection `db.name` as `txn` sees it.
fn read_collection(
    txn: &redb::WriteTransaction,
    db: &str,
    name: &str,
) -> Result<Option<CollectionMeta>> {
    let collections = txn.open_table(tables::COLLECTIONS)?;
    match collections.get((db, name))? {
        Some(value) => Ok(Some(serde_json::from_slice(value.value())?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use kimmy_core::{DocId, Hlc, NodeId, Stamp};

    use super::*;
    use crate::live_count::test_support::assert_counts_exact;
    use crate::{Engine, WalkScope};

    const BATCH: usize = 1_024;

    fn engine() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
    }

    fn pull(into: &Engine, from: &Engine) {
        let mine = into.version_vector().unwrap();
        let theirs = from.version_vector().unwrap();
        if let Some(start) = mine.behind(&theirs) {
            let entries =
                from.entries_for_peer(start, BATCH, WalkScope::Background).unwrap().entries;
            into.apply_batch(&entries).unwrap();
        }
    }

    fn sync(a: &Engine, b: &Engine) {
        pull(a, b);
        pull(b, a);
    }

    fn quiesce(engines: &[&Engine]) {
        for _ in 0..4 {
            for (i, x) in engines.iter().enumerate() {
                for y in &engines[i + 1..] {
                    sync(x, y);
                }
            }
            for e in engines {
                e.finish_purges_now().unwrap();
            }
        }
    }

    fn docs_of(e: &Engine, db: &str, name: &str, ids: &[i64]) -> Vec<i64> {
        let coll = e.get_collection(db, name).unwrap();
        ids.iter().copied().filter(|n| e.get(&coll, &DocId::Int64(*n)).unwrap().is_some()).collect()
    }

    /// A creates and drops; B, which knew nothing, creates the name afterwards.
    /// Entries arrive in stamp order, so A's document reaches B before A's
    /// drop does. The drop is aimed at a life that was over here (it predates
    /// B's creation) and is ignored: and the document of the life before stays
    /// in B's.
    fn the_race() -> (Engine, Engine, tempfile::TempDir, tempfile::TempDir) {
        let (a, da) = engine();
        let (b, db) = engine();
        let on_a = a.create_collection("shop", "orders").unwrap();
        a.insert(&on_a, doc! { "_id": 1i64, "from": "a" }).unwrap();
        assert!(a.drop_collection("shop", "orders").unwrap());
        a.finish_purges_now().unwrap();
        let on_b = b.create_collection("shop", "orders").unwrap();
        assert!(on_b.created > on_a.created, "premise: B's creation is the later");
        b.insert(&on_b, doc! { "_id": 2i64, "from": "b" }).unwrap();
        (a, b, da, db)
    }

    /// The reproducer of the finding: both members end with the same documents,
    /// and B's floor stands at A's drop, which is also what says the sweep was
    /// done.
    #[test]
    fn a_drop_that_reaches_a_newer_life_removes_what_that_life_took_in_of_the_one_before() {
        let (a, b, _da, _db) = the_race();
        quiesce(&[&a, &b]);
        assert_eq!(docs_of(&a, "shop", "orders", &[1, 2]), vec![2]);
        assert_eq!(docs_of(&b, "shop", "orders", &[1, 2]), vec![2], "the straggler is gone");
        for e in [&a, &b] {
            assert_eq!(e.count_by_id(CollectionId::derive("shop", "orders")).unwrap(), Some(1));
            assert_counts_exact(e, "a late drop");
        }
        let dropped = *b
            .collection_tombstones()
            .unwrap()
            .get(&CollectionId::derive("shop", "orders"))
            .unwrap();
        let floor = b.get_collection("shop", "orders").unwrap().incarnation_floor;
        assert_eq!(floor, Some(dropped), "the floor is taken up to the drop");
    }

    use kimmy_core::CollectionId;

    /// Nothing is swept for a life the drop did not end: a document of the
    /// newer life stays, and so does one that arrives after, and a second
    /// delivery of the drop finds nothing owed.
    #[test]
    fn the_sweep_keeps_what_the_newer_life_wrote_and_is_done_once() {
        let (a, b, _da, _db) = the_race();
        quiesce(&[&a, &b]);
        let on_b = b.get_collection("shop", "orders").unwrap();
        b.insert(&on_b, doc! { "_id": 5i64 }).unwrap();
        quiesce(&[&a, &b]);
        assert_eq!(docs_of(&a, "shop", "orders", &[1, 2, 5]), vec![2, 5]);
        assert_eq!(docs_of(&b, "shop", "orders", &[1, 2, 5]), vec![2, 5]);
        let id = CollectionId::derive("shop", "orders");
        let dropped = *b.collection_tombstones().unwrap().get(&id).unwrap();
        let dropped = Stamp::new(dropped, a.node_id());
        assert!(!owes_a_sweep(&b.get_collection("shop", "orders").unwrap(), dropped.hlc));
        assert_eq!(
            b.sweep_late_drop("shop", "orders", dropped).unwrap(),
            0,
            "nothing is owed twice"
        );
    }

    /// A member D that still holds the old life writes Z after the drop's stamp,
    /// then receives the drop. The drop removes what is stamped at or before it
    /// and keeps Z (ADR-231, extended): D keeps its collection, re-dated to just
    /// after the drop, with Z; and A and B, whose lives are newer, keep Z too.
    /// Everyone ends with {2, 3}; document 1 is gone from all three.
    #[test]
    fn a_drop_keeps_the_write_stamped_after_it_on_the_member_that_took_it() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let (d, _dd) = engine();
        let on_a = a.create_collection("shop", "orders").unwrap();
        a.insert(&on_a, doc! { "_id": 1i64 }).unwrap();
        sync(&a, &d);
        assert!(a.drop_collection("shop", "orders").unwrap());
        a.finish_purges_now().unwrap();
        let on_d = d.get_collection("shop", "orders").unwrap();
        d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
        let on_b = b.create_collection("shop", "orders").unwrap();
        b.insert(&on_b, doc! { "_id": 2i64 }).unwrap();
        quiesce(&[&a, &b, &d]);
        for (who, e) in [("a", &a), ("b", &b), ("d", &d)] {
            assert_eq!(docs_of(e, "shop", "orders", &[1, 2, 3]), vec![2, 3], "{who}");
            assert_counts_exact(e, who);
        }
    }

    /// Nobody recreated the name: D keeps document 3 in a collection the drop
    /// removed on A, re-dated above the drop and floored at it. A has no
    /// collection to put the write in, so the write waits there as history; the
    /// existence repair (the loop test) brings the collection back.
    #[test]
    fn a_drop_that_meets_a_later_write_keeps_the_collection_re_dated_above_the_drop() {
        let (a, _da) = engine();
        let (d, _dd) = engine();
        let on_a = a.create_collection("shop", "orders").unwrap();
        a.insert(&on_a, doc! { "_id": 1i64 }).unwrap();
        sync(&a, &d);
        assert!(a.drop_collection("shop", "orders").unwrap());
        a.finish_purges_now().unwrap();
        let on_d = d.get_collection("shop", "orders").unwrap();
        d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
        quiesce(&[&a, &d]);
        let dropped = a.collection_dropped_at(on_a.id).unwrap().unwrap();
        let kept = d.get_collection("shop", "orders").unwrap();
        assert_eq!(docs_of(&d, "shop", "orders", &[1, 3]), vec![3]);
        assert!(kept.created > dropped.hlc, "re-dated above the drop: {:?}", kept.created);
        assert_eq!(kept.incarnation_floor, Some(dropped.hlc));
        assert!(a.get_collection("shop", "orders").is_err(), "A has nothing to put it in");
    }

    /// A replicated drop that meets nothing stamped after it buries the
    /// collection and purges it as it always did: no collection, no rows, no
    /// floor, counts exact.
    #[test]
    fn a_replicated_drop_that_meets_nothing_later_buries_as_before() {
        let (a, _da) = engine();
        let (d, _dd) = engine();
        let on_a = a.create_collection("shop", "orders").unwrap();
        a.insert(&on_a, doc! { "_id": 1i64 }).unwrap();
        sync(&a, &d);
        assert!(a.drop_collection("shop", "orders").unwrap());
        quiesce(&[&a, &d]);
        assert!(d.get_collection("shop", "orders").is_err());
        assert_eq!(d.rows_under(on_a.id).unwrap(), 0);
        assert_counts_exact(&d, "a buried collection");
    }

    /// A database drop is not changed: a collection that holds a write stamped
    /// after it is buried with it, as ADR-229 documents.
    #[test]
    fn a_database_drop_still_buries_a_collection_holding_a_later_write() {
        let (a, _da) = engine();
        let (d, _dd) = engine();
        let on_a = a.create_collection("shop", "orders").unwrap();
        a.insert(&on_a, doc! { "_id": 1i64 }).unwrap();
        a.create_collection("shop", "items").unwrap();
        sync(&a, &d);
        a.drop_database("shop").unwrap();
        a.finish_purges_now().unwrap();
        let on_d = d.get_collection("shop", "orders").unwrap();
        d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
        quiesce(&[&a, &d]);
        assert!(d.get_collection("shop", "orders").is_err(), "buried with the database");
        assert!(d.get_collection("shop", "items").is_err());
        assert_counts_exact(&d, "a database drop");
    }

    /// D-3. A whole-database snapshot page carries the database's drop, and the
    /// collection tombstones a database drop writes, at the database drop's own
    /// stamp. The snapshot route cannot tell those from explicit collection
    /// drops, and a database survivor never keeps a write (ADR-229): the
    /// collection is buried whole, as the entry route buries it.
    #[test]
    fn a_whole_database_page_buries_a_collection_holding_a_later_write() {
        let (a, _da) = engine();
        let (d, _dd) = engine();
        let on_a = a.create_collection("shop", "orders").unwrap();
        a.insert(&on_a, doc! { "_id": 1i64 }).unwrap();
        a.create_collection("shop", "items").unwrap();
        sync(&a, &d);
        a.drop_database("shop").unwrap();
        a.finish_purges_now().unwrap();
        let on_d = d.get_collection("shop", "orders").unwrap();
        d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
        let page = a.snapshot_page(None, None).unwrap();
        assert!(
            page.dropped_collections.iter().any(|(id, _)| *id == on_a.id),
            "premise: the page carries the collection's tombstone"
        );
        d.apply_snapshot_page(a.node_id(), &mut crate::SnapshotProgress::whole_database(), &page)
            .unwrap();
        d.finish_purges_now().unwrap();
        assert!(d.get_collection("shop", "orders").is_err(), "buried with the database");
        assert!(d.get_collection("shop", "items").is_err());
        assert_counts_exact(&d, "a database drop by page");
    }

    /// D-3, the scoped route with the database's tombstone held: the collection
    /// was buried by the database's drop, with a drop of its own above the
    /// database's stamp, and that does not keep a later write.
    #[test]
    fn a_scoped_page_at_the_databases_own_stamp_buries_a_collection_holding_a_later_write() {
        let (a, _da) = engine();
        let (d, _dd) = engine();
        let on_a = a.create_collection("shop", "orders").unwrap();
        a.insert(&on_a, doc! { "_id": 1i64 }).unwrap();
        a.create_collection("shop", "items").unwrap();
        sync(&a, &d);
        a.drop_database("shop").unwrap();
        a.finish_purges_now().unwrap();
        let on_d = d.get_collection("shop", "orders").unwrap();
        d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
        // D learns of the database's drop first (its tombstone), then the scoped
        // repair of `orders` carries the collection's tombstone at the same stamp.
        let db_drop = a.database_dropped_at("shop").unwrap().unwrap();
        d.record_database_tombstone_for_test(kimmy_core::database_drop_id("shop"), db_drop)
            .unwrap();
        let page = a.snapshot_page(None, Some(on_a.id)).unwrap();
        assert!(page.dropped.is_some_and(|at| at >= db_drop), "premise: the page carries the drop");
        d.apply_snapshot_page(
            a.node_id(),
            &mut crate::SnapshotProgress::of_collection(on_a.id),
            &page,
        )
        .unwrap();
        d.finish_purges_now().unwrap();
        assert!(d.get_collection("shop", "orders").is_err(), "buried with the database");
    }

    /// D-3, the other side: the scoped repair of a collection the sender dropped
    /// by a drop of its own, in a database that stands, keeps what the receiver
    /// wrote after it, as the entry route does.
    #[test]
    fn a_scoped_page_of_an_explicit_collection_drop_keeps_a_later_write() {
        let (a, _da) = engine();
        let (d, _dd) = engine();
        let on_a = a.create_collection("shop", "orders").unwrap();
        a.insert(&on_a, doc! { "_id": 1i64 }).unwrap();
        sync(&a, &d);
        assert!(a.drop_collection("shop", "orders").unwrap());
        a.finish_purges_now().unwrap();
        let on_d = d.get_collection("shop", "orders").unwrap();
        d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
        let page = a.snapshot_page(None, Some(on_a.id)).unwrap();
        assert!(page.dropped.is_some(), "premise: the page carries the drop");
        d.apply_snapshot_page(
            a.node_id(),
            &mut crate::SnapshotProgress::of_collection(on_a.id),
            &page,
        )
        .unwrap();
        assert_eq!(docs_of(&d, "shop", "orders", &[1, 3]), vec![3]);
        assert_counts_exact(&d, "a scoped page that kept");
    }

    /// A stop between two chunks of the survivors path leaves the collection
    /// re-dated by the first commit, its floor not taken, and the start's
    /// reconcile finishes it (D-1).
    #[test]
    fn the_survivors_path_stops_between_chunks_and_the_drop_applied_again_finishes() {
        let n = SWEEP_CHUNK * 2 + 17;
        let (b, _dir, id, dropped) = a_store_holding_stragglers(n, 0);
        // The collection holds a record stamped after the drop, and its life
        // began before it: the life the drop is aimed at.
        let mut meta = b.get_collection("shop", "orders").unwrap();
        meta.created = Hlc::new(dropped.hlc.wall_ms - 100, 0);
        {
            let txn = b.begin_write(WriterHolder::Ddl).unwrap();
            Engine::put_collection_meta(&txn, &meta).unwrap();
            txn.commit().unwrap();
        }
        let later = Stamp::new(Hlc::new(dropped.hlc.wall_ms + 5, 0), dropped.node);
        {
            let key = crate::docs::doc_key(&DocId::Int64(1_000_000)).unwrap();
            let body = bson::serialize_to_vec(&doc! { "_id": 1_000_000i64 }).unwrap();
            let txn = b.begin_write(WriterHolder::Replication).unwrap();
            let mut docs = txn.open_table(tables::DOCS).unwrap();
            crate::live_count::put_record(
                &txn,
                &mut docs,
                id.0,
                &key,
                &codec::encode_doc_record(&kimmy_core::DocRecord::live(later, body)),
            )
            .unwrap();
            drop(docs);
            txn.commit().unwrap();
        }
        let asked = std::cell::Cell::new(0);
        let one_chunk = || {
            asked.set(asked.get() + 1);
            if asked.get() > 1 {
                Err(crate::StorageError::Stopping(crate::StopReason::Shutdown))
            } else {
                Ok(())
            }
        };
        assert!(matches!(
            b.drop_keeping_later_writes("shop", "orders", dropped, &one_chunk),
            Err(crate::StorageError::Stopping(_))
        ));
        // D-1: re-dated by the first commit, so the sweep is owed and a start
        // finds it; the floor, which says it is done, is not taken yet.
        let standing = b.get_collection("shop", "orders").unwrap();
        assert!(standing.created > dropped.hlc, "re-dated by the first commit");
        assert!(standing.is_redated());
        assert_eq!(standing.incarnation_floor, None);
        assert!(owes_a_sweep(&standing, dropped.hlc));
        assert_eq!(b.collection_dropped_at(id).unwrap(), Some(dropped));
        assert_eq!(b.count_by_id(id).unwrap(), Some((n - SWEEP_CHUNK + 1) as u64));

        assert!(b.reconcile_late_drops().unwrap() > 0, "the start finds it owed");
        let standing = b.get_collection("shop", "orders").unwrap();
        assert!(standing.created > dropped.hlc);
        assert_eq!(standing.incarnation_floor, Some(dropped.hlc));
        assert_eq!(b.count_by_id(id).unwrap(), Some(1));
        assert_counts_exact(&b, "the survivors path");
    }

    /// A second drop over a life already re-dated keeps the first `from`: it is
    /// the creation a database's drop is judged by. A marker left for another
    /// `created` (the life was made again since) starts over.
    #[test]
    fn a_second_re_date_keeps_the_first_creation() {
        let mut meta = CollectionMeta::new(
            CollectionId::derive("shop", "orders"),
            "shop",
            "orders",
            Hlc::new(10, 0),
            None,
        );
        redate(&mut meta, Hlc::new(100, 0));
        assert_eq!(meta.created, Hlc::new(100, 0).successor());
        assert_eq!(meta.redated.map(|r| r.from), Some(Hlc::new(10, 0)));
        redate(&mut meta, Hlc::new(200, 0));
        assert_eq!(meta.created, Hlc::new(200, 0).successor());
        assert_eq!(meta.redated.map(|r| r.from), Some(Hlc::new(10, 0)), "the first, not the last");
        meta.created = Hlc::new(300, 0);
        redate(&mut meta, Hlc::new(400, 0));
        assert_eq!(meta.redated.map(|r| r.from), Some(Hlc::new(300, 0)), "a new life starts over");
    }

    /// The rule's two clauses: a life owes a sweep only when it began after the
    /// drop and its floor is below it.
    #[test]
    fn a_life_owes_a_sweep_only_if_it_began_after_the_drop_and_its_floor_is_below_it() {
        let meta = |created: u64, floor: Option<u64>| {
            let mut meta = CollectionMeta::new(
                CollectionId::derive("shop", "orders"),
                "shop",
                "orders",
                Hlc::new(created, 0),
                floor.map(|f| Hlc::new(f, 0)),
            );
            meta.incarnation_floor = floor.map(|f| Hlc::new(f, 0));
            meta
        };
        let drop_at = Hlc::new(100, 0);
        assert!(owes_a_sweep(&meta(200, None), drop_at), "newer, no floor");
        assert!(owes_a_sweep(&meta(200, Some(50)), drop_at), "newer, floor below");
        assert!(!owes_a_sweep(&meta(200, Some(100)), drop_at), "floor at the drop: done");
        assert!(!owes_a_sweep(&meta(200, Some(150)), drop_at), "floor above the drop");
        assert!(!owes_a_sweep(&meta(100, None), drop_at), "created at the drop is not newer");
        assert!(!owes_a_sweep(&meta(50, None), drop_at), "older: the drop is aimed at it");
    }

    /// The vector shadow of a collection is swept with it.
    #[test]
    fn the_vector_shadow_is_swept_with_its_collection() {
        let (b, _db) = engine();
        let parent = b.create_collection("shop", "orders").unwrap();
        b.configure_vectors(
            "shop",
            "orders",
            kimmy_core::VectorConfig {
                fields: vec!["text".into()],
                provider: kimmy_core::ProviderConfig::Byo {},
                dim: 4,
                metric: Default::default(),
                document_prefix: None,
                query_prefix: None,
                chunk: Default::default(),
            },
        )
        .unwrap();
        let shadow =
            b.get_collection("shop", &kimmy_core::vector_meta::shadow_name("orders")).unwrap();
        let node = NodeId::from_bytes([7; 16]);
        let dropped = Stamp::new(Hlc::new(parent.created.wall_ms - 10, 0), node);
        let straggler = Stamp::new(Hlc::new(parent.created.wall_ms - 20, 0), node);
        {
            let key = crate::docs::doc_key(&DocId::Int64(1)).unwrap();
            let body = bson::serialize_to_vec(&doc! { "_id": 1i64 }).unwrap();
            let txn = b.begin_write(WriterHolder::Replication).unwrap();
            let mut docs = txn.open_table(tables::DOCS).unwrap();
            crate::live_count::put_record(
                &txn,
                &mut docs,
                shadow.id.0,
                &key,
                &codec::encode_doc_record(&kimmy_core::DocRecord::live(straggler, body)),
            )
            .unwrap();
            drop(docs);
            txn.commit().unwrap();
        }
        assert_eq!(b.count_by_id(shadow.id).unwrap(), Some(1), "premise");
        let generation = b.vector_generation(shadow.id);
        b.sweep_after_an_ignored_drop(&parent, dropped).unwrap();
        assert!(b.vector_generation(shadow.id) > generation, "the vector index was not told");
        assert_eq!(b.count_by_id(shadow.id).unwrap(), Some(0));
        for name in ["orders", shadow.name.as_str()] {
            assert_eq!(
                b.get_collection("shop", name).unwrap().incarnation_floor,
                Some(dropped.hlc),
                "{name}"
            );
        }
        assert_counts_exact(&b, "a shadow swept");
    }

    /// A database drop that reaches a survivor by the database's tombstone alone
    /// (no drop entry for the collection ever comes: the dropper never held it):
    /// the sweep of survivors removes what it took in of the life before.
    #[test]
    fn a_database_tombstone_alone_removes_what_a_survivor_took_in_of_the_life_before() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let (c, _dc) = engine();
        // C holds a life of `shop.orders` that A never receives.
        let on_c = c.create_collection("shop", "orders").unwrap();
        c.insert(&on_c, doc! { "_id": 9i64, "from": "c" }).unwrap();
        // A drops the database (it holds another collection of it).
        a.create_collection("shop", "items").unwrap();
        a.drop_database("shop").unwrap();
        a.finish_purges_now().unwrap();
        let on_b = b.create_collection("shop", "orders").unwrap();
        b.insert(&on_b, doc! { "_id": 2i64, "from": "b" }).unwrap();
        // C's life reaches B before the drop does.
        pull(&b, &c);
        assert_eq!(docs_of(&b, "shop", "orders", &[2, 9]), vec![2, 9], "premise");
        pull(&b, &a);
        b.finish_purges_now().unwrap();
        assert_eq!(docs_of(&b, "shop", "orders", &[2, 9]), vec![2], "the survivor lost document 9");
        assert_counts_exact(&b, "a survivor swept");
    }

    /// A database drop: a collection created after it stands, and what it took
    /// in of the life before, before it learned of the drop, goes.
    #[test]
    fn a_database_drop_removes_what_a_survivor_took_in_of_the_life_before() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let on_a = a.create_collection("shop", "orders").unwrap();
        a.insert(&on_a, doc! { "_id": 1i64, "from": "a" }).unwrap();
        a.drop_database("shop").unwrap();
        a.finish_purges_now().unwrap();
        let on_b = b.create_collection("shop", "orders").unwrap();
        b.insert(&on_b, doc! { "_id": 2i64, "from": "b" }).unwrap();
        quiesce(&[&a, &b]);
        assert_eq!(docs_of(&b, "shop", "orders", &[1, 2]), vec![2], "the survivor keeps its own");
        assert_eq!(docs_of(&a, "shop", "orders", &[1, 2]), vec![2]);
        assert_counts_exact(&b, "a database drop that reached a survivor late");
    }

    /// A store left with the stragglers (a drop applied before the sweep
    /// existed) is healed by the reconcile, once.
    #[test]
    fn the_reconcile_heals_a_store_built_with_the_bug() {
        let (b, _db) = engine();
        let id = CollectionId::derive("shop", "orders");
        let on_b = b.create_collection("shop", "orders").unwrap();
        b.insert(&on_b, doc! { "_id": 2i64 }).unwrap();
        // What the old rule left: a document of the life before, below a drop
        // this member holds, in a life created after it, and no floor.
        let node = NodeId::from_bytes([7; 16]);
        let dropped = Stamp::new(Hlc::new(on_b.created.wall_ms - 10, 0), node);
        let straggler = Stamp::new(Hlc::new(on_b.created.wall_ms - 20, 0), node);
        {
            let key = crate::docs::doc_key(&DocId::Int64(1)).unwrap();
            let body = bson::serialize_to_vec(&doc! { "_id": 1i64 }).unwrap();
            let record = kimmy_core::DocRecord::live(straggler, body);
            let txn = b.begin_write(WriterHolder::Replication).unwrap();
            let mut docs = txn.open_table(tables::DOCS).unwrap();
            crate::live_count::put_record(
                &txn,
                &mut docs,
                id.0,
                &key,
                &codec::encode_doc_record(&record),
            )
            .unwrap();
            drop(docs);
            txn.commit().unwrap();
        }
        b.record_collection_drop(id, dropped).unwrap();
        assert_eq!(docs_of(&b, "shop", "orders", &[1, 2]), vec![1, 2], "premise");
        assert!(owes_a_sweep(&b.get_collection("shop", "orders").unwrap(), dropped.hlc));

        assert_eq!(b.reconcile_late_drops().unwrap(), 1);
        assert_eq!(docs_of(&b, "shop", "orders", &[1, 2]), vec![2]);
        assert_eq!(
            b.get_collection("shop", "orders").unwrap().incarnation_floor,
            Some(dropped.hlc)
        );
        assert_counts_exact(&b, "the reconcile");
        assert_eq!(b.reconcile_late_drops().unwrap(), 0, "the floor says it was done");
    }

    /// Raw stragglers: live records of `count` documents stamped before a drop
    /// that this member holds, in a life created after it, with no floor.
    fn a_store_holding_stragglers(
        count: usize,
        pad: usize,
    ) -> (Engine, tempfile::TempDir, CollectionId, Stamp) {
        let (b, dir) = engine();
        let id = CollectionId::derive("shop", "orders");
        let on_b = b.create_collection("shop", "orders").unwrap();
        let node = NodeId::from_bytes([7; 16]);
        let dropped = Stamp::new(Hlc::new(on_b.created.wall_ms - 10, 0), node);
        let straggler = Stamp::new(Hlc::new(on_b.created.wall_ms - 20, 0), node);
        let padding = "x".repeat(pad);
        let txn = b.begin_write(WriterHolder::Replication).unwrap();
        let mut docs = txn.open_table(tables::DOCS).unwrap();
        for i in 0..count as i64 {
            let key = crate::docs::doc_key(&DocId::Int64(i)).unwrap();
            let body = bson::serialize_to_vec(&doc! { "_id": i, "pad": padding.as_str() }).unwrap();
            let record = kimmy_core::DocRecord::live(straggler, body);
            crate::live_count::put_record(
                &txn,
                &mut docs,
                id.0,
                &key,
                &codec::encode_doc_record(&record),
            )
            .unwrap();
        }
        drop(docs);
        txn.commit().unwrap();
        b.record_collection_drop(id, dropped).unwrap();
        assert_eq!(b.count_by_id(id).unwrap(), Some(count as u64), "premise");
        (b, dir, id, dropped)
    }

    /// A stop that lands between two chunks leaves exactly the chunks that
    /// committed, the floor unset, and the next call finishes: the chunk is the
    /// unit (a chunk size of a million, or a stop looked for before the first
    /// chunk only, would remove all of it or none).
    #[test]
    fn a_sweep_stops_between_chunks_and_the_next_call_finishes() {
        let n = SWEEP_CHUNK * 2 + 17;
        let (b, _dir, id, dropped) = a_store_holding_stragglers(n, 0);
        let asked = std::cell::Cell::new(0);
        let one_chunk = || {
            asked.set(asked.get() + 1);
            if asked.get() > 1 {
                Err(crate::StorageError::Stopping(crate::StopReason::Shutdown))
            } else {
                Ok(())
            }
        };
        assert!(matches!(
            b.sweep_late_drop_until("shop", "orders", dropped, &one_chunk),
            Err(crate::StorageError::Stopping(_))
        ));
        assert_eq!(
            b.count_by_id(id).unwrap(),
            Some((n - SWEEP_CHUNK) as u64),
            "exactly the one chunk that committed"
        );
        assert_eq!(b.get_collection("shop", "orders").unwrap().incarnation_floor, None);
        assert_counts_exact(&b, "a sweep stopped between chunks");

        assert_eq!(b.sweep_late_drop("shop", "orders", dropped).unwrap(), n - SWEEP_CHUNK);
        assert_eq!(b.count_by_id(id).unwrap(), Some(0));
        assert_eq!(
            b.get_collection("shop", "orders").unwrap().incarnation_floor,
            Some(dropped.hlc)
        );
        assert_counts_exact(&b, "a sweep across chunks");
    }

    /// A chunk is bounded by the bytes it examines as well as by its rows.
    #[test]
    fn a_chunk_is_bounded_by_bytes_too() {
        // Eleven records of a mebibyte: the chunk ends once it has examined
        // eight mebibytes, not after a thousand rows.
        let (b, _dir, id, dropped) = a_store_holding_stragglers(11, 1024 * 1024);
        let asked = std::cell::Cell::new(0);
        let one_chunk = || {
            asked.set(asked.get() + 1);
            if asked.get() > 1 {
                Err(crate::StorageError::Stopping(crate::StopReason::Shutdown))
            } else {
                Ok(())
            }
        };
        let _ = b.sweep_late_drop_until("shop", "orders", dropped, &one_chunk);
        let left = b.count_by_id(id).unwrap().unwrap();
        assert!(
            (1..=4).contains(&left),
            "the first chunk removed too much or too little: {left} left"
        );
        b.sweep_late_drop("shop", "orders", dropped).unwrap();
        assert_eq!(b.count_by_id(id).unwrap(), Some(0));
    }

    /// A SIGTERM at a start sets only the open's stop. It must end a sweep of
    /// one large collection between two chunks, not after the whole of it, and
    /// the next start finishes.
    #[test]
    fn a_stop_at_a_start_ends_the_sweep_of_one_large_collection_between_chunks() {
        let n = SWEEP_CHUNK * 3;
        let (b, _dir, id, _dropped) = a_store_holding_stragglers(n, 0);
        hooks::stop_the_open_after_chunks(1);
        let stopped = b.reconcile_late_drops();
        crate::open_progress::tests::THIS_THREAD_STOPS.with(|s| s.set(false));
        assert!(matches!(stopped, Err(crate::StorageError::Stopping(_))), "{stopped:?}");
        assert_eq!(
            b.count_by_id(id).unwrap(),
            Some((n - SWEEP_CHUNK) as u64),
            "one chunk, then the stop"
        );
        assert_eq!(b.get_collection("shop", "orders").unwrap().incarnation_floor, None);

        assert_eq!(b.reconcile_late_drops().unwrap(), n - SWEEP_CHUNK);
        assert_eq!(b.count_by_id(id).unwrap(), Some(0));
        assert_counts_exact(&b, "a sweep stopped at a start and finished at the next");
    }

    /// The index entries of a swept document go with it, and the document is no
    /// longer found through the index.
    #[test]
    fn a_swept_document_leaves_no_index_entry_behind() {
        let (b, _db) = engine();
        let on_b = b.create_collection("shop", "orders").unwrap();
        b.create_index(
            "shop",
            "orders",
            vec![kimmy_core::IndexField { path: "x".into(), descending: false }],
            false,
            Some("by_x".into()),
        )
        .unwrap();
        b.insert(&on_b, doc! { "_id": 2i64, "x": 6 }).unwrap();
        let id = CollectionId::derive("shop", "orders");
        let node = NodeId::from_bytes([7; 16]);
        let dropped = Stamp::new(Hlc::new(on_b.created.wall_ms - 10, 0), node);
        let straggler = Stamp::new(Hlc::new(on_b.created.wall_ms - 20, 0), node);
        let coll = b.get_collection("shop", "orders").unwrap();
        let entry = kimmy_core::OplogEntry {
            stamp: straggler,
            kind: kimmy_core::OpKind::Insert,
            collection: id,
            doc_id: Some(DocId::Int64(1)),
            body: Some(bson::serialize_to_vec(&doc! { "_id": 1i64, "x": 5 }).unwrap()),
        };
        assert!(b.apply_remote(&coll, &entry).unwrap());
        let entries = |e: &Engine| {
            use redb::ReadableDatabase;
            let db = e.db();
            let txn = db.begin_read().unwrap();
            txn.open_table(tables::INDEX_ENTRIES).unwrap().iter().unwrap().count()
        };
        assert_eq!(entries(&b), 2, "premise: both documents are indexed");
        b.record_collection_drop(id, dropped).unwrap();
        assert_eq!(b.sweep_late_drop("shop", "orders", dropped).unwrap(), 1);
        assert_eq!(entries(&b), 1, "the swept document's index entry is gone");
        assert_eq!(docs_of(&b, "shop", "orders", &[1, 2]), vec![2]);
    }

    /// A record that does not decode does not fail the sweep, or the start:
    /// a body that does not decode is removed without index maintenance, and a
    /// header that does not decode is left where it is.
    #[test]
    fn a_record_that_does_not_decode_does_not_stop_the_sweep() {
        let (b, _dir, id, dropped) = a_store_holding_stragglers(3, 0);
        {
            let node = NodeId::from_bytes([7; 16]);
            let straggler = Stamp::new(Hlc::new(dropped.hlc.wall_ms - 10, 0), node);
            let txn = b.begin_write(WriterHolder::Replication).unwrap();
            let mut docs = txn.open_table(tables::DOCS).unwrap();
            // A live record whose body is not a document.
            let key = crate::docs::doc_key(&DocId::Int64(100)).unwrap();
            crate::live_count::put_record(
                &txn,
                &mut docs,
                id.0,
                &key,
                &codec::encode_doc_record(&kimmy_core::DocRecord::live(
                    straggler,
                    b"not a bson document".to_vec(),
                )),
            )
            .unwrap();
            // A row whose header is garbage.
            let key = crate::docs::doc_key(&DocId::Int64(101)).unwrap();
            drop(docs);
            // Written outside the counted path on purpose: a row that is not a
            // record at all.
            txn.open_table(tables::DOCS)
                .unwrap()
                .insert((id.0, key.as_slice()), b"x".as_slice())
                .unwrap();
            txn.commit().unwrap();
        }
        let removed = b.sweep_late_drop("shop", "orders", dropped).unwrap();
        assert_eq!(removed, 4, "three documents and the one whose body does not decode");
        assert_eq!(
            b.get_collection("shop", "orders").unwrap().incarnation_floor,
            Some(dropped.hlc),
            "the sweep finished"
        );
    }

    /// The definitions of the life before go with its documents: an index
    /// created in it, and a vector configuration changed in it, are not the
    /// newer life's.
    #[test]
    fn the_definitions_of_the_life_before_are_swept_with_its_documents() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let on_a = a.create_collection("shop", "orders").unwrap();
        a.create_index(
            "shop",
            "orders",
            vec![kimmy_core::IndexField { path: "from".into(), descending: false }],
            false,
            Some("by_from".into()),
        )
        .unwrap();
        a.configure_vectors(
            "shop",
            "orders",
            kimmy_core::VectorConfig {
                fields: vec!["text".into()],
                provider: kimmy_core::ProviderConfig::Byo {},
                dim: 4,
                metric: Default::default(),
                document_prefix: None,
                query_prefix: None,
                chunk: Default::default(),
            },
        )
        .unwrap();
        a.insert(&on_a, doc! { "_id": 1i64, "from": "a", "text": "t" }).unwrap();
        assert!(a.drop_collection("shop", "orders").unwrap());
        a.finish_purges_now().unwrap();
        let on_b = b.create_collection("shop", "orders").unwrap();
        b.insert(&on_b, doc! { "_id": 2i64, "from": "b" }).unwrap();
        quiesce(&[&a, &b]);
        let standing = b.get_collection("shop", "orders").unwrap();
        assert!(
            standing.indexes.is_empty(),
            "the life before's index survived: {:?}",
            standing.indexes
        );
        assert!(standing.vector.is_none(), "the life before's vector configuration survived");
        assert_eq!(docs_of(&b, "shop", "orders", &[1, 2]), vec![2]);
        let a_standing = a.get_collection("shop", "orders").unwrap();
        assert!(a_standing.indexes.is_empty() && a_standing.vector.is_none());
        // And an index of the newer life, made after the drop, stays.
        b.create_index(
            "shop",
            "orders",
            vec![kimmy_core::IndexField { path: "from".into(), descending: false }],
            false,
            Some("by_from".into()),
        )
        .unwrap();
        quiesce(&[&a, &b]);
        assert_eq!(b.get_collection("shop", "orders").unwrap().indexes.len(), 1);
        assert_eq!(a.get_collection("shop", "orders").unwrap().indexes.len(), 1);
    }

    /// The tie the floor exists for: a record stamped at the drop's own `Hlc`.
    #[test]
    fn a_record_stamped_at_the_drops_own_hlc_is_swept_and_one_after_it_is_kept() {
        let (b, _db) = engine();
        let id = CollectionId::derive("shop", "orders");
        let on_b = b.create_collection("shop", "orders").unwrap();
        let node = NodeId::from_bytes([7; 16]);
        let dropped = Stamp::new(Hlc::new(on_b.created.wall_ms - 10, 0), node);
        let put = |n: i64, stamp: Stamp| {
            let key = crate::docs::doc_key(&DocId::Int64(n)).unwrap();
            let body = bson::serialize_to_vec(&doc! { "_id": n }).unwrap();
            let txn = b.begin_write(WriterHolder::Replication).unwrap();
            let mut docs = txn.open_table(tables::DOCS).unwrap();
            crate::live_count::put_record(
                &txn,
                &mut docs,
                id.0,
                &key,
                &codec::encode_doc_record(&kimmy_core::DocRecord::live(stamp, body)),
            )
            .unwrap();
            drop(docs);
            txn.commit().unwrap();
        };
        put(1, Stamp::new(dropped.hlc, NodeId::from_bytes([9; 16])));
        put(2, Stamp::new(Hlc::new(dropped.hlc.wall_ms + 1, 0), node));
        b.record_collection_drop(id, dropped).unwrap();
        assert_eq!(b.sweep_late_drop("shop", "orders", dropped).unwrap(), 1);
        assert_eq!(docs_of(&b, "shop", "orders", &[1, 2]), vec![2]);
    }

    // -- #185 follow-ups: the exceptions, the boundary, the layers ---------------

    fn by_from() -> Vec<kimmy_core::IndexField> {
        vec![kimmy_core::IndexField { path: "from".into(), descending: false }]
    }

    fn byo_config() -> kimmy_core::VectorConfig {
        kimmy_core::VectorConfig {
            fields: vec!["text".into()],
            provider: kimmy_core::ProviderConfig::Byo {},
            dim: 4,
            metric: Default::default(),
            document_prefix: None,
            query_prefix: None,
            chunk: Default::default(),
        }
    }

    /// Rewrite the stored definition of `shop.orders` on `e`.
    fn edit_meta(e: &Engine, edit: impl FnOnce(&mut CollectionMeta)) {
        let mut meta = e.get_collection("shop", "orders").unwrap();
        edit(&mut meta);
        let txn = e.begin_write(WriterHolder::Ddl).unwrap();
        Engine::put_collection_meta(&txn, &meta).unwrap();
        txn.commit().unwrap();
    }

    /// A drops `shop.orders`; B makes the name afterwards. Returns B's id for the
    /// collection and A's drop.
    fn a_drop_and_a_later_life() -> (Engine, Engine, tempfile::TempDir, tempfile::TempDir, Stamp) {
        let (a, da) = engine();
        let (b, db) = engine();
        let on_a = a.create_collection("shop", "orders").unwrap();
        a.insert(&on_a, doc! { "_id": 1i64 }).unwrap();
        assert!(a.drop_collection("shop", "orders").unwrap());
        a.finish_purges_now().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let on_b = b.create_collection("shop", "orders").unwrap();
        b.insert(&on_b, doc! { "_id": 2i64 }).unwrap();
        let dropped = a.collection_dropped_at(on_a.id).unwrap().unwrap();
        (a, b, da, db, dropped)
    }

    /// f-1. An index with no `created` stamp (one made before the stamp existed)
    /// cannot be told from the newer life's, and the sweep leaves it.
    #[test]
    fn an_index_with_no_created_stamp_is_left_by_the_sweep() {
        let (a, b, _da, _db, _dropped) = a_drop_and_a_later_life();
        b.create_index("shop", "orders", by_from(), false, Some("old_style".into())).unwrap();
        edit_meta(&b, |meta| meta.indexes[0].created = None);
        quiesce(&[&a, &b]);
        let standing = b.get_collection("shop", "orders").unwrap();
        assert_eq!(standing.indexes.len(), 1, "swept: {:?}", standing.indexes);
        assert_eq!(docs_of(&b, "shop", "orders", &[1, 2]), vec![2]);
    }

    /// f-1. The same for a vector configuration with no `vector_changed`.
    #[test]
    fn a_vector_configuration_with_no_vector_changed_is_left_by_the_sweep() {
        let (a, b, _da, _db, _dropped) = a_drop_and_a_later_life();
        b.configure_vectors("shop", "orders", byo_config()).unwrap();
        edit_meta(&b, |meta| meta.vector_changed = None);
        quiesce(&[&a, &b]);
        let standing = b.get_collection("shop", "orders").unwrap();
        assert!(standing.vector.is_some(), "the unstamped configuration was turned off");
        assert_eq!(docs_of(&b, "shop", "orders", &[1, 2]), vec![2]);
    }

    /// f-1. A definition tied at the drop (the same `Hlc`, from a node above the
    /// dropper's) is selected by the sweep and declined by the drop of the index,
    /// which keeps it and records the tombstone: the tie is on the selected side
    /// of the boundary and the definition stands.
    #[test]
    fn a_definition_tied_at_the_drop_is_kept_and_its_drop_is_declined() {
        let (a, b, _da, _db, dropped) = a_drop_and_a_later_life();
        b.create_index("shop", "orders", by_from(), false, Some("tied".into())).unwrap();
        let above = Stamp::new(dropped.hlc, NodeId::from_bytes([0xff; 16]));
        assert!(above > dropped, "premise: the tie sorts above the dropper's");
        edit_meta(&b, |meta| meta.indexes[0].created = Some(above));
        let id = b.get_collection("shop", "orders").unwrap().id;
        quiesce(&[&a, &b]);
        let standing = b.get_collection("shop", "orders").unwrap();
        assert_eq!(standing.indexes.len(), 1, "the tie was swept");
        assert_eq!(
            b.index_dropped_at(id, kimmy_core::IndexMeta::derive_id("tied")).unwrap(),
            Some(dropped),
            "the tie is selected and its drop declined, with the tombstone recorded"
        );
    }

    /// f-1. A newer life's index with the same derived id as the old one's (same
    /// name) survives, and the sweep does not even ask to drop it: no tombstone is
    /// recorded for it, so the outer filter is what holds and not only the guard
    /// inside the drop.
    #[test]
    fn a_newer_lifes_index_with_the_old_ones_derived_id_survives_untouched() {
        let (a, b, _da, _db, dropped) = a_drop_and_a_later_life();
        // The old life had `by_from` too, from A, stamped before its drop.
        let on_a_id = CollectionId::derive("shop", "orders");
        let old = kimmy_core::IndexCreate {
            db: "shop".into(),
            collection: "orders".into(),
            index: kimmy_core::IndexMeta {
                id: kimmy_core::IndexMeta::derive_id("by_from"),
                name: "by_from".into(),
                fields: by_from(),
                unique: false,
                enforcement: Default::default(),
                multikey: false,
                expire_after_secs: None,
                partial_filter: None,
                created: Some(Stamp::new(Hlc::new(dropped.hlc.wall_ms - 1, 0), a.node_id())),
            },
        };
        std::thread::sleep(std::time::Duration::from_millis(5));
        let theirs = b.create_index("shop", "orders", by_from(), false, Some("by_from".into()));
        let mine = theirs.unwrap().created.unwrap();
        assert!(mine.hlc > dropped.hlc, "premise: B's index is newer than the drop");
        // A's older definition arrives at B and loses to B's own.
        let entry = crate::engine::ddl_entry(
            old.index.created.unwrap(),
            kimmy_core::OpKind::CreateIndex,
            on_a_id,
            &old,
        )
        .unwrap();
        b.apply_batch(std::slice::from_ref(&entry)).unwrap();
        quiesce(&[&a, &b]);
        let standing = b.get_collection("shop", "orders").unwrap();
        assert_eq!(standing.indexes.len(), 1);
        assert_eq!(standing.indexes[0].created, Some(mine));
        assert_eq!(
            b.index_dropped_at(on_a_id, kimmy_core::IndexMeta::derive_id("by_from")).unwrap(),
            None,
            "the newer index was never offered to the drop of the index"
        );
    }

    /// f-2. A's index `X` of the life before reaches B before B's own
    /// `create_index X`; B's create is told the index exists, and the old life's
    /// drop then used to sweep it, leaving B's client without the index. The create
    /// is now a new creation, stamped under the writer and replicated, so the sweep
    /// leaves it and every member holds the index.
    #[test]
    fn an_identical_create_over_an_inherited_index_survives_the_sweep_on_every_member() {
        let (a, da) = engine();
        let (b, db) = engine();
        let (c, dc) = engine();
        let _dirs = (da, db, dc);
        let on_a = a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", by_from(), false, Some("by_from".into())).unwrap();
        a.insert(&on_a, doc! { "_id": 1i64, "from": "a" }).unwrap();
        assert!(a.drop_collection("shop", "orders").unwrap());
        a.finish_purges_now().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let on_b = b.create_collection("shop", "orders").unwrap();
        b.insert(&on_b, doc! { "_id": 2i64, "from": "b" }).unwrap();
        // B takes A's entries up to the drop: the index of the life before comes in.
        let start = b.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
        let entries = a.entries_for_peer(start, BATCH, WalkScope::Background).unwrap().entries;
        let drop_at = entries.iter().position(|e| e.kind == kimmy_core::OpKind::DropCollection);
        let (before, rest) = entries.split_at(drop_at.expect("the drop is in the window"));
        b.apply_batch(before).unwrap();
        let inherited = b.get_collection("shop", "orders").unwrap();
        assert_eq!(inherited.indexes.len(), 1, "premise: the old index arrived");
        assert!(
            inherited.indexes[0].created.unwrap().hlc < inherited.created,
            "premise: stamped before B's life"
        );
        // B's client creates it: told it exists.
        let before_stamp = inherited.indexes[0].created;
        let answered = b.create_index("shop", "orders", by_from(), false, Some("by_from".into()));
        assert_eq!(answered.unwrap().name, "by_from");
        let after = b.get_collection("shop", "orders").unwrap();
        assert!(after.indexes[0].created > before_stamp, "re-stamped under B's writer");
        // And the drop arrives.
        b.apply_batch(rest).unwrap();
        quiesce(&[&a, &b, &c]);
        for (who, e) in [("a", &a), ("b", &b), ("c", &c)] {
            match e.get_collection("shop", "orders") {
                Ok(held) => {
                    assert!(
                        held.index("by_from").is_some(),
                        "{who} lost the index: {:?}",
                        held.indexes
                    )
                }
                Err(_) => assert_eq!(who, "never"),
            }
        }
        assert_eq!(docs_of(&b, "shop", "orders", &[1, 2]), vec![2]);
    }

    /// f-2, narrowly: an identical create over an index stamped at or above the
    /// collection's `created` stays the no-op it is. A client that creates on every
    /// boot mints no definition entry and moves no stamp.
    #[test]
    fn an_identical_create_over_an_index_of_this_life_mints_nothing() {
        let (b, _db) = engine();
        b.create_collection("shop", "orders").unwrap();
        let first = b.create_index("shop", "orders", by_from(), false, Some("by_from".into()));
        let first = first.unwrap();
        let vector = b.version_vector().unwrap();
        for _ in 0..3 {
            let again = b.create_index("shop", "orders", by_from(), false, Some("by_from".into()));
            assert_eq!(again.unwrap().created, first.created);
        }
        assert_eq!(b.version_vector().unwrap(), vector, "an entry was minted");
    }

    /// f-2 for the vector configuration: a local `configure_vectors` is never a
    /// no-op (it takes a fresh stamp and replicates), so the same sequence is safe
    /// already. Pinned, so it stays so.
    #[test]
    fn an_identical_vector_configuration_made_after_inheriting_it_survives_the_sweep() {
        let (a, da) = engine();
        let (b, db) = engine();
        let _dirs = (da, db);
        a.create_collection("shop", "orders").unwrap();
        a.configure_vectors("shop", "orders", byo_config()).unwrap();
        assert!(a.drop_collection("shop", "orders").unwrap());
        a.finish_purges_now().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        b.create_collection("shop", "orders").unwrap();
        let start = b.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
        let entries = a.entries_for_peer(start, BATCH, WalkScope::Background).unwrap().entries;
        let drop_at =
            entries.iter().position(|e| e.kind == kimmy_core::OpKind::DropCollection).unwrap();
        let (before, rest) = entries.split_at(drop_at);
        b.apply_batch(before).unwrap();
        assert!(b.get_collection("shop", "orders").unwrap().vector.is_some(), "premise: inherited");
        b.configure_vectors("shop", "orders", byo_config()).unwrap();
        b.apply_batch(rest).unwrap();
        quiesce(&[&a, &b]);
        assert!(b.get_collection("shop", "orders").unwrap().vector.is_some());
    }
}
