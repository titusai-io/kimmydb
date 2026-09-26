//! Atomic filtered writes — find-modify-return and update/delete by filter —
//! inside one write transaction.
//!
//! # Why the match happens here rather than in a read pass
//!
//! Matching in a *read* transaction and writing afterwards is wrong whenever
//! the write depends on what was read: two callers claiming "exactly one"
//! pending job both see it and both claim it, and two callers incrementing
//! the same counter both read the same image and one increment is lost.
//!
//! redb has a single writer, so a match found *inside* the write transaction
//! cannot be taken by anyone else between the match and the commit. That makes
//! this atomic by construction, with no retry loop, no ABA window, and no
//! possibility of reporting "nothing matched" while something did.
//!
//! The cost is that the writer is held for the match as well as the commit. An
//! indexed filter adds microseconds; an unindexed one adds the whole collection
//! scan, and that blocks every other write on the node. [`MAX_CANDIDATES`] is
//! the bound that keeps the worst case a refusal rather than a stall.
//!
//! # Why the query language is not in this crate
//!
//! `kimmy-query` is a **dev-only** dependency here, deliberately — the engine
//! does storage, not semantics. So filtering, ordering and update operators
//! arrive as [`ModifySpec`], a set of pure functions over documents that the
//! caller supplies and this module calls *inside* the transaction. It is the
//! same shape as the guard [`crate::Engine::delete_guarded`] takes, one step
//! further: there the caller decides "still eligible?", here it also decides
//! "which one?" and "changed how?".
//!
//! # Why `update` and `delete` come through here too
//!
//! They used to match in a read pass and write afterwards, on the grounds
//! that "change everything matching" did not care which image it changed. It
//! did: the operators ran on the image the *read* pass returned, so two
//! concurrent `$inc`s on one document could both read `n = 5` and both store
//! `n = 6` — a lost update on a single node, against the documented
//! per-document atomicity (ADR-083). [`Engine::modify_where`] is the same
//! in-transaction body as [`Engine::find_and_modify`], applied to every match
//! instead of a chosen one, so the two cannot drift apart.

use std::cmp::Ordering;

use bson::Document;
use kimmy_core::{DocId, DocRecord, OpKind, OplogEntry, Stamp};
use redb::ReadableTable;

use crate::docs::extract_id;
use crate::engine::{WriteTxn, WriterHolder, append_oplog, doc_range_after};
use crate::error::{Result, StorageError};
use crate::meta::CollectionMeta;
use crate::{Engine, codec, index, tables};

/// How many matching documents may be held inside the write transaction.
///
/// Sorting to choose one means materialising every match, and that happens
/// while the single writer is held. A full scan of 10,000 documents is ~8 ms
/// ([Benchmarks](../../../docs/benchmarks.md)) and `find`'s own `MAX_LIMIT` is
/// 10,000 for exactly that reason, so the same ceiling applies here — but as a
/// **refusal** rather than a truncation, because silently choosing from a
/// prefix of the matches would return the wrong document with no way to tell.
pub const MAX_CANDIDATES: usize = 10_000;

/// Documents a `multi: true` filtered write commits per transaction when
/// nothing configured otherwise (ADR-086).
///
/// The same order as the TTL per-pass cap: large enough that the per-commit
/// fsync is amortised across a thousand documents, small enough that the
/// single writer is released every few milliseconds of scanning.
pub const DEFAULT_MULTI_CHUNK_DOCS: usize = 1_000;

/// Where to look for candidate documents, in the engine's own terms.
///
/// The caller plans; this scans. Encoded byte ranges rather than a query type
/// keeps the crate boundary intact.
#[derive(Clone, Debug)]
pub enum Candidates {
    /// Every live document in the collection.
    Scan,
    /// Exactly these encoded document keys, looked up directly.
    ///
    /// What a filter pinning `_id` plans to. Without this variant an update
    /// by primary key — the commonest write there is — would scan the whole
    /// collection under the single writer to find one document.
    Keys(Vec<Vec<u8>>),
    /// The union of one index's key ranges, both bounds inclusive.
    Index {
        index_id: u32,
        ranges: Vec<(Vec<u8>, Vec<u8>)>,
        /// Whether the ranges intersect *both* ends, which is only sound while
        /// the index is not multikey. Re-checked inside this transaction; a
        /// multikey index falls back to a collection scan rather than silently
        /// losing documents — the same rule the read path follows, and the
        /// reason it must be re-read in the scanning snapshot.
        both_bounds: bool,
    },
}

/// What the caller decides, as pure functions over documents.
pub trait ModifySpec {
    /// Whether this document is a candidate.
    fn matches(&self, doc: &Document) -> bool;

    /// Order for choosing among matches; the first after sorting wins.
    ///
    /// Returning `Ordering::Equal` for everything leaves the scan's own order,
    /// which is `_id` order for an index scan and storage order otherwise.
    fn compare(&self, a: &Document, b: &Document) -> Ordering;

    /// Why `compare` cannot place this document, if it cannot.
    ///
    /// Asked of every match before any two are compared, so a document the
    /// caller's order has no position for — one holding a `Decimal128` where
    /// the sort would read it — is refused by name rather than placed
    /// somewhere. Nothing is refused by default.
    fn unsortable(&self, _doc: &Document) -> Option<String> {
        None
    }

    /// The new document, or `None` to remove it.
    fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String>;

    /// The document to insert when nothing matched, if this is an upsert.
    fn upsert(&self) -> Option<std::result::Result<Document, String>>;

    /// The version the caller expects the matched document to be at.
    ///
    /// `Some` makes the write conditional: a match at any other stamp — or
    /// no match at all, when the caller expected one — aborts the whole
    /// transaction with [`StorageError::Stale`]. Checked inside the write
    /// transaction, against the image about to be written, so there is no
    /// window between the check and the write.
    fn expected_stamp(&self) -> Option<Stamp> {
        None
    }
}

/// What happened, and the images either side of it.
#[derive(Clone, Debug, Default)]
pub struct ModifyOutcome {
    /// The document as it was before, when something matched.
    pub before: Option<Document>,
    /// The document as it is now, absent when it was removed.
    pub after: Option<Document>,
    pub matched: bool,
    pub upserted: Option<DocId>,
    /// The stamp the write produced, when it wrote anything.
    pub stamp: Option<Stamp>,
}

/// What a filtered write did, counted inside the transaction that did it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ModifyManyOutcome {
    /// Documents the candidate scan looked at.
    pub examined: u64,
    /// Documents the spec matched.
    pub matched: u64,
    /// Documents written — replaced, or tombstoned when the spec removed them.
    ///
    /// Equal to `matched` today: every match is written, whether or not the
    /// operators changed anything (the `modified` deviation is documented).
    /// Reported separately so a guard that declines a match has somewhere to
    /// show up.
    pub modified: u64,
    /// Write transactions the request cost — one per chunk (ADR-086). Zero
    /// when nothing matched.
    pub commits: u64,
    /// The stamp of the last document written, when one was.
    ///
    /// One version names one document, so this is only *the* result when the
    /// request wrote one — the single-document form of `update` and `delete`,
    /// which is where a caller needs it for the conditional write that follows
    /// (ADR-084). A multi write reports it too, but it describes the last
    /// document of the last chunk and nothing else.
    pub stamp: Option<Stamp>,
}

impl Engine {
    /// Change every matching document — or the first `stop_after` of them, in
    /// scan order — in write transactions of at most
    /// [`Engine::multi_chunk_docs`] documents each (ADR-086).
    ///
    /// Within a chunk the operators run on the image the write transaction
    /// holds, so there is no window between matching and writing for another
    /// writer to slip into, and a failure aborts the chunk whole: nothing of
    /// it is written, minted or published. Between chunks the writer is
    /// released; the next chunk resumes strictly after the last document key
    /// the previous one wrote, so a document is never visited twice and a
    /// writer that slips in between chunks is an ordinary concurrent writer.
    /// A failure in a later chunk leaves the earlier ones committed — the
    /// oplog reflects exactly what landed.
    ///
    /// Once a chunk has committed, the request is part done, and it keeps
    /// going rather than give up half way (ADR-192): the later chunks wait
    /// for the writer with no budget, and stop only if the node is past its
    /// drain deadline. Any failure after the first commit is
    /// [`StorageError::PartiallyApplied`], counting the committed chunks, so
    /// nothing that landed is answered as if it had not.
    ///
    /// A single-document request (`stop_after = Some(1)`) is one chunk of
    /// one, and behaves exactly as before.
    pub fn modify_where(
        &self,
        coll: &CollectionMeta,
        candidates: &Candidates,
        spec: &dyn ModifySpec,
        stop_after: Option<usize>,
    ) -> Result<ModifyManyOutcome> {
        let chunk = self.multi_chunk_docs();
        // A request that stops after one document is a client's ordinary
        // write and is attributed as one; anything that may take a chunk is
        // a bulk, whatever it ends up matching (ADR-159). Decided from the
        // budget rather than from what matched, because the hold is bought
        // before the match is known.
        let holder = if stop_after == Some(1) { WriterHolder::Write } else { WriterHolder::Bulk };
        let mut outcome = ModifyManyOutcome::default();
        let mut after: Option<Vec<u8>> = None;

        loop {
            // This chunk's budget: the chunk size, or what is left of
            // `stop_after`, whichever is smaller.
            let remaining =
                stop_after.map_or(usize::MAX, |n| n.saturating_sub(outcome.matched as usize));
            let budget = remaining.min(chunk);
            if budget == 0 {
                break;
            }

            let continuing = outcome.commits > 0;
            #[cfg(test)]
            if continuing {
                crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::BetweenChunks);
            }
            match self.modify_chunk(
                holder,
                continuing,
                coll,
                candidates,
                spec,
                budget,
                after.as_deref(),
            ) {
                Ok((examined, None)) => {
                    outcome.examined += examined;
                    break;
                }
                Ok((examined, Some((matched, entries, last_key)))) => {
                    outcome.examined += examined;
                    outcome.commits += 1;
                    outcome.matched += matched as u64;
                    outcome.modified += entries.len() as u64;
                    outcome.stamp = entries.last().map(|e| e.stamp);
                    // Published per chunk, after its commit: a subscriber
                    // sees a chunk whole before the next one begins.
                    self.publish(entries);

                    after = Some(last_key);
                    if matched < budget {
                        break;
                    }
                }
                // Before the first commit a failure is the request's whole
                // answer: nothing of it was written.
                Err((e, _)) if outcome.commits == 0 => return Err(e),
                // After it, what the earlier chunks wrote stands, is
                // published, and replicates, and the answer has to say so
                // (ADR-192). A commit whose outcome is unknown is counted
                // apart: it may be there, and may not.
                Err((e, in_doubt)) => {
                    return Err(StorageError::PartiallyApplied {
                        applied: crate::Applied::Modify {
                            matched: outcome.matched,
                            modified: outcome.modified,
                            commits: outcome.commits,
                            in_doubt,
                        },
                        cause: Box::new(e),
                    });
                }
            }
        }

        if outcome.matched == 0 && spec.expected_stamp().is_some() {
            // The caller expected a version to be there, and nothing was.
            return Err(StorageError::Stale { current: None });
        }
        Ok(outcome)
    }

    /// One chunk of [`Self::modify_where`]: match up to `budget` documents
    /// strictly after `after`, change them, and commit. `None` when nothing
    /// (more) matched, which commits nothing. Otherwise how many matched, the
    /// entries to publish, and the key the next chunk resumes after; with,
    /// either way, how many documents the scan examined.
    ///
    /// `continuing` is whether the request has already committed a chunk,
    /// which decides how it waits for the writer (ADR-192). A failure comes
    /// with the number of documents whose write is in doubt: the chunk's, if
    /// its commit's outcome is unknown, and otherwise none.
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn modify_chunk(
        &self,
        holder: WriterHolder,
        continuing: bool,
        coll: &CollectionMeta,
        candidates: &Candidates,
        spec: &dyn ModifySpec,
        budget: usize,
        after: Option<&[u8]>,
    ) -> std::result::Result<(u64, Option<(usize, Vec<OplogEntry>, Vec<u8>)>), (StorageError, u64)>
    {
        fn none(e: impl Into<StorageError>) -> (StorageError, u64) {
            (e.into(), 0)
        }
        let txn =
            if continuing { self.begin_write_continuing(holder) } else { self.begin_write(holder) }
                .map_err(none)?;
        let (matches, examined) =
            match self.collect_matches(&txn, coll, candidates, spec, Some(budget), after) {
                Ok(found) => found,
                Err(e) => {
                    txn.abort().map_err(none)?;
                    return Err(none(e));
                }
            };

        let Some((_, last)) = matches.last() else {
            // Nothing (more) matched: a no-op must not commit, mint or
            // publish.
            txn.abort().map_err(none)?;
            return Ok((examined, None));
        };
        let last_key = match extract_id(last).and_then(|id| crate::docs::doc_key(&id)) {
            Ok(key) => key,
            Err(e) => {
                txn.abort().map_err(none)?;
                return Err(none(e));
            }
        };

        let mut entries = Vec::with_capacity(matches.len());
        for (stamp, before) in &matches {
            match self.modify_in_txn(&txn, coll, *stamp, before, spec) {
                Ok((_, entry)) => entries.push(entry),
                Err(e) => {
                    txn.abort().map_err(none)?;
                    return Err(none(e));
                }
            }
        }

        match txn.commit() {
            Ok(()) => Ok((examined, Some((matches.len(), entries, last_key)))),
            Err(e @ StorageError::OutcomeUnknown(_)) => Err((e, entries.len() as u64)),
            Err(e) => Err(none(e)),
        }
    }

    /// Find one document, change it, and return it — atomically.
    pub fn find_and_modify(
        &self,
        coll: &CollectionMeta,
        candidates: &Candidates,
        spec: &dyn ModifySpec,
    ) -> Result<ModifyOutcome> {
        let txn = self.begin_write(WriterHolder::Write)?;

        let chosen = match self.choose(&txn, coll, candidates, spec) {
            Ok(chosen) => chosen,
            Err(e) => {
                txn.abort()?;
                return Err(e);
            }
        };

        let Some((stamp, before)) = chosen else {
            // Nothing matched. A caller that expected a version finds it
            // gone; otherwise an upsert inserts, and anything else is a
            // no-op that must not mint an oplog entry or publish an event.
            if spec.expected_stamp().is_some() {
                txn.abort()?;
                return Err(StorageError::Stale { current: None });
            }
            let Some(doc) = spec.upsert() else {
                txn.abort()?;
                return Ok(ModifyOutcome::default());
            };
            let doc = match doc {
                Ok(doc) => doc,
                Err(e) => {
                    txn.abort()?;
                    return Err(StorageError::Core(kimmy_core::Error::InvalidQuery(e)));
                }
            };
            let (id, entry) = match self.insert_in_txn(&txn, coll, doc) {
                Ok(pair) => pair,
                Err(e) => {
                    txn.abort()?;
                    return Err(e);
                }
            };
            let inserted: Document = bson::deserialize_from_slice(
                entry.body.as_deref().expect("an insert carries its body"),
            )?;
            txn.commit()?;
            let stamp = entry.stamp;
            self.publish(vec![entry]);
            return Ok(ModifyOutcome {
                before: None,
                after: Some(inserted),
                matched: false,
                upserted: Some(id),
                stamp: Some(stamp),
            });
        };

        let (next, entry) = match self.modify_in_txn(&txn, coll, stamp, &before, spec) {
            Ok(done) => done,
            Err(e) => {
                txn.abort()?;
                return Err(e);
            }
        };

        txn.commit()?;
        let stamp = entry.stamp;
        self.publish(vec![entry]);

        Ok(ModifyOutcome {
            before: Some(before),
            after: next,
            matched: true,
            upserted: None,
            stamp: Some(stamp),
        })
    }

    /// Apply the spec to one matched document and write the result.
    ///
    /// The one body behind both `find_and_modify` and `modify_where`, the
    /// way `insert` and `insert_many` share `insert_in_txn`: a rule added
    /// here — a guard, a stamp check — holds for every filtered write at
    /// once. Returns the image written (`None` for a removal) and the oplog
    /// entry to publish once the transaction commits.
    fn modify_in_txn(
        &self,
        txn: &WriteTxn<'_>,
        coll: &CollectionMeta,
        current: Stamp,
        before: &Document,
        spec: &dyn ModifySpec,
    ) -> Result<(Option<Document>, OplogEntry)> {
        if let Some(expected) = spec.expected_stamp()
            && expected != current
        {
            return Err(StorageError::Stale { current: Some(current) });
        }
        let id = extract_id(before)?;
        let next = spec
            .apply(before)
            .map_err(|e| StorageError::Core(kimmy_core::Error::InvalidQuery(e)))?;
        let entry = self.write_chosen(txn, coll, &id, before, next.clone())?;
        Ok((next, entry))
    }

    /// Collect matches inside the transaction and pick the first after sorting.
    fn choose(
        &self,
        txn: &redb::WriteTransaction,
        coll: &CollectionMeta,
        candidates: &Candidates,
        spec: &dyn ModifySpec,
    ) -> Result<Option<(Stamp, Document)>> {
        // Every match, because the sort has to see them all to pick one.
        let (mut matches, _) = self.collect_matches(txn, coll, candidates, spec, None, None)?;

        if matches.is_empty() {
            return Ok(None);
        }
        for (_, doc) in &matches {
            if let Some(why) = spec.unsortable(doc) {
                return Err(StorageError::Core(kimmy_core::Error::InvalidQuery(why)));
            }
        }
        // `sort_by` rather than picking a minimum: the comparator is the
        // caller's whole sort specification, and a stable sort keeps the
        // scan's order for documents the sort does not separate.
        matches.sort_by(|a, b| spec.compare(&a.1, &b.1));
        Ok(Some(matches.swap_remove(0)))
    }

    /// Every live document among the candidates that the spec matches, in
    /// document-key order, with how many were examined to find them.
    ///
    /// `limit` stops the scan once that many have matched — what a
    /// single-document `update` wants, and what a chunk of a `multi` one
    /// wants. `None` collects them all, which is what a sort needs; only then
    /// does the [`MAX_CANDIDATES`] refusal apply, because a bounded scan
    /// cannot hold the writer for an unbounded time. `after` resumes strictly
    /// past an encoded document key, which is how one chunk follows another
    /// without revisiting anything — every candidate path delivers keys in
    /// order, so it is a bound, not a filter.
    fn collect_matches(
        &self,
        txn: &redb::WriteTransaction,
        coll: &CollectionMeta,
        candidates: &Candidates,
        spec: &dyn ModifySpec,
        limit: Option<usize>,
        after: Option<&[u8]>,
    ) -> Result<(Vec<(Stamp, Document)>, u64)> {
        let mut matches: Vec<(Stamp, Document)> = Vec::new();
        let mut examined = 0u64;

        // `Ok(false)` asks the scan to stop: the limit is reached.
        let mut consider =
            |stamp: Stamp, doc: Document, matches: &mut Vec<(Stamp, Document)>| -> Result<bool> {
                examined += 1;
                if !spec.matches(&doc) {
                    return Ok(true);
                }
                matches.push((stamp, doc));
                if limit.is_none() && matches.len() > MAX_CANDIDATES {
                    // Refused, not truncated: choosing from a prefix would return
                    // a document that is not the one the sort asked for, and no
                    // caller could tell it happened.
                    return Err(StorageError::Core(kimmy_core::Error::InvalidQuery(format!(
                        "the filter matched more than {MAX_CANDIDATES} documents; \
                     narrow the filter, or add an index and a tighter one"
                    ))));
                }
                Ok(!limit.is_some_and(|n| matches.len() >= n))
            };

        let docs = txn.open_table(tables::DOCS)?;

        // Direct lookups share one body: a `$in` union of index ranges and a
        // list of primary keys can both offer one document twice. Keys are
        // sorted first so the walk is in key order, which `after` relies on.
        let mut lookup = |keys: Vec<Vec<u8>>, matches: &mut Vec<(Stamp, Document)>| -> Result<()> {
            let mut keys = keys;
            keys.sort();
            keys.dedup();
            for key in keys {
                if after.is_some_and(|bound| key.as_slice() <= bound) {
                    continue;
                }
                let Some(raw) = docs.get((coll.id.0, key.as_slice()))? else {
                    continue;
                };
                let record = codec::decode_doc_record(raw.value())?;
                if record.deleted {
                    continue;
                }
                let doc = bson::deserialize_from_slice(&record.body)?;
                if !consider(record.stamp, doc, matches)? {
                    break;
                }
            }
            Ok(())
        };

        match candidates {
            Candidates::Keys(keys) => lookup(keys.clone(), &mut matches)?,
            Candidates::Index { index_id, ranges, both_bounds } => {
                // The multikey flag is re-read here, in the transaction that
                // scans — a `false` from the caller's earlier read proves
                // nothing about this snapshot.
                let sound = !both_bounds || !self.index_is_multikey(txn, coll, *index_id)?;
                if sound {
                    let mut keys = Vec::new();
                    for (lower, upper) in ranges {
                        keys.extend(index::scan_range_in_write(
                            txn,
                            coll.id,
                            *index_id,
                            lower,
                            Some(upper),
                            index::Unkeyed::Include,
                        )?);
                    }
                    lookup(keys, &mut matches)?;
                } else {
                    scan_until(&docs, coll, after, &mut |stamp, doc| {
                        consider(stamp, doc, &mut matches)
                    })?;
                }
            }
            Candidates::Scan => scan_until(&docs, coll, after, &mut |stamp, doc| {
                consider(stamp, doc, &mut matches)
            })?,
        }

        Ok((matches, examined))
    }

    fn index_is_multikey(
        &self,
        txn: &redb::WriteTransaction,
        coll: &CollectionMeta,
        index_id: u32,
    ) -> Result<bool> {
        let collections = txn.open_table(tables::COLLECTIONS)?;
        let Some(raw) = collections.get((coll.db.as_str(), coll.name.as_str()))? else {
            return Ok(false);
        };
        let fresh: CollectionMeta = serde_json::from_slice(raw.value())?;
        Ok(fresh.index_by_id(index_id).is_none_or(|i| i.multikey))
    }

    /// Write the chosen document's new state and return its oplog entry.
    fn write_chosen(
        &self,
        txn: &WriteTxn<'_>,
        coll: &CollectionMeta,
        id: &DocId,
        before: &Document,
        next: Option<Document>,
    ) -> Result<OplogEntry> {
        let key = crate::docs::doc_key(id)?;
        let stamp = self.next_stamp();

        let (record, body, kind) = match &next {
            Some(doc) => {
                let body = bson::serialize_to_vec(doc)?;
                (DocRecord::live(stamp, body.clone()), Some(body), OpKind::Replace)
            }
            // A tombstone, exactly as an ordinary delete leaves — so a removal
            // through this route replicates and streams like any other delete.
            None => (DocRecord::tombstone(stamp), None, OpKind::Delete),
        };

        {
            let mut docs = txn.open_table(tables::DOCS)?;
            crate::live_count::put_record(
                txn,
                &mut docs,
                coll.id.0,
                &key,
                &codec::encode_doc_record(&record),
            )?;
        }

        let newly_multikey = index::maintain(self, txn, coll, Some(before), next.as_ref(), &key)?;
        index::mark_multikey(txn, &coll.db, &coll.name, &newly_multikey)?;

        let entry = OplogEntry { stamp, kind, collection: coll.id, doc_id: Some(id.clone()), body };
        append_oplog(txn, &entry)?;
        Ok(entry)
    }
}

/// Every live document in the collection strictly after `after`, inside the
/// caller's transaction, until `f` answers `false`.
fn scan_until(
    docs: &impl ReadableTable<(u64, &'static [u8]), &'static [u8]>,
    coll: &CollectionMeta,
    after: Option<&[u8]>,
    f: &mut impl FnMut(Stamp, Document) -> Result<bool>,
) -> Result<()> {
    for entry in docs.range(doc_range_after(coll.id, after))? {
        let (_, value) = entry?;
        let record = codec::decode_doc_record(value.value())?;
        if record.deleted {
            continue;
        }
        if !f(record.stamp, bson::deserialize_from_slice(&record.body)?)? {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    /// A spec built from closures, so each test states only what it varies.
    struct TestSpec<M, C, A> {
        matches: M,
        compare: C,
        apply: A,
        upsert: Option<Document>,
    }

    impl<M, C, A> ModifySpec for TestSpec<M, C, A>
    where
        M: Fn(&Document) -> bool,
        C: Fn(&Document, &Document) -> Ordering,
        A: Fn(&Document) -> std::result::Result<Option<Document>, String>,
    {
        fn matches(&self, doc: &Document) -> bool {
            (self.matches)(doc)
        }
        fn compare(&self, a: &Document, b: &Document) -> Ordering {
            (self.compare)(a, b)
        }
        fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
            (self.apply)(doc)
        }
        fn upsert(&self) -> Option<std::result::Result<Document, String>> {
            self.upsert.clone().map(Ok)
        }
    }

    fn engine() -> (Engine, CollectionMeta, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let c = e.create_collection("app", "jobs").unwrap();
        (e, c, dir)
    }

    fn i64_of(doc: &Document, key: &str) -> i64 {
        doc.get_i64(key).or_else(|_| doc.get_i32(key).map(i64::from)).unwrap()
    }

    /// Claim the lowest-`created` pending job, marking it claimed.
    fn claim() -> impl ModifySpec {
        TestSpec {
            matches: |d: &Document| d.get_str("status").map(|s| s == "pending").unwrap_or(false),
            compare: |a: &Document, b: &Document| i64_of(a, "created").cmp(&i64_of(b, "created")),
            apply: |d: &Document| {
                let mut next = d.clone();
                next.insert("status", "claimed");
                Ok(Some(next))
            },
            upsert: None,
        }
    }

    fn seed(engine: &Engine, coll: &CollectionMeta) {
        for (id, created, status) in
            [(1i64, 30i64, "pending"), (2, 10, "pending"), (3, 20, "done"), (4, 20, "pending")]
        {
            engine.insert(coll, doc! {"_id": id, "created": created, "status": status}).unwrap();
        }
    }

    #[test]
    fn the_sort_decides_which_match_is_taken() {
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);

        let out = engine.find_and_modify(&coll, &Candidates::Scan, &claim()).unwrap();
        assert!(out.matched);
        // _id 2 has the lowest `created` among pending; _id 3 is not pending.
        assert_eq!(out.before.as_ref().unwrap().get_i64("_id").unwrap(), 2);
        assert_eq!(out.before.unwrap().get_str("status").unwrap(), "pending");
        assert_eq!(out.after.unwrap().get_str("status").unwrap(), "claimed");
    }

    #[test]
    fn a_claim_is_visible_immediately_and_the_next_takes_another() {
        // Draining a queue must never hand out the same job twice.
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);

        let mut claimed = Vec::new();
        for _ in 0..3 {
            let out = engine.find_and_modify(&coll, &Candidates::Scan, &claim()).unwrap();
            assert!(out.matched);
            claimed.push(out.before.unwrap().get_i64("_id").unwrap());
        }
        claimed.sort();
        assert_eq!(claimed, vec![1, 2, 4], "each pending job claimed exactly once");

        // Nothing pending left.
        let out = engine.find_and_modify(&coll, &Candidates::Scan, &claim()).unwrap();
        assert!(!out.matched);
        assert!(out.before.is_none());
    }

    #[test]
    fn no_match_writes_nothing_and_publishes_nothing() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! {"_id": 1, "status": "done"}).unwrap();

        let mut rx = engine.subscribe();
        let out = engine.find_and_modify(&coll, &Candidates::Scan, &claim()).unwrap();
        assert!(!out.matched);
        assert!(rx.try_recv().is_err(), "a no-op must not publish an event");
    }

    #[test]
    fn remove_leaves_a_tombstone_and_an_ordinary_delete_entry() {
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);

        let remove = TestSpec {
            matches: |d: &Document| d.get_str("status").map(|s| s == "pending").unwrap_or(false),
            compare: |a: &Document, b: &Document| i64_of(a, "created").cmp(&i64_of(b, "created")),
            apply: |_: &Document| Ok(None),
            upsert: None,
        };

        let mut rx = engine.subscribe();
        let out = engine.find_and_modify(&coll, &Candidates::Scan, &remove).unwrap();
        assert!(out.matched);
        assert_eq!(out.before.unwrap().get_i64("_id").unwrap(), 2);
        assert!(out.after.is_none(), "there is no document after a removal");

        let event = rx.try_recv().unwrap();
        assert_eq!(event.kind, OpKind::Delete);
        assert!(engine.get(&coll, &DocId::Int64(2)).unwrap().is_none());
    }

    #[test]
    fn upsert_inserts_when_nothing_matched() {
        let (engine, coll, _dir) = engine();

        let spec = TestSpec {
            matches: |_: &Document| false,
            compare: |_: &Document, _: &Document| Ordering::Equal,
            apply: |d: &Document| Ok(Some(d.clone())),
            upsert: Some(doc! {"_id": 99, "status": "pending"}),
        };

        let mut rx = engine.subscribe();
        let out = engine.find_and_modify(&coll, &Candidates::Scan, &spec).unwrap();
        assert!(!out.matched, "an upsert did not match; it created");
        assert_eq!(out.upserted, Some(DocId::Int64(99)));
        assert_eq!(out.after.unwrap().get_str("status").unwrap(), "pending");

        // A created document is an insert to a change-stream subscriber.
        assert_eq!(rx.try_recv().unwrap().kind, OpKind::Insert);
        assert!(engine.get(&coll, &DocId::Int64(99)).unwrap().is_some());
    }

    #[test]
    fn upsert_does_not_fire_when_something_matched() {
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);

        let spec = TestSpec {
            matches: |d: &Document| d.get_str("status").map(|s| s == "pending").unwrap_or(false),
            compare: |a: &Document, b: &Document| i64_of(a, "created").cmp(&i64_of(b, "created")),
            apply: |d: &Document| {
                let mut next = d.clone();
                next.insert("status", "claimed");
                Ok(Some(next))
            },
            upsert: Some(doc! {"_id": 99}),
        };

        let out = engine.find_and_modify(&coll, &Candidates::Scan, &spec).unwrap();
        assert!(out.matched);
        assert_eq!(out.upserted, None);
        assert!(engine.get(&coll, &DocId::Int64(99)).unwrap().is_none());
    }

    #[test]
    fn an_index_plan_and_a_scan_agree() {
        let (engine, _coll, _dir) = engine();
        engine
            .create_index(
                "app",
                "jobs",
                vec![kimmy_core::IndexField::ascending("status")],
                false,
                Some("status_1".into()),
            )
            .unwrap();
        let coll = engine.get_collection("app", "jobs").unwrap();
        seed(&engine, &coll);

        let index = coll.index("status_1").unwrap();
        let probe = kimmy_core::keyenc::encode_compound_ordered(&[(
            bson::Bson::String("pending".into()),
            false,
        )])
        .unwrap();
        let candidates = Candidates::Index {
            index_id: index.id,
            ranges: vec![(probe.clone(), probe)],
            both_bounds: false,
        };

        let out = engine.find_and_modify(&coll, &candidates, &claim()).unwrap();
        assert!(out.matched);
        // The same answer the scan gives: lowest `created` among pending.
        assert_eq!(out.before.unwrap().get_i64("_id").unwrap(), 2);
    }

    #[test]
    fn an_index_entry_is_maintained_through_the_change() {
        // The modified document must leave the index consistent, or a later
        // claim would find a candidate whose document no longer matches.
        let (engine, _coll, _dir) = engine();
        engine
            .create_index(
                "app",
                "jobs",
                vec![kimmy_core::IndexField::ascending("status")],
                false,
                Some("status_1".into()),
            )
            .unwrap();
        let coll = engine.get_collection("app", "jobs").unwrap();
        seed(&engine, &coll);

        let index = coll.index("status_1").unwrap();
        let probe = kimmy_core::keyenc::encode_compound_ordered(&[(
            bson::Bson::String("pending".into()),
            false,
        )])
        .unwrap();
        let candidates = Candidates::Index {
            index_id: index.id,
            ranges: vec![(probe.clone(), probe.clone())],
            both_bounds: false,
        };

        // Claim all three pending jobs through the index.
        for _ in 0..3 {
            assert!(engine.find_and_modify(&coll, &candidates, &claim()).unwrap().matched);
        }
        // The index must now offer no `pending` candidates at all.
        let out = engine.find_and_modify(&coll, &candidates, &claim()).unwrap();
        assert!(!out.matched, "stale index entries survived the modification");
    }

    #[test]
    fn too_many_matches_is_refused_rather_than_truncated() {
        // Choosing from a prefix would return a document the sort did not
        // pick, and no caller could tell it happened.
        let (engine, coll, _dir) = engine();
        // One transaction: 10,001 separate commits would make this test a
        // minute long on its own, which is how a suite stops being run.
        let batch: Vec<Document> = (0..=MAX_CANDIDATES as i64)
            .map(|id| doc! {"_id": id, "created": id, "status": "pending"})
            .collect();
        engine.insert_many(&coll, batch).unwrap();

        let err = engine.find_and_modify(&coll, &Candidates::Scan, &claim());
        assert!(err.is_err(), "over the cap must refuse");
        // And nothing was written by the attempt.
        assert_eq!(
            engine.get(&coll, &DocId::Int64(0)).unwrap().unwrap().get_str("status").unwrap(),
            "pending"
        );
    }

    #[test]
    fn a_failing_apply_aborts_and_leaves_the_document_alone() {
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);

        let spec = TestSpec {
            matches: |d: &Document| d.get_str("status").map(|s| s == "pending").unwrap_or(false),
            compare: |a: &Document, b: &Document| i64_of(a, "created").cmp(&i64_of(b, "created")),
            apply: |_: &Document| Err("nope".to_string()),
            upsert: None,
        };

        let mut rx = engine.subscribe();
        assert!(engine.find_and_modify(&coll, &Candidates::Scan, &spec).is_err());
        assert_eq!(
            engine.get(&coll, &DocId::Int64(2)).unwrap().unwrap().get_str("status").unwrap(),
            "pending"
        );
        assert!(rx.try_recv().is_err(), "a failed modify must publish nothing");
    }

    #[test]
    fn concurrent_claims_never_hand_out_the_same_job_twice() {
        // The reason the match lives inside the write transaction. Eight
        // threads race for four jobs: every claim must be distinct, and
        // exactly four must succeed.
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let coll = engine.create_collection("app", "jobs").unwrap();
        for id in 0..4i64 {
            engine.insert(&coll, doc! {"_id": id, "created": id, "status": "pending"}).unwrap();
        }

        let winners = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let engine = Arc::clone(&engine);
            let coll = coll.clone();
            let winners = Arc::clone(&winners);
            handles.push(std::thread::spawn(move || {
                let out = engine.find_and_modify(&coll, &Candidates::Scan, &claim()).unwrap();
                if out.matched {
                    let id = out.before.unwrap().get_i64("_id").unwrap();
                    winners.lock().unwrap().push(id);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let mut claimed = winners.lock().unwrap().clone();
        claimed.sort();
        let distinct: std::collections::BTreeSet<i64> = claimed.iter().copied().collect();
        assert_eq!(
            claimed.len(),
            distinct.len(),
            "a job was claimed twice: {claimed:?} — the match is not atomic"
        );
        assert_eq!(claimed, vec![0, 1, 2, 3], "every job claimed exactly once");
    }

    // -----------------------------------------------------------------------
    // modify_where — update and delete by filter
    // -----------------------------------------------------------------------

    /// Claim every pending job, in scan order, without sorting.
    fn claim_all() -> impl ModifySpec {
        TestSpec {
            matches: |d: &Document| d.get_str("status").map(|s| s == "pending").unwrap_or(false),
            compare: |_: &Document, _: &Document| Ordering::Equal,
            apply: |d: &Document| {
                let mut next = d.clone();
                next.insert("status", "claimed");
                Ok(Some(next))
            },
            upsert: None,
        }
    }

    fn drain(rx: &mut tokio::sync::broadcast::Receiver<std::sync::Arc<OplogEntry>>) -> Vec<OpKind> {
        let mut kinds = Vec::new();
        while let Ok(entry) = rx.try_recv() {
            kinds.push(entry.kind);
        }
        kinds
    }

    #[test]
    fn a_filtered_write_is_one_commit_for_every_match() {
        // The whole point: every match written in the transaction that found
        // it, and one fsync for the request rather than one per document.
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);
        let mut rx = engine.subscribe();

        let before = engine.commits();
        let out = engine.modify_where(&coll, &Candidates::Scan, &claim_all(), None).unwrap();
        assert_eq!(engine.commits() - before, 1, "one request, one commit");
        assert_eq!(
            out,
            ModifyManyOutcome { examined: 4, matched: 3, modified: 3, commits: 1, ..out }
        );

        for id in [1i64, 2, 4] {
            let doc = engine.get(&coll, &DocId::Int64(id)).unwrap().unwrap();
            assert_eq!(doc.get_str("status").unwrap(), "claimed", "_id {id}");
        }
        assert_eq!(
            engine.get(&coll, &DocId::Int64(3)).unwrap().unwrap().get_str("status").unwrap(),
            "done",
            "a non-match is untouched"
        );
        assert_eq!(
            drain(&mut rx),
            vec![OpKind::Replace; 3],
            "one event per document, after commit"
        );
    }

    #[test]
    fn stop_after_takes_the_first_matches_in_scan_order() {
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);

        let out = engine.modify_where(&coll, &Candidates::Scan, &claim_all(), Some(1)).unwrap();
        // Documents scan in key order and _id 1 is pending, so the scan
        // stops at the first document it looks at.
        assert_eq!(
            out,
            ModifyManyOutcome { examined: 1, matched: 1, modified: 1, commits: 1, ..out }
        );
        assert_eq!(
            engine.get(&coll, &DocId::Int64(1)).unwrap().unwrap().get_str("status").unwrap(),
            "claimed"
        );
        assert_eq!(
            engine.get(&coll, &DocId::Int64(2)).unwrap().unwrap().get_str("status").unwrap(),
            "pending",
            "the second match is left for the next request"
        );
    }

    #[test]
    fn nothing_matched_commits_nothing_and_publishes_nothing() {
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);
        let mut rx = engine.subscribe();

        let spec = TestSpec {
            matches: |_: &Document| false,
            compare: |_: &Document, _: &Document| Ordering::Equal,
            apply: |d: &Document| Ok(Some(d.clone())),
            upsert: None,
        };
        let before = engine.commits();
        let out = engine.modify_where(&coll, &Candidates::Scan, &spec, None).unwrap();
        assert_eq!(
            out,
            ModifyManyOutcome { examined: 4, matched: 0, modified: 0, commits: 0, ..out }
        );
        assert_eq!(engine.commits() - before, 0, "a no-op must not reach the disk");
        assert!(rx.try_recv().is_err(), "a no-op must publish nothing");
    }

    #[test]
    fn a_failing_apply_on_a_later_match_writes_nothing_at_all() {
        // All or nothing: the earlier matches were written in the same
        // transaction, so the abort takes them back too.
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);
        let mut rx = engine.subscribe();

        let spec = TestSpec {
            matches: |d: &Document| d.get_str("status").map(|s| s == "pending").unwrap_or(false),
            compare: |_: &Document, _: &Document| Ordering::Equal,
            apply: |d: &Document| {
                if i64_of(d, "_id") == 4 {
                    return Err("nope".to_string());
                }
                let mut next = d.clone();
                next.insert("status", "claimed");
                Ok(Some(next))
            },
            upsert: None,
        };
        let before = engine.commits();
        assert!(engine.modify_where(&coll, &Candidates::Scan, &spec, None).is_err());
        assert_eq!(engine.commits() - before, 0);
        for id in [1i64, 2, 4] {
            let doc = engine.get(&coll, &DocId::Int64(id)).unwrap().unwrap();
            assert_eq!(doc.get_str("status").unwrap(), "pending", "_id {id} must be untouched");
        }
        assert!(rx.try_recv().is_err(), "a failed request must publish nothing");
    }

    #[test]
    fn a_removing_spec_tombstones_every_match() {
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);
        let mut rx = engine.subscribe();

        let spec = TestSpec {
            matches: |d: &Document| d.get_str("status").map(|s| s == "pending").unwrap_or(false),
            compare: |_: &Document, _: &Document| Ordering::Equal,
            apply: |_: &Document| Ok(None),
            upsert: None,
        };
        let out = engine.modify_where(&coll, &Candidates::Scan, &spec, None).unwrap();
        assert_eq!(
            out,
            ModifyManyOutcome { examined: 4, matched: 3, modified: 3, commits: 1, ..out }
        );
        for id in [1i64, 2, 4] {
            assert!(engine.get(&coll, &DocId::Int64(id)).unwrap().is_none(), "_id {id} removed");
        }
        assert!(engine.get(&coll, &DocId::Int64(3)).unwrap().is_some());
        assert_eq!(drain(&mut rx), vec![OpKind::Delete; 3], "removals are ordinary deletes");
    }

    #[test]
    fn keys_are_looked_up_directly_and_deduplicated() {
        // A primary-key plan: no scan, missing and tombstoned keys are plain
        // misses, and a key offered twice is examined once.
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);
        assert!(engine.delete(&coll, &DocId::Int64(2)).unwrap());

        let key = |id: i64| crate::docs::doc_key(&DocId::Int64(id)).unwrap();
        let candidates = Candidates::Keys(vec![key(2), key(99), key(1), key(1), key(3)]);

        let out = engine.modify_where(&coll, &candidates, &claim_all(), None).unwrap();
        assert_eq!(
            out,
            ModifyManyOutcome { examined: 2, matched: 1, modified: 1, commits: 1, ..out }
        );
        assert_eq!(
            engine.get(&coll, &DocId::Int64(1)).unwrap().unwrap().get_str("status").unwrap(),
            "claimed"
        );
        assert_eq!(
            engine.get(&coll, &DocId::Int64(4)).unwrap().unwrap().get_str("status").unwrap(),
            "pending",
            "a key not offered is not touched"
        );
    }

    #[test]
    fn concurrent_increments_are_all_kept() {
        // The lost-update defect, at the engine: eight threads each add one
        // to the same counter a hundred times. Every increment must land.
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let coll = engine.create_collection("app", "counters").unwrap();
        engine.insert(&coll, doc! {"_id": 1i64, "n": 0i64}).unwrap();
        let key = crate::docs::doc_key(&DocId::Int64(1)).unwrap();

        let mut handles = Vec::new();
        for _ in 0..8 {
            let engine = Arc::clone(&engine);
            let coll = coll.clone();
            let candidates = Candidates::Keys(vec![key.clone()]);
            handles.push(std::thread::spawn(move || {
                let spec = TestSpec {
                    matches: |_: &Document| true,
                    compare: |_: &Document, _: &Document| Ordering::Equal,
                    apply: |d: &Document| {
                        let mut next = d.clone();
                        next.insert("n", i64_of(d, "n") + 1);
                        Ok(Some(next))
                    },
                    upsert: None,
                };
                for _ in 0..100 {
                    let out = engine.modify_where(&coll, &candidates, &spec, Some(1)).unwrap();
                    assert_eq!(out.modified, 1);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let doc = engine.get(&coll, &DocId::Int64(1)).unwrap().unwrap();
        assert_eq!(i64_of(&doc, "n"), 800, "an increment was lost");
    }

    #[test]
    fn a_filtered_write_past_the_cap_commits_in_chunks_rather_than_refusing() {
        // What `find_and_modify` refuses, a `multi` write chunks (ADR-086):
        // 10,001 matches at the default chunk of 1,000 is eleven commits,
        // every document written, none visited twice.
        let (engine, coll, _dir) = engine();
        let batch: Vec<Document> = (0..=MAX_CANDIDATES as i64)
            .map(|id| doc! {"_id": id, "created": id, "status": "pending"})
            .collect();
        engine.insert_many(&coll, batch).unwrap();

        // `find_and_modify` still refuses: it has to sort the whole set.
        assert!(engine.find_and_modify(&coll, &Candidates::Scan, &claim()).is_err());

        let before = engine.commits();
        let out = engine.modify_where(&coll, &Candidates::Scan, &claim_all(), None).unwrap();
        assert_eq!(out.commits, 11);
        assert_eq!(engine.commits() - before, 11);
        assert_eq!((out.matched, out.modified), (10_001, 10_001));
        assert_eq!(out.examined, 10_001, "no document examined twice across chunks");
        assert_eq!(
            engine.get(&coll, &DocId::Int64(10_000)).unwrap().unwrap().get_str("status").unwrap(),
            "claimed"
        );
    }

    #[test]
    fn a_multi_write_commits_in_chunks_of_the_configured_size() {
        let (engine, coll, _dir) = engine();
        engine.set_multi_chunk_docs(5);
        assert_eq!(engine.multi_chunk_docs(), 5);
        let batch: Vec<Document> =
            (0..12i64).map(|id| doc! {"_id": id, "created": id, "status": "pending"}).collect();
        engine.insert_many(&coll, batch).unwrap();
        let mut rx = engine.subscribe();

        let before = engine.commits();
        let out = engine.modify_where(&coll, &Candidates::Scan, &claim_all(), None).unwrap();
        assert_eq!(
            out,
            ModifyManyOutcome { examined: 12, matched: 12, modified: 12, commits: 3, ..out }
        );
        assert_eq!(engine.commits() - before, 3, "5 + 5 + 2");

        // Events arrive chunk by chunk, in key order: a subscriber sees the
        // first five whole before the second chunk begins.
        let mut ids = Vec::new();
        while let Ok(entry) = rx.try_recv() {
            ids.push(entry.doc_id.clone().unwrap().to_string());
        }
        let expected: Vec<String> = (0..12i64).map(|i| DocId::Int64(i).to_string()).collect();
        assert_eq!(ids, expected);

        // The clamp: zero would never advance, and above the cap would hold
        // the writer longer than `find_and_modify` may.
        engine.set_multi_chunk_docs(0);
        assert_eq!(engine.multi_chunk_docs(), 1);
        engine.set_multi_chunk_docs(usize::MAX);
        assert_eq!(engine.multi_chunk_docs(), MAX_CANDIDATES);
    }

    #[test]
    fn a_failure_in_a_later_chunk_leaves_the_earlier_chunks_committed() {
        // The bound on what a crash or a refusal can lose is one chunk: what
        // landed before it stays, and the oplog says exactly what that was.
        let (engine, coll, _dir) = engine();
        engine.set_multi_chunk_docs(4);
        let batch: Vec<Document> =
            (0..10i64).map(|id| doc! {"_id": id, "created": id, "status": "pending"}).collect();
        engine.insert_many(&coll, batch).unwrap();
        let mut rx = engine.subscribe();

        let spec = TestSpec {
            matches: |d: &Document| d.get_str("status").map(|s| s == "pending").unwrap_or(false),
            compare: |_: &Document, _: &Document| Ordering::Equal,
            apply: |d: &Document| {
                if i64_of(d, "_id") == 6 {
                    return Err("nope".to_string());
                }
                let mut next = d.clone();
                next.insert("status", "claimed");
                Ok(Some(next))
            },
            upsert: None,
        };
        let before = engine.commits();
        let failed = engine.modify_where(&coll, &Candidates::Scan, &spec, None).unwrap_err();
        // Answered with what landed, never as a failure that wrote nothing
        // (ADR-192).
        let StorageError::PartiallyApplied { applied, cause } = failed else {
            panic!("a failure after a commit is partly applied: {failed:?}");
        };
        assert_eq!(
            applied,
            crate::Applied::Modify { matched: 4, modified: 4, commits: 1, in_doubt: 0 }
        );
        assert!(matches!(*cause, StorageError::Core(_)), "the apply's own error: {cause:?}");
        assert_eq!(engine.commits() - before, 1, "chunk one committed, chunk two aborted");
        for id in 0..4i64 {
            let doc = engine.get(&coll, &DocId::Int64(id)).unwrap().unwrap();
            assert_eq!(doc.get_str("status").unwrap(), "claimed", "_id {id} is in chunk one");
        }
        for id in 4..10i64 {
            let doc = engine.get(&coll, &DocId::Int64(id)).unwrap().unwrap();
            assert_eq!(doc.get_str("status").unwrap(), "pending", "_id {id} is past the failure");
        }
        let mut events = 0;
        while rx.try_recv().is_ok() {
            events += 1;
        }
        assert_eq!(events, 4, "exactly the committed chunk was published");
    }

    #[test]
    fn chunks_resume_through_keys_and_an_index_without_revisiting() {
        // `after` is a bound on every candidate path, not only the scan.
        let (engine, coll, _dir) = engine();
        engine.set_multi_chunk_docs(3);
        let batch: Vec<Document> =
            (0..8i64).map(|id| doc! {"_id": id, "created": id, "status": "pending"}).collect();
        engine.insert_many(&coll, batch).unwrap();

        // Keys, offered out of order and with a duplicate.
        let key = |id: i64| crate::docs::doc_key(&DocId::Int64(id)).unwrap();
        let keys = Candidates::Keys(vec![key(7), key(2), key(5), key(2), key(0), key(3), key(6)]);
        let out = engine.modify_where(&coll, &keys, &claim_all(), None).unwrap();
        assert_eq!(
            out,
            ModifyManyOutcome { examined: 6, matched: 6, modified: 6, commits: 2, ..out }
        );
        assert_eq!(
            engine.get(&coll, &DocId::Int64(1)).unwrap().unwrap().get_str("status").unwrap(),
            "pending",
            "a key not offered is untouched"
        );
    }

    // -----------------------------------------------------------------------
    // Conditional writes (ADR-084)
    // -----------------------------------------------------------------------

    /// `claim()`, conditional on the chosen document being at `expected`.
    struct ConditionalClaim {
        expected: Option<Stamp>,
    }

    impl ModifySpec for ConditionalClaim {
        fn matches(&self, d: &Document) -> bool {
            d.get_str("status").map(|s| s == "pending").unwrap_or(false)
        }
        fn compare(&self, a: &Document, b: &Document) -> Ordering {
            i64_of(a, "created").cmp(&i64_of(b, "created"))
        }
        fn apply(&self, d: &Document) -> std::result::Result<Option<Document>, String> {
            let mut next = d.clone();
            next.insert("status", "claimed");
            Ok(Some(next))
        }
        fn upsert(&self) -> Option<std::result::Result<Document, String>> {
            None
        }
        fn expected_stamp(&self) -> Option<Stamp> {
            self.expected
        }
    }

    fn stale_of(err: StorageError) -> Option<Stamp> {
        match err {
            StorageError::Stale { current } => current,
            other => panic!("expected Stale, got {other:?}"),
        }
    }

    #[test]
    fn a_conditional_modify_succeeds_at_the_current_stamp_and_moves_it() {
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);
        let (current, _) = engine.get_stamped(&coll, &DocId::Int64(2)).unwrap().unwrap();

        let spec = ConditionalClaim { expected: Some(current) };
        let out = engine.find_and_modify(&coll, &Candidates::Scan, &spec).unwrap();
        assert!(out.matched);
        let produced = out.stamp.expect("a write reports its stamp");
        assert_ne!(produced, current);
        assert_eq!(engine.get_stamped(&coll, &DocId::Int64(2)).unwrap().unwrap().0, produced);

        // Through `modify_where` too: the same body, so the same behaviour.
        let (current, _) = engine.get_stamped(&coll, &DocId::Int64(1)).unwrap().unwrap();
        let key = crate::docs::doc_key(&DocId::Int64(1)).unwrap();
        let spec = ConditionalClaim { expected: Some(current) };
        let out = engine.modify_where(&coll, &Candidates::Keys(vec![key]), &spec, Some(1)).unwrap();
        assert_eq!(out.modified, 1);
    }

    #[test]
    fn a_stale_stamp_aborts_writes_nothing_and_publishes_nothing() {
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);
        let (old, _) = engine.get_stamped(&coll, &DocId::Int64(2)).unwrap().unwrap();
        // Move the document on so `old` is stale.
        engine.find_and_modify(&coll, &Candidates::Scan, &claim()).unwrap();
        let (current, _) = engine.get_stamped(&coll, &DocId::Int64(2)).unwrap().unwrap();
        assert_ne!(current, old);
        // Put it back to pending so the conditional claim matches it again.
        let mut doc = engine.get(&coll, &DocId::Int64(2)).unwrap().unwrap();
        doc.insert("status", "pending");
        let current = engine.replace(&coll, &DocId::Int64(2), doc, false).unwrap().stamp.unwrap();

        let mut rx = engine.subscribe();
        let before = engine.commits();
        let spec = ConditionalClaim { expected: Some(old) };
        let err = engine.find_and_modify(&coll, &Candidates::Scan, &spec).unwrap_err();
        assert_eq!(stale_of(err), Some(current), "the refusal names the current stamp");
        assert_eq!(engine.commits() - before, 0);
        assert!(rx.try_recv().is_err(), "a refused write publishes nothing");
        assert_eq!(
            engine.get(&coll, &DocId::Int64(2)).unwrap().unwrap().get_str("status").unwrap(),
            "pending"
        );

        // `modify_where` refuses the same way, even mid-batch: nothing lands.
        let key = crate::docs::doc_key(&DocId::Int64(2)).unwrap();
        let err =
            engine.modify_where(&coll, &Candidates::Keys(vec![key]), &spec, None).unwrap_err();
        assert_eq!(stale_of(err), Some(current));
        assert_eq!(engine.commits() - before, 0);
    }

    #[test]
    fn expecting_a_version_of_a_document_that_is_gone_is_stale() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! {"_id": 1i64, "status": "done"}).unwrap();
        let (stamp, _) = engine.get_stamped(&coll, &DocId::Int64(1)).unwrap().unwrap();

        // Nothing matches (status is not pending) but a version was expected.
        let spec = ConditionalClaim { expected: Some(stamp) };
        let err = engine.find_and_modify(&coll, &Candidates::Scan, &spec).unwrap_err();
        assert_eq!(stale_of(err), None);
        let err = engine.modify_where(&coll, &Candidates::Scan, &spec, None).unwrap_err();
        assert_eq!(stale_of(err), None);

        // Without an expectation the same non-match is an ordinary no-op.
        let spec = ConditionalClaim { expected: None };
        assert!(!engine.find_and_modify(&coll, &Candidates::Scan, &spec).unwrap().matched);
    }

    #[test]
    fn racing_conditional_claims_have_exactly_one_winner() {
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let coll = engine.create_collection("app", "jobs").unwrap();
        engine.insert(&coll, doc! {"_id": 1i64, "created": 1i64, "status": "pending"}).unwrap();
        let (stamp, _) = engine.get_stamped(&coll, &DocId::Int64(1)).unwrap().unwrap();

        let mut handles = Vec::new();
        for _ in 0..8 {
            let engine = Arc::clone(&engine);
            let coll = coll.clone();
            handles.push(std::thread::spawn(move || {
                let spec = ConditionalClaim { expected: Some(stamp) };
                engine.find_and_modify(&coll, &Candidates::Scan, &spec).is_ok()
            }));
        }
        let wins = handles.into_iter().map(|h| h.join().unwrap()).filter(|w| *w).count();
        assert_eq!(wins, 1, "every racer named the same version; one may win");
    }

    /// Three single-document chunks over a shared engine, for the tests of
    /// what happens between a request's commits (ADR-192).
    fn three_chunks() -> (std::sync::Arc<Engine>, CollectionMeta, tempfile::TempDir) {
        let (engine, coll, dir) = engine();
        engine.set_multi_chunk_docs(1);
        let batch: Vec<Document> =
            (0..3i64).map(|id| doc! {"_id": id, "created": id, "status": "pending"}).collect();
        engine.insert_many(&coll, batch).unwrap();
        (std::sync::Arc::new(engine), coll, dir)
    }

    fn claimed(engine: &Engine, coll: &CollectionMeta) -> Vec<i64> {
        (0..3i64)
            .filter(|id| {
                let doc = engine.get(coll, &DocId::Int64(*id)).unwrap().unwrap();
                doc.get_str("status").unwrap() == "claimed"
            })
            .collect()
    }

    /// Hold the writer from another thread for `hold`, returning once it is
    /// held.
    fn hold_writer_for(engine: &std::sync::Arc<Engine>, hold: std::time::Duration) {
        let (held, is_held) = std::sync::mpsc::channel();
        let engine = std::sync::Arc::clone(engine);
        std::thread::spawn(move || {
            let guard = engine.hold_writer(WriterHolder::Bulk);
            held.send(()).unwrap();
            std::thread::sleep(hold);
            drop(guard);
        });
        is_held.recv().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_later_chunk_waits_out_a_writer_held_past_the_budget() {
        // The finding: chunk two used to give up after the request's budget
        // and answer "nothing was written" over a committed chunk one. Once a
        // chunk has landed, the request keeps going (ADR-192).
        let (engine, coll, _dir) = three_chunks();
        let budget = std::time::Duration::from_millis(100);
        let out = {
            let engine = std::sync::Arc::clone(&engine);
            let coll = coll.clone();
            crate::engine::with_write_wait_budget(budget, async move {
                let holder = std::sync::Arc::clone(&engine);
                crate::sync::race_hooks::at(
                    crate::sync::race_hooks::Race::BetweenChunks,
                    move || {
                        hold_writer_for(&holder, budget * 3);
                    },
                );
                engine.modify_where(&coll, &Candidates::Scan, &claim_all(), None)
            })
            .await
        };
        let out = out.expect("a later chunk waits for the writer without the budget");
        assert_eq!(out.commits, 3);
        assert_eq!(out.matched, 3);
        assert_eq!(engine.writer_wait_timeouts(), 0, "no wait gave up");
        assert_eq!(claimed(&engine, &coll), vec![0, 1, 2]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_first_chunk_that_cannot_get_the_writer_wrote_nothing() {
        // Before the first commit the budget still applies, and its refusal
        // is still true: nothing was written.
        let (engine, coll, _dir) = three_chunks();
        let budget = std::time::Duration::from_millis(100);
        hold_writer_for(&engine, budget * 3);
        let refused = {
            let engine = std::sync::Arc::clone(&engine);
            let coll = coll.clone();
            crate::engine::with_write_wait_budget(budget, async move {
                engine.modify_where(&coll, &Candidates::Scan, &claim_all(), None)
            })
            .await
        };
        assert!(matches!(refused, Err(StorageError::WriterBusy { .. })), "{refused:?}");
        assert!(claimed(&engine, &coll).is_empty());
    }

    #[test]
    fn a_node_past_its_drain_deadline_stops_between_chunks_and_says_what_landed() {
        let (engine, coll, _dir) = three_chunks();
        let stopper = std::sync::Arc::clone(&engine);
        crate::sync::race_hooks::at(crate::sync::race_hooks::Race::BetweenChunks, move || {
            stopper.set_stopping();
        });
        let failed = engine.modify_where(&coll, &Candidates::Scan, &claim_all(), None).unwrap_err();
        let StorageError::PartiallyApplied { applied, cause } = failed else {
            panic!("a stop after a commit is partly applied: {failed:?}");
        };
        assert_eq!(
            applied,
            crate::Applied::Modify { matched: 1, modified: 1, commits: 1, in_doubt: 0 }
        );
        assert!(
            matches!(*cause, StorageError::Stopping(crate::StopReason::DrainDeadline)),
            "{cause:?}"
        );
        assert_eq!(claimed(&engine, &coll), vec![0], "chunk one stands, and nothing after it");
    }

    #[test]
    fn a_stop_does_not_touch_a_request_that_has_committed_nothing() {
        // The flag bounds the later transactions of a request, not its first.
        let (engine, coll, _dir) = three_chunks();
        engine.set_stopping();
        let out = engine.modify_where(&coll, &Candidates::Scan, &claim_all(), Some(1)).unwrap();
        assert_eq!(out.commits, 1);
    }

    #[test]
    fn a_later_chunk_whose_commit_outcome_is_unknown_is_counted_in_doubt() {
        // Armed after the first commit, the next fsync fails: that chunk may
        // or may not be on disk, and the answer keeps it apart from what is.
        let (engine, coll, _dir) = three_chunks();
        let armer = std::sync::Arc::clone(&engine);
        crate::sync::race_hooks::at(crate::sync::race_hooks::Race::BetweenChunks, move || {
            assert!(armer.arm_test_storage_failure("sync_data"));
        });
        let failed = engine.modify_where(&coll, &Candidates::Scan, &claim_all(), None).unwrap_err();
        let StorageError::PartiallyApplied { applied, cause } = failed else {
            panic!("an unknown commit after a known one is partly applied: {failed:?}");
        };
        assert_eq!(
            applied,
            crate::Applied::Modify { matched: 1, modified: 1, commits: 1, in_doubt: 1 }
        );
        assert!(matches!(*cause, StorageError::OutcomeUnknown(_)), "{cause:?}");
    }

    #[test]
    fn a_multi_write_that_starts_past_the_drain_deadline_commits_one_chunk_and_says_so() {
        // The stop bounds a request's later transactions, not its first: one
        // begun after the deadline commits its first chunk, then stops.
        let (engine, coll, _dir) = three_chunks();
        engine.set_stopping();
        let failed =
            engine.modify_where(&coll, &Candidates::Scan, &claim_all(), Some(2)).unwrap_err();
        let StorageError::PartiallyApplied { applied, cause } = failed else {
            panic!("a stop after a commit is partly applied: {failed:?}");
        };
        assert_eq!(
            applied,
            crate::Applied::Modify { matched: 1, modified: 1, commits: 1, in_doubt: 0 }
        );
        assert!(
            matches!(*cause, StorageError::Stopping(crate::StopReason::DrainDeadline)),
            "{cause:?}"
        );
        assert_eq!(claimed(&engine, &coll), vec![0]);
    }
}
