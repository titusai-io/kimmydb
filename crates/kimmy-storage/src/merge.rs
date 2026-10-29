//! Writing a computed set of documents into a collection — what `$merge` and
//! `$out` do once a pipeline has produced them (ADR-255).
//!
//! # One write path
//!
//! Every document goes through the bodies an ordinary insert and an ordinary
//! replace use ([`Engine::insert_in_txn`] and [`Engine::replace_in_txn`]), so
//! the size and id caps, the unique and other indexes, the stamp, the oplog
//! entry and the change-stream publish are the ones a client write has. There
//! is no second implementation to keep in step with them.
//!
//! # Chunks, and what a failure leaves
//!
//! The documents are written in transactions of at most
//! [`Engine::multi_chunk_docs`], the way a `multi` update is: a failure aborts
//! the chunk it is in and leaves the earlier ones committed, published and
//! replicating. Once one chunk has committed the request goes on waiting for
//! the writer without a budget, and any failure after it is
//! [`StorageError::PartiallyApplied`] carrying what had landed (ADR-192). It is
//! **not atomic**, and cannot be: a single transaction over a pipeline's whole
//! output would hold the single writer for as long as the output is long.
//!
//! # Why the key is `_id`
//!
//! A match is by `_id`, decided inside the write transaction against the
//! stored document. The store has no enforced uniqueness on any other key
//! across members (ADR-006), so two members merging on another key at the same
//! time would each find nothing and each insert. `_id` is the one key the
//! storage layer itself keeps unique per member and converges per document.

use bson::Document;
use kimmy_core::{DocId, OplogEntry};
use redb::ReadableTable;

use crate::docs::{doc_key, extract_id};
use crate::engine::WriterHolder;
use crate::error::{Result, StorageError};
use crate::meta::CollectionMeta;
use crate::{Engine, ID_FIELD, codec, tables};

/// What a document that matches a stored one (by `_id`) does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WhenMatched {
    /// The stored document is replaced whole; its `_id` is kept.
    Replace,
    /// The incoming document's top-level fields are laid over the stored one:
    /// fields only the stored document has are kept, the others overwritten.
    Merge,
    /// The stored document is left alone.
    KeepExisting,
}

/// How a computed document is written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SinkMode {
    /// Insert every document; one whose `_id` is already stored fails the
    /// chunk it is in. A duplicate `_id` among the documents is refused before
    /// anything is written.
    Insert,
    /// Match each document to the stored one with its `_id`. A document with no
    /// `_id` cannot match.
    Merge { when_matched: WhenMatched, insert_unmatched: bool },
}

/// What a write did, by the decision taken for each document.
///
/// These count decisions, not changed bytes: a document merged over a stored
/// one that already held its values is `merged` all the same, so
/// `matched == replaced + merged + kept` always holds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SinkCounts {
    pub matched: u64,
    pub inserted: u64,
    pub replaced: u64,
    pub merged: u64,
    pub kept: u64,
    pub discarded: u64,
}

impl SinkCounts {
    fn add(&mut self, other: &SinkCounts) {
        self.matched += other.matched;
        self.inserted += other.inserted;
        self.replaced += other.replaced;
        self.merged += other.merged;
        self.kept += other.kept;
        self.discarded += other.discarded;
    }
}

/// The answer of [`Engine::write_documents`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SinkOutcome {
    pub counts: SinkCounts,
    /// Write transactions that committed something. A chunk that wrote nothing
    /// (everything kept, discarded or unchanged) commits nothing.
    pub commits: u64,
}

impl Engine {
    /// Write `docs` into `coll` as `mode` says, in chunks of
    /// [`Engine::multi_chunk_docs`] documents; see the module documentation.
    pub fn write_documents(
        &self,
        coll: &CollectionMeta,
        docs: Vec<Document>,
        mode: SinkMode,
    ) -> Result<SinkOutcome> {
        self.check_output_ids(&docs)?;
        if mode == SinkMode::Insert {
            refuse_duplicate_ids(&docs)?;
        }
        let chunk = self.multi_chunk_docs();
        let mut outcome = SinkOutcome::default();
        let mut rest = docs.into_iter();
        loop {
            let part: Vec<Document> = rest.by_ref().take(chunk).collect();
            if part.is_empty() {
                return Ok(outcome);
            }
            match self.write_chunk(outcome.commits > 0, coll, part, mode) {
                Ok((counts, entries)) => {
                    outcome.counts.add(&counts);
                    if !entries.is_empty() {
                        outcome.commits += 1;
                        // Published per chunk, after its commit, as a
                        // multi update's are.
                        self.publish(entries);
                    }
                }
                Err((e, _)) if outcome.commits == 0 => return Err(e),
                Err((e, in_doubt)) => {
                    return Err(StorageError::PartiallyApplied {
                        applied: crate::Applied::Write {
                            counts: outcome.counts,
                            commits: outcome.commits,
                            in_doubt,
                        },
                        cause: Box::new(e),
                    });
                }
            }
        }
    }

    /// Refuse the whole output if any document's `_id` could not be stored: a
    /// type no `_id` may have, or one over the size cap. Every document is in
    /// memory before the first write, so a client error in the pipeline's own
    /// output is answered as one, with nothing written and, when the caller asks
    /// first, nothing created. A document with no `_id` is given one on insert.
    pub fn check_output_ids(&self, docs: &[Document]) -> Result<()> {
        for doc in docs {
            if let Some(value) = doc.get(ID_FIELD) {
                let id = DocId::try_from_bson(value)?;
                if self.caps_apply() {
                    kimmy_core::limits::check_id(&id)?;
                }
            }
        }
        Ok(())
    }

    /// Refuse an output in which two documents carry one `_id`, as `write_documents`
    /// does before its first chunk; the caller runs it before creating a target.
    pub fn check_unique_output_ids(&self, docs: &[Document]) -> Result<()> {
        refuse_duplicate_ids(docs)
    }

    /// One transaction of [`Self::write_documents`]: the counts of the
    /// decisions taken and the entries to publish. A failure comes with the
    /// number of documents whose commit is in doubt, as in `modify_chunk`.
    fn write_chunk(
        &self,
        continuing: bool,
        coll: &CollectionMeta,
        part: Vec<Document>,
        mode: SinkMode,
    ) -> std::result::Result<(SinkCounts, Vec<OplogEntry>), (StorageError, u64)> {
        fn none(e: impl Into<StorageError>) -> (StorageError, u64) {
            (e.into(), 0)
        }
        let txn = if continuing {
            self.begin_write_continuing(WriterHolder::Bulk)
        } else {
            self.begin_write(WriterHolder::Bulk)
        }
        .map_err(none)?;

        let mut counts = SinkCounts::default();
        let mut entries = Vec::with_capacity(part.len());
        for doc in part {
            if let Err(e) = self.write_one(&txn, coll, doc, mode, &mut counts, &mut entries) {
                txn.abort().map_err(none)?;
                return Err(none(e));
            }
        }
        if entries.is_empty() {
            // Nothing written: nothing to commit, mint or publish (ADR-208).
            txn.abort().map_err(none)?;
            return Ok((counts, entries));
        }
        match txn.commit() {
            Ok(()) => Ok((counts, entries)),
            Err(e @ StorageError::OutcomeUnknown(_)) => Err((e, entries.len() as u64)),
            Err(e) => Err(none(e)),
        }
    }

    fn write_one(
        &self,
        txn: &crate::engine::WriteTxn<'_>,
        coll: &CollectionMeta,
        doc: Document,
        mode: SinkMode,
        counts: &mut SinkCounts,
        entries: &mut Vec<OplogEntry>,
    ) -> Result<()> {
        let SinkMode::Merge { when_matched, insert_unmatched } = mode else {
            entries.push(self.insert_in_txn(txn, coll, doc)?.1);
            counts.inserted += 1;
            return Ok(());
        };
        let stored = match doc.get(ID_FIELD) {
            Some(value) => {
                let id = DocId::try_from_bson(value)?;
                stored_document(txn, coll, &id)?.map(|stored| (id, stored))
            }
            None => None,
        };
        let Some((id, stored)) = stored else {
            if insert_unmatched {
                entries.push(self.insert_in_txn(txn, coll, doc)?.1);
                counts.inserted += 1;
            } else {
                counts.discarded += 1;
            }
            return Ok(());
        };
        counts.matched += 1;
        let next = match when_matched {
            WhenMatched::KeepExisting => {
                counts.kept += 1;
                return Ok(());
            }
            WhenMatched::Replace => {
                counts.replaced += 1;
                doc
            }
            WhenMatched::Merge => {
                counts.merged += 1;
                let mut next = stored;
                for (field, value) in doc {
                    if field != ID_FIELD {
                        next.insert(field, value);
                    }
                }
                next
            }
        };
        let (_, entry) = self.replace_in_txn(txn, coll, &id, next, false, None)?;
        entries.extend(entry);
        Ok(())
    }
}

/// The live document with `id`, read in the write transaction so that an
/// earlier write of the same chunk is seen.
fn stored_document(
    txn: &crate::engine::WriteTxn<'_>,
    coll: &CollectionMeta,
    id: &DocId,
) -> Result<Option<Document>> {
    let key = doc_key(id)?;
    let docs = txn.open_table(tables::DOCS)?;
    match docs.get((coll.id.0, key.as_slice()))? {
        Some(raw) => Ok(codec::decode_doc_record(raw.value())?.document()?),
        None => Ok(None),
    }
}

/// Two documents with one `_id` cannot both be inserted, and finding out in the
/// chunk that holds the second would leave the first one's chunk written.
fn refuse_duplicate_ids(docs: &[Document]) -> Result<()> {
    let mut seen = std::collections::HashSet::with_capacity(docs.len());
    for (index, doc) in docs.iter().enumerate() {
        if doc.get(ID_FIELD).is_none() {
            continue;
        }
        let id = extract_id(doc)?;
        if !seen.insert(doc_key(&id)?) {
            return Err(kimmy_core::Error::DuplicateKey(format!(
                "{} (the document at index {index} repeats an earlier `_id`)",
                doc.get(ID_FIELD).map_or_else(|| id.to_string(), ToString::to_string)
            ))
            .into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bson::{Bson, doc};

    use super::*;

    fn engine() -> (Engine, CollectionMeta, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let c = e.create_collection("app", "rollup").unwrap();
        (e, c, dir)
    }

    const MERGE: SinkMode =
        SinkMode::Merge { when_matched: WhenMatched::Replace, insert_unmatched: true };

    #[test]
    fn an_id_no_document_may_have_refuses_the_whole_output_in_both_modes() {
        let (engine, coll, _dir) = engine();
        engine.set_multi_chunk_docs(2);
        let bad_ids = [
            Bson::Null,
            Bson::Double(2.5),
            Bson::Document(doc! { "d": "mon" }),
            Bson::Array(vec![Bson::Int32(1)]),
        ];
        for mode in [SinkMode::Insert, MERGE] {
            for bad in &bad_ids {
                let docs = vec![
                    doc! { "_id": 1, "n": 1 },
                    doc! { "_id": 2, "n": 2 },
                    doc! { "_id": 3, "n": 3 },
                    doc! { "_id": bad.clone() },
                ];
                let before = engine.commits();
                let err = engine.write_documents(&coll, docs, mode).unwrap_err();
                assert!(
                    matches!(err, StorageError::Core(kimmy_core::Error::InvalidDocumentId { .. })),
                    "{mode:?} {bad:?}: {err:?}"
                );
                assert_eq!(engine.commits(), before, "{mode:?} {bad:?}: nothing committed");
                assert!(engine.get(&coll, &DocId::Int64(1)).unwrap().is_none());
            }
        }
    }

    #[test]
    fn the_allowed_id_types_are_written_and_a_missing_id_is_minted() {
        let (engine, coll, _dir) = engine();
        let docs = vec![
            doc! { "_id": bson::oid::ObjectId::new() },
            doc! { "_id": "s" },
            doc! { "_id": 7_i32 },
            doc! { "_id": 8_i64 },
            doc! { "_id": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![1, 2] }) },
            doc! { "_id": Bson::Decimal128(kimmy_core::decimal::parse("2.5").unwrap()) },
            doc! { "n": 1 },
        ];
        let out = engine.write_documents(&coll, docs, MERGE).unwrap();
        assert_eq!(out.counts.inserted, 7);
    }
}
