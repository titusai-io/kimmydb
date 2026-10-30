//! Vector configuration lifecycle and shadow collections.
//!
//! Enabling vectors on a collection creates a companion collection named
//! `{collection}.__vectors`. It is an ordinary collection — same durability,
//! same oplog, same eventual replication — rather than a parallel storage
//! mechanism that would need its own correctness argument.
//!
//! The `__` segment prefix is reserved for system objects, so a user cannot
//! create a collection that shadows one.

use std::collections::HashMap;

use kimmy_core::{
    ChunkSource, CollectionId, DocId, Error as CoreError, Hlc, ResumeToken, Unstamped,
    VectorConfig, VectorRecord, vector_meta,
};
use tracing::info;

use redb::{ReadableDatabase, ReadableTable};

use crate::docs::WriteScope;
use crate::engine::{WriteTxn, WriterHolder};
use crate::error::{Result, StorageError};
use crate::meta::CollectionMeta;
use crate::walk::WalkScope;

/// What turning vectors off did; see [`Engine::disable_vectors_reporting`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VectorsOff {
    /// The configuration was on and this call turned it off.
    pub disabled: bool,
    /// This call dropped the stored vectors.
    pub dropped_vectors: bool,
}

/// One document's chunk set, encoded for its shadow collection ahead of any
/// writer being taken.
///
/// A scope's contract (ADR-149) is to hold its inputs ready and do nothing
/// but write them, since every other writer on the node waits behind it; so
/// the BSON encoding of each chunk record happens here, before
/// [`crate::Engine::write_batch`] is opened, and [`WriteScope::put_vectors`]
/// takes the result. The ids are kept beside the documents so the scope can
/// decide which stored chunks are not rewritten without decoding anything.
pub struct VectorWrite {
    /// The document, as its key identifies it.
    document: DocId,
    docs: Vec<(DocId, bson::Document)>,
}

impl VectorWrite {
    /// Encode `records` — the complete new chunk set of the document keyed
    /// `document` — for the shadow collection, each at
    /// [`VectorRecord::id`]`(source, chunk)` (ADR-239). `source` is the
    /// document's chunk source ([`ChunkSource::from`]); one that is not the
    /// document's is refused, since its chunks would sit outside the
    /// document's run, where no read of it looks.
    pub fn encode(
        document: &DocId,
        source: &ChunkSource,
        records: &[VectorRecord],
    ) -> Result<Self> {
        if !source.is_of(document) {
            return Err(StorageError::Corrupt(format!(
                "a chunk source {source:?} that is not the document {document:?}'s"
            )));
        }
        // A record that names another document would sit in this one's run
        // for good, as debris it is re-embedded over.
        debug_assert!(
            records.iter().all(|record| record.source == *document),
            "every record of a document's chunk set names that document"
        );
        let docs = records
            .iter()
            .map(|record| {
                let doc = bson::serialize_to_document(record)
                    .map_err(|e| StorageError::Corrupt(format!("encoding vector record: {e}")))?;
                Ok((VectorRecord::id(source, record.chunk), doc))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { document: document.clone(), docs })
    }
}

/// Where a vector configuration comes from, which decides how the shadow
/// collection it needs is created if it is missing (ADR-178).
#[derive(Clone, Copy, Debug)]
pub(crate) enum Configured {
    /// A client's configuration on this member. The shadow is created at a
    /// stamp minted here and logged, before the configuration's own entry,
    /// so a peer applying the window creates it from that entry, at that
    /// stamp, like any collection.
    Locally,
    /// A peer's `ConfigureVectors` entry, stamped so. A shadow missing here is
    /// created **at that stamp, unlogged**, and not at all when this member
    /// holds a newer tombstone for it. Never at a stamp minted here: a stamp
    /// later than the shadow's drop outlived the drop, and the shadow it
    /// created replicated back to the members that had dropped it.
    FromEntry(kimmy_core::Stamp),
    /// A snapshot page's definition, with the stamp of the change it describes
    /// (`CollectionState::vector_changed`). The shadow is its own collection on the
    /// page, restored at its own stamp, and is not created here.
    FromSnapshot(kimmy_core::Stamp),
    /// A late-drop sweep turning off a configuration of the life before (ADR-231),
    /// at the stamp given: the drop's, or the change's own where that sorts above the
    /// drop's in the drop's own `Hlc` (ADR-236: a tie is the drop's). Judged by
    /// last-writer-wins strictly: a configuration at that stamp is turned off (the drop's
    /// stamp alone would read as older than such a change and keep it on this member
    /// alone), and one changed above it since the sweep read it is kept. Only ever turns
    /// a configuration off.
    Swept(kimmy_core::Stamp),
}

impl Configured {
    /// Whether this change comes **dated from another member**: a snapshot page's
    /// (a winner by stamp, from a sender whose own coverage names it, ADR-163,
    /// ADR-226) or a replicated entry's, so the history behind it is the origin's
    /// and cannot be replayed here. Never a local change, which mints its own stamp
    /// and is a client's request to be refused or not on this member's state.
    fn is_dated_from_elsewhere(self) -> bool {
        matches!(self, Self::FromSnapshot(_) | Self::FromEntry(_))
    }

    /// The stamp of the change this applies, when it comes with one from another
    /// member: what the standing state is judged against (ADR-226). A local change
    /// mints its own, under the writer.
    fn stamp(self) -> Option<kimmy_core::Stamp> {
        match self {
            Self::Locally => None,
            Self::FromEntry(stamp) | Self::Swept(stamp) | Self::FromSnapshot(stamp) => Some(stamp),
        }
    }
}

impl crate::Engine {
    /// Enable or replace auto-embedding for a collection.
    ///
    /// Creates the shadow collection if needed. Rejects an invalid
    /// configuration up front, so a typo surfaces here rather than on the first
    /// document write.
    pub fn configure_vectors(
        &self,
        db: &str,
        collection: &str,
        config: VectorConfig,
    ) -> Result<CollectionMeta> {
        self.configure_vectors_inner(db, collection, config, Configured::Locally, &|_| Ok(false))
    }

    /// `log = false` when applying a replicated configuration. See
    /// `create_index_inner` for why a replicated change must not mint an entry.
    pub(crate) fn configure_vectors_inner(
        &self,
        db: &str,
        collection: &str,
        config: VectorConfig,
        by: Configured,
        history: &dyn Fn(&CollectionMeta) -> Result<bool>,
    ) -> Result<CollectionMeta> {
        // **A loop, not a self-call.** Every retry passes the same arguments, so
        // a lost race is a `continue`. It was a self-call, which made an
        // unbounded retry a stack overflow — and a bound low enough to protect
        // the stack (sixteen) fired under ordinary concurrent DDL, because K
        // changes to one collection queue at the writer gate and the last loses
        // K−1 races with nothing wrong at all. A loop cannot overflow, so the
        // bound below is only there to stop a check that can never pass.
        for _ in 0..crate::Engine::MAX_DEFINITION_RETRIES {
            let log = matches!(by, Configured::Locally);
            config.validate().map_err(|e| StorageError::Core(CoreError::InvalidQuery(e)))?;

            // A shadow collection holds vectors, not documents; configuring
            // embeddings on one would be a recursive absurdity.
            if vector_meta::is_shadow(collection) {
                return Err(StorageError::Core(CoreError::InvalidName {
                    name: collection.to_string(),
                    reason: "vectors cannot be configured on a shadow collection",
                }));
            }

            let mut meta = self.get_collection(db, collection)?;
            let read = meta.clone();
            // A configuration of a life of the collection that has since been
            // dropped and recreated here is history, and is not applied to the
            // life that stands. Judged on the definition read here, which the
            // writer's `definition_is` below holds unchanged, with the drop
            // tombstones held as it is judged (`definition_of_an_ended_life`), and
            // before the shadow is created, so history mints no shadow either. A
            // drop that lands after the judgement changes the definition (a burial,
            // a re-dating) or records a tombstone its own sweep then judges.
            if history(&meta)? {
                #[cfg(test)]
                crate::sync::race_hooks::absorbed(crate::sync::race_hooks::Race::VectorHistory);
                return Ok(meta);
            }
            #[cfg(test)]
            crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::VectorConfiguration);
            // Last writer wins, on the stamp (ADR-226): a change at or below the one
            // standing here is the same change arriving again, or an older one
            // arriving late, and is not applied. An unstamped standing state reads as
            // older than any stamped change.
            if by.stamp().is_some_and(|incoming| meta.vector_changed.is_some_and(|c| c >= incoming))
            {
                return Ok(meta);
            }

            // A dimension change is safe exactly when the server can rebuild the
            // vectors itself: the embedding worker treats every ConfigureVectors
            // entry as a reindex trigger and re-embeds from the documents, and
            // both search paths skip records whose width does not match, so the
            // old vectors are invisible while the backfill replaces them.
            //
            // For byo the server holds vectors it can never regenerate — the
            // client computed them — so a changed width there still requires an
            // explicit drop, or search would quietly serve from a shrinking
            // remnant of the old width while nothing replaces it.
            //
            // **Not for a dated change from elsewhere** (ADR-227): a page's, or an entry's.
            // The origin cannot have made this change in place either: it turned the
            // configuration off between the two, and the entries that say so are what a
            // page stands in for, and are what an entry that arrives without them (the off
            // lost on its stamp to a change made here, or not yet applied) presupposes.
            // Applied here as that history reads: the configuration off and on again,
            // one change, in this transaction, so the dimension changes with nothing
            // to drop and no window in which the collection is without one. The stored vectors are left as they
            // are, as for an entry's off (`drop_vectors` is never a peer's to decide), and
            // the new width skips them. Judged below the history check and the
            // last-writer-wins guard, so a change that loses changes nothing, and for a
            // dated one only: a page with no stamp is not a winner and never overrides,
            // and a local change is refused as it always was.
            if let Some(existing) = &meta.vector
                && existing.dim != config.dim
                && !config.provider.embeds_server_side()
                && !by.is_dated_from_elsewhere()
            {
                return Err(StorageError::Core(CoreError::InvalidQuery(format!(
                    "vector dimension cannot change from {} to {} in place for client-supplied \
                 vectors; drop the vector configuration first, which discards them",
                    existing.dim, config.dim
                ))));
            }

            // The shadow is created in the same transaction as the configuration
            // it serves, so neither is ever durable without the other: minted
            // before the configuration's own commit, one that turned out history,
            // or whose collection was dropped in between, left a shadow whose
            // creation replicated; minted after it, a crash between the two left
            // a configuration the embedding worker skips for want of a shadow.
            let shadow = vector_meta::shadow_name(collection);
            let shadow_missing = match self.get_collection(db, &shadow) {
                Ok(_) => false,
                Err(StorageError::Core(CoreError::CollectionNotFound { .. })) => true,
                Err(e) => return Err(e),
            };
            // A dropped shadow's rows refuse the shadow's creation, inside its
            // transaction (`create_collection_in_txn`, ADR-189); a shadow
            // standing is this collection's own, and re-enabling over it is
            // not refused.
            if shadow_missing {
                #[cfg(test)]
                crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::SystemCreate);
            }

            meta.vector = Some(config.clone());
            let txn = self.begin_write(WriterHolder::Ddl)?;
            #[cfg(test)]
            crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::VectorsWriting);
            // The shadow first, so a local creation is logged before the
            // configuration, the order a peer applying the window meets them in.
            let created = match by {
                Configured::Locally => Some(self.create_collection_in_txn(
                    &txn,
                    db,
                    &shadow,
                    true,
                    None,
                    &|_| false,
                    || self.next_stamp(),
                    None,
                )),
                Configured::FromEntry(stamp) => Some(self.create_collection_in_txn(
                    &txn,
                    db,
                    &shadow,
                    false,
                    Some(stamp.hlc),
                    &|dropped| crate::sync::creation_is_history(stamp.hlc, dropped.hlc),
                    || stamp,
                    None,
                )),
                // A sweep only turns a configuration off, and never configures.
                Configured::FromSnapshot(_) | Configured::Swept(_) => None,
            };
            let shadow_entry = match created {
                Some(Ok(crate::engine::InTxn::Created(_, entry))) => entry,
                Some(Ok(crate::engine::InTxn::Exists)) => {
                    // Made by a concurrent configuration of this collection since
                    // it was found missing: the shadow this one wanted.
                    #[cfg(test)]
                    if shadow_missing {
                        crate::sync::race_hooks::absorbed(
                            crate::sync::race_hooks::Race::SystemCreate,
                        );
                    }
                    None
                }
                // A peer's configuration older than the shadow's drop here: the
                // configuration still applies, and the drop that came after it
                // stands.
                Some(Ok(crate::engine::InTxn::History)) | None => None,
                Some(Err(e)) => {
                    txn.abort()?;
                    return Err(e);
                }
            };
            if !crate::Engine::definition_is(&txn, &read)? {
                txn.abort()?;
                #[cfg(test)]
                crate::sync::race_hooks::absorbed(
                    crate::sync::race_hooks::Race::VectorConfiguration,
                );
                continue;
            }
            // The stamp of this change: minted here, after the shadow's, for a local
            // one, and the entry's own for a peer's.
            let stamp = match by {
                Configured::Locally => Some(self.next_stamp_above(meta.vector_changed)),
                other => other.stamp(),
            };
            meta.vector_changed = stamp;
            crate::Engine::put_collection_meta(&txn, &meta)?;
            // The configuration changed here (set or turned off): a refusal of it
            // is mended, and must not explain a later loss.
            crate::Engine::clear_refusal_hints_for_in(
                &txn,
                meta.id,
                &crate::definition_digest::DefinitionKey::vector(),
            )?;

            let logged = if log {
                let entry = crate::engine::ddl_entry(
                    stamp.expect("a local change mints its stamp"),
                    kimmy_core::OpKind::ConfigureVectors,
                    meta.id,
                    &kimmy_core::VectorSet {
                        db: db.to_string(),
                        collection: collection.to_string(),
                        config: Some(config),
                    },
                )?;
                crate::engine::append_oplog(&txn, &entry)?;
                Some(entry)
            } else {
                None
            };
            txn.commit()?;
            #[cfg(test)]
            crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::VectorsCommitted);
            let published: Vec<_> = shadow_entry.into_iter().chain(logged).collect();
            if !published.is_empty() {
                self.publish(published);
            }

            info!(db, collection, shadow = %shadow, "configured auto-embedding");
            return Ok(meta);
        }
        // Unreachable under contention: see `MAX_DEFINITION_RETRIES`.
        Err(crate::Engine::retries_exhausted_error(db, collection))
    }

    /// Turn off auto-embedding, optionally discarding the vectors.
    ///
    /// Keeping them by default means re-enabling with the same settings does
    /// not force a full re-embed, which for a remote provider is a real cost.
    pub fn disable_vectors(&self, db: &str, collection: &str, drop_vectors: bool) -> Result<bool> {
        Ok(self.disable_vectors_reporting(db, collection, drop_vectors)?.disabled)
    }

    /// [`Self::disable_vectors`], saying what this call did: whether it turned
    /// the configuration off, and whether it dropped the stored vectors.
    ///
    /// With `drop_vectors` both happen in one transaction (ADR-192), so no
    /// answer can report one without the other having landed. A call on a
    /// collection whose vectors are already off drops a shadow that is still
    /// there — what a partial disable by an earlier build left — and reports
    /// that it did.
    pub fn disable_vectors_reporting(
        &self,
        db: &str,
        collection: &str,
        drop_vectors: bool,
    ) -> Result<VectorsOff> {
        let off =
            self.disable_vectors_inner(db, collection, drop_vectors, Configured::Locally, &|_| {
                Ok(false)
            })?;
        if off.disabled || !drop_vectors {
            return Ok(off);
        }
        #[cfg(test)]
        crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::LeftoverShadow);
        let dropped_vectors = self.drop_leftover_shadow(db, collection)?;
        Ok(VectorsOff { disabled: false, dropped_vectors })
    }

    /// Drop the shadow of a collection whose vectors are off, in one
    /// transaction that decides it under the writer: only while the parent
    /// still stands with its configuration off. Decided outside it, a
    /// configuration turned back on in between — which adopts the kept
    /// shadow — lost its vectors, leaving vectors on and the shadow gone.
    fn drop_leftover_shadow(&self, db: &str, collection: &str) -> Result<bool> {
        let shadow = vector_meta::shadow_name(collection);
        let txn = self.begin_write(WriterHolder::Ddl)?;
        let off = {
            let collections = txn.open_table(crate::tables::COLLECTIONS)?;
            match collections.get((db, collection))? {
                Some(raw) => {
                    let parent: CollectionMeta = serde_json::from_slice(raw.value())?;
                    parent.vector.is_none()
                }
                // A parent gone takes its shadow with it; nothing is left to
                // this call.
                None => false,
            }
        };
        let burial = if off { self.bury_in_txn(&txn, db, &shadow, None) } else { Ok(None) };
        let burial = match burial {
            Ok(Some(burial)) => burial,
            Ok(None) => {
                txn.abort()?;
                return Ok(false);
            }
            Err(e) => {
                txn.abort()?;
                return Err(e);
            }
        };
        txn.commit()?;
        self.finish_burial(db, &shadow, burial);
        Ok(true)
    }

    pub(crate) fn disable_vectors_inner(
        &self,
        db: &str,
        collection: &str,
        drop_vectors: bool,
        by: Configured,
        history: &dyn Fn(&CollectionMeta) -> Result<bool>,
    ) -> Result<VectorsOff> {
        let log = matches!(by, Configured::Locally);
        // **A loop, not a self-call.** Every retry passes the same arguments, so
        // a lost race is a `continue`. It was a self-call, which made an
        // unbounded retry a stack overflow — and a bound low enough to protect
        // the stack (sixteen) fired under ordinary concurrent DDL, because K
        // changes to one collection queue at the writer gate and the last loses
        // K−1 races with nothing wrong at all. A loop cannot overflow, so the
        // bound below is only there to stop a check that can never pass.
        for _ in 0..crate::Engine::MAX_DEFINITION_RETRIES {
            let mut meta = self.get_collection(db, collection)?;
            let read = meta.clone();
            // As `configure_vectors_inner`: turning vectors off for a life that
            // has since been dropped and recreated here is history.
            if history(&meta)? {
                return Ok(VectorsOff { disabled: false, dropped_vectors: false });
            }
            #[cfg(test)]
            crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::VectorRemoval);
            // Last writer wins, on the stamp (ADR-226), as for a configuration. A sweep's
            // turning off a configuration of the life before is judged strictly: the
            // configuration at the swept stamp itself (a change in the drop's own `Hlc`,
            // passed at its own stamp) is the drop's and goes, and one changed above it
            // since the sweep read it (a client's, or a replicated change stamped after
            // the drop, landing before the sweep takes the writer) is the newer life's
            // and stays, as it does on every other member.
            let swept = matches!(by, Configured::Swept(_));
            if by.stamp().is_some_and(|incoming| {
                meta.vector_changed
                    .is_some_and(|c| if swept { c > incoming } else { c >= incoming })
            }) {
                return Ok(VectorsOff { disabled: false, dropped_vectors: false });
            }
            // Already off. A **local** call is still a change: it is stamped and logged,
            // so the stamp is in this member's servable vector and a snapshot page
            // carrying it passes the receiver's coverage gate (ADR-163, ADR-226); a
            // retried disable therefore writes where it used to write nothing. A
            // peer's, or a page's, only records the stamp. Neither buries a shadow
            // (the caller drops a leftover one itself) or reports a configuration
            // turned off.
            let already_off = meta.vector.is_none();
            if already_off && by.stamp().is_none() && !log {
                return Ok(VectorsOff { disabled: false, dropped_vectors: false });
            }

            meta.vector = None;
            let txn = self.begin_write(WriterHolder::Ddl)?;
            // The definition written back was read before the writer
            // (`Engine::definition_is`).
            if !crate::Engine::definition_is(&txn, &read)? {
                txn.abort()?;
                #[cfg(test)]
                crate::sync::race_hooks::absorbed(crate::sync::race_hooks::Race::VectorRemoval);
                continue;
            }
            let stamp = match by {
                Configured::Locally => Some(self.next_stamp_above(meta.vector_changed)),
                other => other.stamp(),
            };
            meta.vector_changed = stamp;
            crate::Engine::put_collection_meta(&txn, &meta)?;
            // The configuration changed here (set or turned off): a refusal of it
            // is mended, and must not explain a later loss.
            crate::Engine::clear_refusal_hints_for_in(
                &txn,
                meta.id,
                &crate::definition_digest::DefinitionKey::vector(),
            )?;

            let logged = if log {
                let entry = crate::engine::ddl_entry(
                    stamp.expect("a local change mints its stamp"),
                    kimmy_core::OpKind::ConfigureVectors,
                    meta.id,
                    &kimmy_core::VectorSet {
                        db: db.to_string(),
                        collection: collection.to_string(),
                        config: None,
                    },
                )?;
                crate::engine::append_oplog(&txn, &entry)?;
                Some(entry)
            } else {
                None
            };
            // The stored vectors go in the same transaction, buried as a drop
            // of the shadow by name, with its own tombstone and its own
            // `DropCollection` entry, so a peer drops its copy too. Its own
            // commit used to follow this one, and a failure between the two
            // answered an error for a configuration change that had landed
            // (ADR-192).
            let shadow = if drop_vectors && !already_off {
                let burial =
                    self.bury_in_txn(&txn, db, &vector_meta::shadow_name(collection), None);
                match burial {
                    Ok(burial) => burial,
                    Err(e) => {
                        txn.abort()?;
                        return Err(e);
                    }
                }
            } else {
                None
            };
            txn.commit()?;
            if let Some(entry) = logged {
                self.publish(vec![entry]);
            }
            let dropped_vectors = shadow.is_some();
            if let Some(burial) = shadow {
                self.finish_burial(db, &vector_meta::shadow_name(collection), burial);
            }
            if already_off {
                return Ok(VectorsOff { disabled: false, dropped_vectors: false });
            }
            info!(db, collection, drop_vectors, "disabled auto-embedding");
            return Ok(VectorsOff { disabled: true, dropped_vectors });
        }
        // Unreachable under contention: see `MAX_DEFINITION_RETRIES`.
        Err(crate::Engine::retries_exhausted_error(db, collection))
    }

    /// The shadow collection holding a collection's vectors, if configured.
    pub fn vector_collection(&self, db: &str, collection: &str) -> Result<Option<CollectionMeta>> {
        let meta = self.get_collection(db, collection)?;
        if meta.vector.is_none() {
            return Ok(None);
        }
        Ok(Some(self.get_collection(db, &vector_meta::shadow_name(collection))?))
    }

    /// Collections in a database that have embedding enabled.
    ///
    /// The embedding worker uses this to know what to watch. Shadow
    /// collections are excluded, so it cannot try to embed its own output.
    pub fn vector_enabled_collections(&self, db: &str) -> Result<Vec<CollectionMeta>> {
        Ok(self
            .list_collections(db)?
            .into_iter()
            .filter(|c| c.vector.is_some() && !vector_meta::is_shadow(&c.name))
            .collect())
    }

    /// Find a collection by its internal id.
    ///
    /// Oplog entries carry the id, not the name, so any consumer of the log
    /// needs this to decide whether an entry is interesting. A catalogue walk
    /// in one read transaction, stopping at the first match: a caller that
    /// knows the name should use [`Self::get_collection`], which is a point
    /// read.
    pub fn collection_by_id(&self, id: kimmy_core::CollectionId) -> Result<Option<CollectionMeta>> {
        let txn = self.db().begin_read()?;
        let collections = txn.open_table(crate::tables::COLLECTIONS)?;
        for entry in collections.iter()? {
            let (_, value) = entry?;
            let meta: CollectionMeta = serde_json::from_slice(value.value())?;
            if meta.id == id {
                return Ok(Some(meta));
            }
        }
        Ok(None)
    }

    /// Every collection this node holds, **shadow collections included**, as
    /// its id and the `created` stamp of the incarnation standing under it.
    ///
    /// Deliberately not [`Self::all_collection_ids`], which hides a shadow
    /// whose parent is present (ADR-138) because the cross-member existence
    /// check must not read the owning member's lifecycle lag as divergence.
    /// The ids it hides are exactly the ones a vector index is keyed by, so
    /// reconciling the index cache against it would forget every live graph
    /// on the node. The two are one walk (`Engine::collections`) one argument
    /// apart: `PairedShadows::Included` here, `Hidden` there.
    ///
    /// The stamp rides along because the id alone cannot vouch for a graph or
    /// a snapshot: ids are derived from names, so a name dropped and created
    /// again is live at the same id, and whatever was built for its
    /// predecessor is keyed and stored under it too.
    pub fn live_collections(&self) -> Result<HashMap<CollectionId, Hlc>> {
        Ok(self
            .collections(crate::engine::PairedShadows::Included)?
            .into_iter()
            .map(|c| (c.id, c.created))
            .collect())
    }

    /// Persist a background consumer's position in the oplog.
    ///
    /// Keyed by name so several consumers can track independently. Stored
    /// rather than recomputed, because rebuilding it would mean re-reading the
    /// whole log on every restart.
    pub fn put_consumer_position(&self, consumer: &str, token: ResumeToken) -> Result<()> {
        let txn = self.begin_write(WriterHolder::Embedding)?;
        if let Err(e) = self.put_consumer_position_in_txn(&txn, consumer, token) {
            txn.abort()?;
            return Err(e);
        }
        txn.commit()?;
        Ok(())
    }

    /// The whole of [`Self::put_consumer_position`] except the transaction's
    /// lifecycle, so a scope can carry a position in the commit it makes
    /// anyway ([`WriteScope::put_consumer_position`]).
    ///
    /// A position is one row of the metadata table: it mints no stamp and
    /// appends no oplog entry, so ADR-148's rule about where a stamp is
    /// minted has nothing here to apply to, and there is nothing to publish
    /// after the caller commits.
    pub(crate) fn put_consumer_position_in_txn(
        &self,
        txn: &WriteTxn<'_>,
        consumer: &str,
        token: ResumeToken,
    ) -> Result<()> {
        let mut meta = txn.open_table(crate::tables::META)?;
        meta.insert(consumer_key(consumer).as_str(), token.encode().as_bytes())?;
        Ok(())
    }

    /// Where a background consumer left off, if it has ever recorded a position.
    pub fn consumer_position(&self, consumer: &str) -> Result<Option<ResumeToken>> {
        let txn = self.db().begin_read()?;
        let meta = txn.open_table(crate::tables::META)?;
        let Some(raw) = meta.get(consumer_key(consumer).as_str())? else {
            return Ok(None);
        };
        let text = std::str::from_utf8(raw.value())
            .map_err(|_| StorageError::Corrupt("consumer position is not utf-8".into()))?;
        Ok(Some(ResumeToken::decode(text)?))
    }

    /// Record which configuration a collection's vectors were last *fully*
    /// embedded under.
    ///
    /// Written by the embedding worker only after a completed backfill scan —
    /// the same position-after-work rule every consumer follows, at the scan
    /// level. A stored fingerprint that differs from the live configuration
    /// is how the worker knows a replayed `ConfigureVectors` entry demands a
    /// full re-embed rather than a staleness-checked no-op: the per-document
    /// HLC cannot see a configuration change, because configurations do not
    /// touch documents.
    pub fn put_vector_fingerprint(&self, collection: CollectionId, fingerprint: u64) -> Result<()> {
        let txn = self.begin_write(WriterHolder::Embedding)?;
        {
            let mut meta = txn.open_table(crate::tables::META)?;
            meta.insert(fingerprint_key(collection).as_str(), &fingerprint.to_be_bytes()[..])?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Forget the fingerprint [`Self::put_vector_fingerprint`] recorded, so the
    /// collection reads as never fully scanned here. The embedding worker calls
    /// it when a configuration change is backfilled by another member: what was
    /// recorded attests a configuration this member no longer knows its vectors
    /// to be under, nor to be behind. Commits only when there was one.
    pub fn clear_vector_fingerprint(&self, collection: CollectionId) -> Result<()> {
        if self.vector_fingerprint(collection)?.is_none() {
            return Ok(());
        }
        let txn = self.begin_write(WriterHolder::Embedding)?;
        {
            let mut meta = txn.open_table(crate::tables::META)?;
            meta.remove(fingerprint_key(collection).as_str())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// The configuration fingerprint the last completed backfill recorded.
    pub fn vector_fingerprint(&self, collection: CollectionId) -> Result<Option<u64>> {
        let txn = self.db().begin_read()?;
        let meta = txn.open_table(crate::tables::META)?;
        let Some(raw) = meta.get(fingerprint_key(collection).as_str())? else {
            return Ok(None);
        };
        let bytes: [u8; 8] = raw
            .value()
            .try_into()
            .map_err(|_| StorageError::Corrupt("vector fingerprint is not 8 bytes".into()))?;
        Ok(Some(u64::from_be_bytes(bytes)))
    }

    /// Record that a collection owes a scan: its embedding provider failed
    /// retryably, so a batch was held in memory, or its batches were skipped,
    /// and the vectors those would have written are missing. `force` says the
    /// scan must read a record with no configuration fingerprint as stale (a
    /// configuration change's backfill) rather than trust its version.
    ///
    /// Written by the embedding worker **before** it records an oplog position
    /// that passes the entries the held or skipped work came from, so a restart
    /// finds the debt without replaying the oplog for it. Cleared by
    /// [`Self::clear_vector_rescan`] when the scan has run.
    pub fn put_vector_rescan(&self, collection: CollectionId, force: bool) -> Result<()> {
        let txn = self.begin_write(WriterHolder::Embedding)?;
        {
            let mut meta = txn.open_table(crate::tables::META)?;
            let value: &[u8] = if force { b"force" } else { b"check" };
            meta.insert(rescan_key(collection).as_str(), value)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Forget the debt [`Self::put_vector_rescan`] recorded. Commits only when
    /// there was one.
    pub fn clear_vector_rescan(&self, collection: CollectionId) -> Result<()> {
        if !self.vector_rescans()?.iter().any(|(id, _)| *id == collection) {
            return Ok(());
        }
        let txn = self.begin_write(WriterHolder::Embedding)?;
        {
            let mut meta = txn.open_table(crate::tables::META)?;
            meta.remove(rescan_key(collection).as_str())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Every collection that owes a scan, with whether it must be forced.
    pub fn vector_rescans(&self) -> Result<Vec<(CollectionId, bool)>> {
        let txn = self.db().begin_read()?;
        let meta = txn.open_table(crate::tables::META)?;
        let mut owed = Vec::new();
        // `;` is the character after `:`, so this range is exactly the prefix.
        for row in meta.range(RESCAN_PREFIX..RESCAN_END)? {
            let (key, value) = row?;
            let Some(id) = key.value().strip_prefix(RESCAN_PREFIX).and_then(|id| id.parse().ok())
            else {
                return Err(StorageError::Corrupt("vector rescan key is not an id".into()));
            };
            let force = match value.value() {
                b"force" => true,
                b"check" => false,
                _ => {
                    return Err(StorageError::Corrupt(
                        "vector rescan is not check or force".into(),
                    ));
                }
            };
            owed.push((CollectionId(id), force));
        }
        Ok(owed)
    }

    // -----------------------------------------------------------------------
    // Vector records
    // -----------------------------------------------------------------------

    /// Replace every chunk belonging to one source document.
    ///
    /// Writes the new chunks and removes any left over from a longer previous
    /// version. Without that cleanup, shortening a document would leave its
    /// tail chunks searchable forever — matches pointing at text the document
    /// no longer contains.
    ///
    /// The whole replacement is one commit, in one [`Engine::write_batch`]
    /// scope (ADR-149): every chunk write and every tail delete goes into the
    /// same transaction, so a document of N chunks costs one fsync rather
    /// than N, and a failure part way through leaves the previous chunk set
    /// exactly as it was — never a document with some new chunks and some
    /// old. This is the single-document form of [`WriteScope::put_vectors`],
    /// which the embedding worker composes for a whole provider batch; the
    /// records are encoded here, before the writer is taken, and the scope
    /// does nothing but write them.
    ///
    /// The chunks go under the ids of [`ChunkSource::from`]`(source)`
    /// (ADR-239).
    ///
    /// The generation moves only after the commit, never inside the scope: a
    /// bump before the commit would let an index build read the new
    /// generation against the old chunks and be served as fresh for it. The
    /// scope records the shadow and `write_batch` bumps it once the commit
    /// has landed. And it moves only when something changed: an empty
    /// `records` over a document with no chunks writes nothing, commits
    /// nothing and leaves the generation where it was, the same rule
    /// [`Engine::delete_vectors`] follows for a document that has nothing to
    /// delete.
    pub fn put_vectors(
        &self,
        shadow: &CollectionMeta,
        source: &DocId,
        records: &[VectorRecord],
    ) -> Result<()> {
        let write = VectorWrite::encode(source, &ChunkSource::from(source), records)?;
        self.write_batch(WriterHolder::Embedding, |scope| scope.put_vectors(shadow, write))
    }

    /// Every vector belonging to one source document, in chunk order, once per
    /// chunk number.
    ///
    /// A document's chunks are those in its run
    /// ([`VectorRecord::run_prefix`]) whose record names it; a chunk there
    /// whose record names another document is not returned.
    pub fn get_vectors(
        &self,
        shadow: &CollectionMeta,
        source: &DocId,
    ) -> Result<Vec<VectorRecord>> {
        Ok(Held::read(self, shadow, source)?.served())
    }

    /// Visit every vector belonging to one source document, in chunk order.
    ///
    /// The bounded form of [`Engine::for_each_vector`]: it reads one
    /// document's chunks, as [`Self::get_vectors`] does, rather than the
    /// collection, so a search that already knows which documents it wants
    /// can fetch their chunks by key instead of scanning for them. A record
    /// that does not decode is skipped with a warning, for the reason
    /// `for_each_vector` gives.
    pub fn for_each_vector_of<F>(
        &self,
        shadow: &CollectionMeta,
        source: &DocId,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(VectorRecord) -> Result<bool>,
    {
        for record in self.get_vectors(shadow, source)? {
            if !f(record)? {
                break;
            }
        }
        Ok(())
    }

    /// Remove every vector belonging to one source document.
    ///
    /// One commit however many chunks the document holds, for the reason
    /// [`Engine::put_vectors`] gives: the chunks are read under the writer
    /// and every delete goes into the same scope, so a failure leaves the set
    /// whole. The generation moves after the commit, and only when a chunk
    /// was removed; a document with no chunks costs no commit.
    pub fn delete_vectors(&self, shadow: &CollectionMeta, source: &DocId) -> Result<usize> {
        self.write_batch(WriterHolder::Embedding, |scope| scope.delete_vectors(shadow, source))
    }

    /// [`WriteScope::delete_vectors_made_under_another`], in a scope of its
    /// own. A document whose chunks are all under `config` costs no commit.
    pub fn delete_vectors_made_under_another(
        &self,
        shadow: &CollectionMeta,
        source: &DocId,
        config: u64,
        unstamped: Unstamped,
    ) -> Result<usize> {
        self.write_batch(WriterHolder::Embedding, |scope| {
            scope.delete_vectors_made_under_another(shadow, source, config, unstamped)
        })
    }

    /// How many times one shadow's chunks have been moved to other ids here: by
    /// a put that removed an id it did not rewrite. It can keep the count of
    /// chunks while changing their ids, so a graph
    /// saved before one is behind though the count agrees; the index cache
    /// compares this, not the count alone, before it adopts a graph a
    /// previous process saved (ADR-239). A chunk a peer's write removes from
    /// the shadow counts too, when it is applied here
    /// (`Engine::apply_remote_in_txn`): the owner makes the moves, and every
    /// other member receives them as an insert and a removal.
    pub fn vector_renames(&self, shadow: CollectionId) -> Result<u64> {
        let txn = self.db().begin_read()?;
        let meta = txn.open_table(crate::tables::META)?;
        read_renames(meta.get(renames_key(shadow).as_str())?.as_ref().map(|v| v.value()))
    }

    /// Visit every stored vector, with the id it is stored under. Used by
    /// search and by index rebuilds; a graph is keyed by the stored id, never
    /// by one recomputed from the record (ADR-239).
    ///
    /// A document that does not decode as a vector record is **skipped, not
    /// fatal**. A shadow collection is an ordinary collection, so a client with
    /// write access can put an arbitrary document in one; failing the scan
    /// would let a single malformed insert turn every subsequent search on that
    /// collection into a 500. Skipping costs one unusable record and keeps
    /// search available.
    ///
    /// It is logged at `warn` rather than passed over silently, because the
    /// other way to arrive here is genuine corruption.
    pub fn for_each_vector<F>(
        &self,
        shadow: &CollectionMeta,
        scope: WalkScope,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(&DocId, VectorRecord) -> Result<bool>,
    {
        self.for_each_doc(shadow, scope, |id, doc| match decode_vector(doc) {
            Ok(record) => f(&id, record),
            Err(e) => {
                tracing::warn!(
                    collection = %shadow.name,
                    document = %id,
                    error = %e,
                    "skipping a document in a vector collection that is not a vector record"
                );
                Ok(true)
            }
        })
    }

    /// Whether a document's vectors need making again (ADR-239's rules, each
    /// over the chunks [`Self::get_vectors`] reads):
    ///
    /// - (a) none is held at all, since a document that has never been
    ///   embedded also needs work;
    /// - (b) one is older than the document itself, or was made under a
    ///   configuration other than the one fingerprinted `config` (ADR-203; a
    ///   record with no fingerprint is judged as `unstamped` says,
    ///   [`VectorRecord::is_stale`]);
    /// - (c) the document's run holds a chunk whose record names another
    ///   document, or none: debris at its ids, which only its own put removes,
    ///   and which a judgement that passed it over would serve beside it.
    ///
    /// The fingerprint rides on the records this reads anyway, so the
    /// configuration costs no read of its own.
    pub fn vectors_are_stale(
        &self,
        shadow: &CollectionMeta,
        source: &DocId,
        current: Hlc,
        config: u64,
        unstamped: Unstamped,
    ) -> Result<bool> {
        Ok(Held::read(self, shadow, source)?.is_stale(current, config, unstamped))
    }
}

impl WriteScope<'_> {
    /// [`crate::Engine::put_vectors`], into this scope's transaction: the
    /// replace-all write of one source document's chunks, with no commit of
    /// its own.
    ///
    /// The one implementation of the chunk loop. `Engine::put_vectors` is
    /// this in a scope of its own; the embedding worker calls it once per
    /// document of a provider batch inside one scope, with its position
    /// beside them (ADR-125, as amended). The write was encoded before the
    /// scope was opened ([`VectorWrite::encode`]), so nothing here but the
    /// writes themselves runs under the writer. What the document holds is
    /// read inside the scope, in its transaction: under the writer, so no
    /// other write can add or remove a chunk between the read and the
    /// replace, and seeing what the scope has written already.
    ///
    /// What goes is chosen by stored id, never recomputed from a chunk number
    /// (ADR-239): every id of the document's run this write does not rewrite,
    /// whatever its record names (another source's chunk there is debris the
    /// staleness check would otherwise re-embed the document for, for ever).
    ///
    /// The generation bump belongs to the commit, not to the write: the
    /// shadow is recorded here, when a chunk was written or removed, and
    /// `write_batch` bumps it once after the commit has landed — never
    /// inside the scope, and never for a scope that aborted. An empty chunk
    /// set over a document that has none writes nothing and records
    /// nothing. A write that removed a chunk it did not rewrite also counts a
    /// move of the shadow's ids ([`crate::Engine::vector_renames`]).
    pub fn put_vectors(&mut self, shadow: &CollectionMeta, write: VectorWrite) -> Result<()> {
        let VectorWrite { document, docs } = write;
        let mut run = Vec::new();
        visit_document(&*self, shadow, &document, |id, _, _, _| run.push(id))?;

        let written = !docs.is_empty();
        let mut rewritten = std::collections::HashSet::with_capacity(docs.len());
        for (id, doc) in docs {
            self.replace(shadow, &id, doc, true)?;
            rewritten.insert(id);
        }

        // Drop what this write did not rewrite: the tail of a previously
        // longer document, and debris.
        let mut removed = 0;
        for id in run {
            if !rewritten.contains(&id) && self.delete(shadow, &id)? {
                removed += 1;
            }
        }
        if written || removed > 0 {
            self.touch_vector_generation(shadow.id);
        }
        if removed > 0 {
            self.note_vector_renames(shadow.id);
        }
        Ok(())
    }

    /// Remove every chunk of one source document when any was made under a
    /// configuration other than the one fingerprinted `config`
    /// ([`VectorRecord::made_under_another`]; a chunk with no fingerprint is
    /// judged as `unstamped` says). The embedding worker calls it when the
    /// current configuration's provider permanently refuses the document:
    /// what it holds is another model's vectors, in another vector space, and
    /// a search would rank them against queries embedded by this one
    /// (ADR-203). Chunks already under `config` are kept. The chunks judged
    /// and removed are the document's own, as [`Self::delete_vectors`] reads
    /// them (ADR-239). Read under the writer, like `delete_vectors`, so no
    /// write can land between the judgement and the delete. Returns how many
    /// were removed.
    pub fn delete_vectors_made_under_another(
        &mut self,
        shadow: &CollectionMeta,
        source: &DocId,
        config: u64,
        unstamped: Unstamped,
    ) -> Result<usize> {
        let held = Held::read(&*self, shadow, source)?;
        if !held.named().any(|r| r.made_under_another(config, unstamped)) {
            return Ok(0);
        }
        self.delete_vectors(shadow, source)
    }

    /// [`crate::Engine::delete_vectors`], into this scope's transaction.
    ///
    /// Every chunk of the document's run whose record names it, read under the
    /// writer, deleted in this scope. A chunk there whose record names another
    /// document is that document's, and stays for its own put, or its own
    /// delete, to remove. The shadow is recorded for the post-commit bump only
    /// when a chunk was removed. Returns how many were.
    pub fn delete_vectors(&mut self, shadow: &CollectionMeta, source: &DocId) -> Result<usize> {
        let mut run = Vec::new();
        visit_document(&*self, shadow, source, |id, _, _, doc| {
            if names(doc, source) {
                run.push(id);
            }
        })?;
        let mut removed = 0;
        for id in run {
            if self.delete(shadow, &id)? {
                removed += 1;
            }
        }
        if removed > 0 {
            self.touch_vector_generation(shadow.id);
        }
        Ok(removed)
    }
}

/// Where one document's chunks are read from: the engine's own read, or a
/// scope's transaction, under the writer and seeing what the scope has
/// already written, which a decision the scope then writes by must see.
trait ShadowRead {
    /// Visit the shadow's documents after each encoded key of `afters` in turn,
    /// **in one read**, so what one place says is never judged beside what
    /// another said at another moment: `f` is told which, and answers `false`
    /// to end that one.
    fn docs_after_each(
        &self,
        shadow: &CollectionMeta,
        afters: &[Vec<u8>],
        f: &mut dyn FnMut(usize, DocId, bson::Document) -> Result<bool>,
    ) -> Result<()>;
}

impl ShadowRead for crate::Engine {
    fn docs_after_each(
        &self,
        shadow: &CollectionMeta,
        afters: &[Vec<u8>],
        f: &mut dyn FnMut(usize, DocId, bson::Document) -> Result<bool>,
    ) -> Result<()> {
        // One document's chunks, which is bounded: `Request`, so only the drain's
        // deadline ends it.
        self.for_each_doc_after_each(shadow, afters, WalkScope::Request, f)
    }
}

impl ShadowRead for WriteScope<'_> {
    fn docs_after_each(
        &self,
        shadow: &CollectionMeta,
        afters: &[Vec<u8>],
        f: &mut dyn FnMut(usize, DocId, bson::Document) -> Result<bool>,
    ) -> Result<()> {
        for (i, after) in afters.iter().enumerate() {
            self.for_each_doc_after(shadow, Some(after), |id, doc| f(i, id, doc))?;
        }
        Ok(())
    }
}

/// Visit one document's chunks, its run, in one read, whatever their records
/// hold: every key under [`VectorRecord::run_prefix`] whose parse names a
/// source of the document ([`ChunkSource::is_of`]), with that source and its
/// chunk number.
///
/// Chunk ids are String keys, and a string key encodes byte-for-byte in `_id`
/// order, so the run is a contiguous range of the shadow collection: its scan
/// starts just past the encoded prefix — which sorts before every key that
/// extends it — and stops at the first key that does not carry it. Reading a
/// document's chunks therefore costs its chunk count, not the collection's.
///
/// The prefix alone is not proof of ownership: a String may itself contain
/// `#`, so `a#1#0` (document `a#1`, chunk 0) sits inside `a#`'s run. Each key
/// is parsed and its source compared, and a key inside the run that belongs to
/// another document is passed over rather than ending the scan.
fn visit_document(
    view: &impl ShadowRead,
    shadow: &CollectionMeta,
    document: &DocId,
    mut run: impl FnMut(DocId, ChunkSource, u32, bson::Document),
) -> Result<()> {
    let prefix = VectorRecord::run_prefix(document);
    let after = kimmy_core::keyenc::encode(&bson::Bson::String(prefix.clone()))?;
    view.docs_after_each(shadow, &[after], &mut |_, id, doc| {
        // Strings sort together, so the first non-string key — or the first
        // string outside the prefix — is the end of the run.
        let DocId::String(raw) = &id else {
            return Ok(false);
        };
        if !raw.starts_with(prefix.as_str()) {
            return Ok(false);
        }
        if let Some((source, chunk)) = VectorRecord::parse_id(&id)
            && source.is_of(document)
        {
            run(id, source, chunk, doc);
        }
        Ok(true)
    })
}

/// Whether a stored chunk's record names `document` as its source.
fn names(doc: bson::Document, document: &DocId) -> bool {
    decode_vector(doc).is_ok_and(|record| record.source == *document)
}

/// What a shadow holds of one document, read for the per-document rules of
/// ADR-239.
struct Held {
    document: DocId,
    /// Every key of the document's run, as its record, `None` where the
    /// record does not decode.
    run: Vec<Option<VectorRecord>>,
}

impl Held {
    fn read(view: &impl ShadowRead, shadow: &CollectionMeta, document: &DocId) -> Result<Self> {
        let mut run = Vec::new();
        visit_document(view, shadow, document, |id, _, _, doc| {
            let record = match decode_vector(doc) {
                Ok(record) => Some(record),
                Err(e) => {
                    tracing::warn!(
                        collection = %shadow.name,
                        document = %id,
                        error = %e,
                        "skipping a document in a vector collection that is not a vector record"
                    );
                    None
                }
            };
            run.push(record);
        })?;
        Ok(Self { document: document.clone(), run })
    }

    /// The run's chunks whose record names the document.
    fn named(&self) -> impl Iterator<Item = &VectorRecord> {
        self.run.iter().filter_map(|r| r.as_ref()).filter(|r| r.source == self.document)
    }

    /// What is served for the document, once per chunk number and in chunk
    /// order: the run's chunks that name it.
    fn served(self) -> Vec<VectorRecord> {
        let Self { document, run } = self;
        let mut by_chunk: std::collections::BTreeMap<u32, VectorRecord> = Default::default();
        for record in run.into_iter().flatten().filter(|r| r.source == document) {
            by_chunk.insert(record.chunk, record);
        }
        by_chunk.into_values().collect()
    }

    /// Rules (a) to (c) of [`crate::Engine::vectors_are_stale`].
    fn is_stale(&self, current: Hlc, config: u64, unstamped: Unstamped) -> bool {
        let none = self.named().next().is_none();
        let behind = self.named().any(|r| r.is_stale(current, config, unstamped));
        let debris = self.named().count() < self.run.len();
        none || behind || debris
    }
}

/// META key counting the moves of one shadow's chunks to other ids.
fn renames_key(shadow: CollectionId) -> String {
    format!("vector_renames:{}", shadow.0)
}

/// Count one move of each shadow's chunk ids, inside the transaction that
/// commits the scope that moved them ([`WriteScope::note_vector_renames`]).
pub(crate) fn bump_vector_renames(txn: &WriteTxn<'_>, shadows: &[CollectionId]) -> Result<()> {
    if shadows.is_empty() {
        return Ok(());
    }
    let mut meta = txn.open_table(crate::tables::META)?;
    for shadow in shadows {
        let key = renames_key(*shadow);
        let now = read_renames(meta.get(key.as_str())?.as_ref().map(|v| v.value()))?;
        meta.insert(key.as_str(), &(now + 1).to_be_bytes()[..])?;
    }
    Ok(())
}

fn read_renames(raw: Option<&[u8]>) -> Result<u64> {
    match raw {
        None => Ok(0),
        Some(raw) => Ok(u64::from_be_bytes(
            raw.try_into()
                .map_err(|_| StorageError::Corrupt("vector renames is not 8 bytes".into()))?,
        )),
    }
}

/// META key holding one consumer's oplog position.
fn consumer_key(consumer: &str) -> String {
    format!("consumer_position:{consumer}")
}

fn fingerprint_key(collection: CollectionId) -> String {
    format!("vector_config:{}", collection.0)
}

const RESCAN_PREFIX: &str = "vector_rescan:";
const RESCAN_END: &str = "vector_rescan;";

fn rescan_key(collection: CollectionId) -> String {
    format!("{RESCAN_PREFIX}{}", collection.0)
}

fn decode_vector(doc: bson::Document) -> Result<VectorRecord> {
    bson::deserialize_from_document(doc)
        .map_err(|e| StorageError::Corrupt(format!("decoding vector record: {e}")))
}

/// Whether the vector chunk record `raw` belongs to a document of `parent` that
/// cannot be live there after a drop stamped `dropped`.
///
/// - The document is **there and deleted**: yes.
/// - The document is **there and live**: no.
/// - The document is **not here**: undecided, and the answer is the version of
///   the document the chunk was made from (`source_hlc`). One at or before the
///   drop is history in this life, so it can never arrive live and the chunk
///   goes; one after it is a document that may be on its way from another
///   origin (the chunk from a member that embedded it, the document from one
///   whose link is slower), so the chunk stays and the document, if it comes,
///   finds it.
///
/// A record that cannot be read as a chunk is not judged orphaned.
pub(crate) fn chunk_has_no_live_parent(
    docs: &impl ReadableTable<(u64, &'static [u8]), &'static [u8]>,
    parent: CollectionId,
    dropped: Hlc,
    raw: &[u8],
) -> bool {
    let Ok(Some(document)) = crate::codec::decode_doc_record(raw).and_then(|r| Ok(r.document()?))
    else {
        return false;
    };
    let Ok(record) = decode_vector(document) else { return false };
    let Ok(key) = crate::docs::doc_key(&record.source) else { return false };
    match docs.get((parent.0, key.as_slice())) {
        Ok(Some(row)) => crate::codec::doc_record_header(row.value()).is_ok_and(|(_, live)| !live),
        Ok(None) => record.source_hlc <= dropped,
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use kimmy_core::{OpKind, ProviderConfig, vector_meta};

    use super::*;
    use crate::Engine;

    fn engine() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        engine.create_collection("app", "docs").unwrap();
        (engine, dir)
    }

    #[test]
    fn a_recorded_rescan_reads_back_and_is_cleared() {
        let (engine, _dir) = engine();
        let (a, b) = (CollectionId(7), CollectionId(8));
        assert!(engine.vector_rescans().unwrap().is_empty(), "nothing owed yet");
        engine.put_vector_rescan(a, false).unwrap();
        engine.put_vector_rescan(b, true).unwrap();
        assert_eq!(engine.vector_rescans().unwrap(), vec![(a, false), (b, true)]);
        engine.put_vector_rescan(a, true).unwrap();
        engine.clear_vector_rescan(b).unwrap();
        engine.clear_vector_rescan(b).unwrap();
        assert_eq!(engine.vector_rescans().unwrap(), vec![(a, true)]);
        assert_eq!(engine.vector_fingerprint(a).unwrap(), None, "no other key reads it");
    }

    #[test]
    fn a_recorded_fingerprint_reads_back() {
        // The idempotency mechanism for reindexing: a completed backfill
        // records the configuration it ran under, and a replayed
        // `ConfigureVectors` entry compares against it. If the read always
        // reported "nothing recorded", every replay would re-embed the whole
        // collection — silently, and expensively.
        let (engine, _dir) = engine();
        let coll = engine.get_collection("app", "docs").unwrap().id;

        assert_eq!(engine.vector_fingerprint(coll).unwrap(), None, "nothing recorded yet");

        engine.put_vector_fingerprint(coll, 0xDEAD_BEEF).unwrap();
        assert_eq!(engine.vector_fingerprint(coll).unwrap(), Some(0xDEAD_BEEF));

        // Replaced rather than accumulated, so a reconfigure leaves one answer.
        engine.put_vector_fingerprint(coll, 7).unwrap();
        assert_eq!(engine.vector_fingerprint(coll).unwrap(), Some(7));
    }

    #[test]
    fn fingerprints_do_not_collide_between_collections() {
        // They share one metadata table, so the key has to carry the
        // collection. A constant key would make configuring vectors on one
        // collection mark every other collection as already backfilled.
        let (engine, _dir) = engine();
        engine.create_collection("app", "other").unwrap();
        let a = engine.get_collection("app", "docs").unwrap().id;
        let b = engine.get_collection("app", "other").unwrap().id;

        engine.put_vector_fingerprint(a, 111).unwrap();

        assert_eq!(engine.vector_fingerprint(a).unwrap(), Some(111));
        assert_eq!(
            engine.vector_fingerprint(b).unwrap(),
            None,
            "one collection's fingerprint must not answer for another"
        );

        engine.put_vector_fingerprint(b, 222).unwrap();
        assert_eq!(engine.vector_fingerprint(a).unwrap(), Some(111), "and must not overwrite it");
        assert_eq!(engine.vector_fingerprint(b).unwrap(), Some(222));
    }

    fn config(dim: usize) -> VectorConfig {
        VectorConfig {
            fields: vec!["body".into()],
            provider: ProviderConfig::Byo {},
            dim,
            metric: Default::default(),
            document_prefix: None,
            query_prefix: None,
            chunk: Default::default(),
        }
    }

    #[test]
    fn a_filter_the_order_cannot_compare_does_not_crash_a_vector_change() {
        // The other pair of callers of the definition write-back. A NaN in any
        // partial filter on this collection made `configure_vectors` and
        // `disable_vectors` abort the node, exactly as a drop or a create did:
        // the check compared metadata with `==`, `NaN != NaN`, and the retry was
        // a self-call with nothing counting it.
        let (engine, _dir) = engine();
        engine
            .create_index_with(
                "app",
                "docs",
                vec![kimmy_core::IndexField::ascending("z")],
                false,
                Default::default(),
                Some("z_nan".into()),
                None,
                Some(bson::doc! { "k": f64::NAN }),
            )
            .unwrap();

        engine.configure_vectors("app", "docs", config(8)).unwrap();
        assert!(engine.vector_collection("app", "docs").unwrap().is_some());
        assert!(engine.disable_vectors("app", "docs", true).unwrap());
    }

    /// Run `change(i)` for `0..n` on `n` threads released together, and require
    /// every one to succeed.
    fn all_at_once<T: Send + 'static>(
        engine: &std::sync::Arc<Engine>,
        n: usize,
        change: impl Fn(&Engine, usize) -> Result<T> + Send + Sync + 'static,
    ) {
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(n));
        let change = std::sync::Arc::new(change);
        let handles: Vec<_> = (0..n)
            .map(|i| {
                let (engine, barrier, change) = (
                    std::sync::Arc::clone(engine),
                    std::sync::Arc::clone(&barrier),
                    std::sync::Arc::clone(&change),
                );
                std::thread::spawn(move || {
                    barrier.wait();
                    change(&engine, i).map(|_| ())
                })
            })
            .collect();
        let errors: Vec<String> = handles
            .into_iter()
            .filter_map(|h| h.join().expect("no thread panics").err().map(|e| e.to_string()))
            .collect();
        assert!(
            errors.is_empty(),
            "{} of {n} concurrent changes on one collection failed, which is the bound firing on \
             contention rather than on a bug: {:?}",
            errors.len(),
            &errors[..errors.len().min(3)]
        );
    }

    #[test]
    fn concurrent_vector_configurations_on_one_collection_all_succeed() {
        // The configuration's loop under contention, which nothing had put it
        // under: cutting it to two attempts left every test green. Each thread
        // configures its own field list, so every one changes the definition
        // and is a race the others can lose.
        let (engine, _dir) = engine();
        let engine = std::sync::Arc::new(engine);
        all_at_once(&engine, 64, |engine, i| {
            engine.configure_vectors(
                "app",
                "docs",
                VectorConfig { fields: vec![format!("body{i}")], ..config(8) },
            )
        });
    }

    #[test]
    fn a_vector_removal_racing_configurations_succeeds() {
        // The removal's loop, likewise. Removals cannot contend with each other
        // — once one lands the rest find nothing to remove and return — so one
        // removal races 63 configurations of its collection, each of which it
        // can lose to. Several rounds, because how often it loses is the
        // scheduler's choice.
        let (engine, _dir) = engine();
        let engine = std::sync::Arc::new(engine);
        for round in 0..8 {
            all_at_once(&engine, 64, move |engine, i| {
                if i == 0 {
                    engine.disable_vectors("app", "docs", false).map(|_| ())
                } else {
                    engine
                        .configure_vectors(
                            "app",
                            "docs",
                            VectorConfig { fields: vec![format!("body{round}_{i}")], ..config(8) },
                        )
                        .map(|_| ())
                }
            });
        }
    }

    #[test]
    fn a_vector_configuration_whose_check_never_passes_is_an_error_not_an_abort() {
        // The bound, forced, at this site: see the note above
        // `a_definition_check_that_never_passes_is_an_error_not_an_abort`.
        let (engine, _dir) = engine();
        crate::engine::definition_hooks::assert_exhausted("app.docs", || {
            engine.configure_vectors("app", "docs", config(8))
        });
        assert!(engine.get_collection("app", "docs").unwrap().vector.is_none(), "nothing set");
    }

    #[test]
    fn a_vector_removal_whose_check_never_passes_is_an_error_not_an_abort() {
        let (engine, _dir) = engine();
        engine.configure_vectors("app", "docs", config(8)).unwrap();
        crate::engine::definition_hooks::assert_exhausted("app.docs", || {
            engine.disable_vectors("app", "docs", true)
        });
        assert!(engine.get_collection("app", "docs").unwrap().vector.is_some(), "still set");
    }

    #[test]
    fn enabling_vectors_creates_the_shadow_collection() {
        let (engine, _dir) = engine();
        engine.configure_vectors("app", "docs", config(8)).unwrap();

        let shadow = engine.get_collection("app", "docs.__vectors").unwrap();
        assert_eq!(shadow.name, vector_meta::shadow_name("docs"));
        assert!(engine.vector_collection("app", "docs").unwrap().is_some());
    }

    #[test]
    fn an_invalid_config_is_rejected_before_anything_is_created() {
        let (engine, _dir) = engine();
        let mut bad = config(8);
        bad.fields.clear();

        assert!(engine.configure_vectors("app", "docs", bad).is_err());
        // Nothing should have been created by the failed attempt.
        assert!(engine.get_collection("app", "docs.__vectors").is_err());
        assert!(engine.get_collection("app", "docs").unwrap().vector.is_none());
    }

    #[test]
    fn turning_vectors_off_and_dropping_them_is_one_commit_and_a_resend_says_it_did_nothing() {
        // Two commits used to follow each other, and a failure between them
        // answered an error over a configuration change that had landed
        // (ADR-192). The shadow now goes in the configuration's transaction.
        let (engine, _dir) = engine();
        engine.configure_vectors("app", "docs", config(8)).unwrap();
        let mut rx = engine.subscribe();

        let before = engine.commits();
        let off = engine.disable_vectors_reporting("app", "docs", true).unwrap();
        assert_eq!(off, VectorsOff { disabled: true, dropped_vectors: true });
        assert_eq!(engine.commits() - before, 1, "the configuration and the shadow together");
        assert!(engine.get_collection("app", "docs.__vectors").is_err(), "the shadow is gone");

        // Both changes replicate: the configuration's entry and the shadow's
        // own drop, so a peer drops its copy of the vectors.
        let mut kinds = Vec::new();
        while let Ok(entry) = rx.try_recv() {
            kinds.push(entry.kind);
        }
        assert_eq!(kinds, vec![OpKind::ConfigureVectors, OpKind::DropCollection]);

        // A resend says it did nothing -- the configuration was already off -- and it
        // is a change all the same (ADR-226): stamped and logged, one commit, so the
        // stamp is in this member's servable vector and a page carrying it passes the
        // receiver's coverage gate. It used to commit nothing.
        let resend = engine.disable_vectors_reporting("app", "docs", true).unwrap();
        assert_eq!(resend, VectorsOff { disabled: false, dropped_vectors: false });
        assert_eq!(engine.commits() - before, 2, "a resend is stamped and logged: one commit");
        let mut kinds = Vec::new();
        while let Ok(entry) = rx.try_recv() {
            kinds.push(entry.kind);
        }
        assert_eq!(kinds, vec![OpKind::ConfigureVectors], "and its entry replicates");
    }

    #[test]
    fn a_resend_drops_a_shadow_an_earlier_partial_disable_left() {
        // What a failure between the two commits left behind before ADR-192:
        // the configuration off and the vectors still stored. A resend with
        // `drop_vectors` finishes the job and says so.
        let (engine, _dir) = engine();
        engine.configure_vectors("app", "docs", config(8)).unwrap();
        assert!(engine.disable_vectors("app", "docs", false).unwrap());
        assert!(engine.get_collection("app", "docs.__vectors").is_ok());

        let off = engine.disable_vectors_reporting("app", "docs", true).unwrap();
        assert_eq!(off, VectorsOff { disabled: false, dropped_vectors: true });
        assert!(engine.get_collection("app", "docs.__vectors").is_err());
    }

    #[test]
    fn a_resend_does_not_drop_the_shadow_of_vectors_turned_back_on_meanwhile() {
        // The resend used to find the configuration off, then drop the shadow
        // in a transaction of its own. A configuration turned back on between
        // the two adopts the kept shadow, and lost it: vectors on, shadow
        // gone. The drop is now decided under the writer.
        let (engine, _dir) = engine();
        engine.configure_vectors("app", "docs", config(8)).unwrap();
        assert!(engine.disable_vectors("app", "docs", false).unwrap());
        let engine = std::sync::Arc::new(engine);
        let competitor = std::sync::Arc::clone(&engine);
        crate::sync::race_hooks::at(crate::sync::race_hooks::Race::LeftoverShadow, move || {
            competitor.configure_vectors("app", "docs", config(8)).unwrap();
        });
        let off = engine.disable_vectors_reporting("app", "docs", true).unwrap();
        assert_eq!(off, VectorsOff { disabled: false, dropped_vectors: false });
        assert!(engine.get_collection("app", "docs").unwrap().vector.is_some());
        assert!(
            engine.get_collection("app", "docs.__vectors").is_ok(),
            "the shadow of vectors that are on was dropped"
        );
    }

    #[test]
    fn the_dimension_cannot_change_in_place() {
        // Mixing widths in one index is meaningless, so this must be explicit.
        let (engine, _dir) = engine();
        engine.configure_vectors("app", "docs", config(8)).unwrap();

        let err = engine.configure_vectors("app", "docs", config(16)).unwrap_err();
        assert!(err.to_string().contains("dimension"), "unhelpful error: {err}");

        // Same width is a fine reconfiguration.
        assert!(engine.configure_vectors("app", "docs", config(8)).is_ok());
    }

    #[test]
    fn disabling_keeps_the_vectors_by_default() {
        // Re-enabling should not force a full re-embed, which for a remote
        // provider costs real money.
        let (engine, _dir) = engine();
        engine.configure_vectors("app", "docs", config(8)).unwrap();

        assert!(engine.disable_vectors("app", "docs", false).unwrap());
        assert!(engine.get_collection("app", "docs").unwrap().vector.is_none());
        assert!(engine.get_collection("app", "docs.__vectors").is_ok(), "vectors were discarded");

        // Disabling twice reports that there was nothing to do.
        assert!(!engine.disable_vectors("app", "docs", false).unwrap());
    }

    #[test]
    fn disabling_can_discard_the_vectors() {
        let (engine, _dir) = engine();
        engine.configure_vectors("app", "docs", config(8)).unwrap();
        engine.disable_vectors("app", "docs", true).unwrap();
        assert!(engine.get_collection("app", "docs.__vectors").is_err());
    }

    #[test]
    fn vectors_cannot_be_configured_on_a_shadow_collection() {
        let (engine, _dir) = engine();
        engine.configure_vectors("app", "docs", config(8)).unwrap();
        assert!(engine.configure_vectors("app", "docs.__vectors", config(8)).is_err());
    }

    #[test]
    fn shadow_collections_are_not_listed_as_embeddable() {
        // Otherwise the worker would try to embed its own output.
        let (engine, _dir) = engine();
        engine.create_collection("app", "other").unwrap();
        engine.configure_vectors("app", "docs", config(8)).unwrap();

        let enabled = engine.vector_enabled_collections("app").unwrap();
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].name, "docs");
    }

    #[test]
    fn a_user_cannot_create_a_collection_that_shadows_one() {
        let (engine, _dir) = engine();
        // The reserved `__` prefix is what protects the namespace.
        assert!(engine.create_collection("app", "__vectors").is_err());
    }

    // -----------------------------------------------------------------------
    // Vector records
    // -----------------------------------------------------------------------

    fn record(chunk: u32, hlc_ms: u64, text: &str) -> VectorRecord {
        VectorRecord {
            source: DocId::Int64(1),
            chunk,
            source_hlc: Hlc::new(hlc_ms, 0),
            vector: vec![chunk as f32, 1.0],
            text: text.into(),
            config: None,
        }
    }

    /// An engine with vectors enabled, returning the shadow collection.
    fn with_vectors() -> (Engine, CollectionMeta, tempfile::TempDir) {
        let (engine, dir) = engine();
        engine.configure_vectors("app", "docs", config(2)).unwrap();
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        (engine, shadow, dir)
    }

    /// A scope holding only a position commits (ADR-149's "nothing written"
    /// rule is measured by writes, not by entries): the embedding worker's
    /// held position with no batch to ride in goes through the same scope
    /// as one with a batch, and must still land.
    #[test]
    fn a_scope_holding_only_a_position_commits_once() {
        let (engine, _dir) = engine();
        let mut rx = engine.subscribe();
        let token = ResumeToken::new(Hlc::new(7, 1), engine.node_id());

        let commits = engine.commits();
        engine
            .write_batch(WriterHolder::Bulk, |scope| {
                scope.put_consumer_position("worker", token.clone())
            })
            .unwrap();
        assert_eq!(engine.commits() - commits, 1, "a position is a write, and a write commits");
        assert_eq!(engine.consumer_position("worker").unwrap(), Some(token), "and it is readable");
        assert!(rx.try_recv().is_err(), "a position is not an entry: nothing to publish");
    }

    /// The scope's vector write bumps the generation after the commit, as
    /// `put_vectors` does, and a scope that aborts bumps nothing: a build
    /// must never read a generation the data has not caught up with.
    #[test]
    fn a_scoped_vector_write_bumps_the_generation_only_once_it_has_committed() {
        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);
        let three: Vec<VectorRecord> = (0..3).map(|n| record(n, 10, "before")).collect();
        engine.put_vectors(&shadow, &source, &three).unwrap();
        let generation = engine.vector_generation(shadow.id);

        let commits = engine.commits();
        let one =
            VectorWrite::encode(&source, &ChunkSource::from(&source), &[record(0, 20, "after")])
                .unwrap();
        engine
            .write_batch(WriterHolder::Bulk, |scope| {
                scope.put_vectors(&shadow, one)?;
                assert_eq!(
                    engine.vector_generation(shadow.id),
                    generation,
                    "not bumped inside the scope: the commit has not happened yet"
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(engine.commits() - commits, 1);
        assert_eq!(engine.vector_generation(shadow.id), generation + 1, "bumped once, after");
        let chunks: Vec<u32> =
            engine.get_vectors(&shadow, &source).unwrap().iter().map(|r| r.chunk).collect();
        assert_eq!(chunks, vec![0], "the tail was dropped in the same commit");

        let two = VectorWrite::encode(
            &source,
            &ChunkSource::from(&source),
            &[record(0, 30, "x"), record(1, 30, "y")],
        )
        .unwrap();
        let err = engine
            .write_batch(WriterHolder::Bulk, |scope| {
                scope.put_vectors(&shadow, two)?;
                Err::<(), _>(StorageError::Transaction("abandoned".into()))
            })
            .unwrap_err();
        assert!(matches!(err, StorageError::Transaction(_)));
        assert_eq!(engine.vector_generation(shadow.id), generation + 1, "an abort bumps nothing");
    }

    #[test]
    fn vectors_round_trip_in_chunk_order() {
        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);
        // Written out of order to prove the read sorts them.
        let records = vec![record(1, 10, "second"), record(0, 10, "first")];
        engine.put_vectors(&shadow, &source, &records).unwrap();

        let read = engine.get_vectors(&shadow, &source).unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].chunk, 0);
        assert_eq!(read[0].text, "first");
        assert_eq!(read[1].text, "second");
        assert_eq!(read[0].vector, vec![0.0, 1.0]);
    }

    #[test]
    fn re_embedding_replaces_chunks_rather_than_accumulating() {
        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);

        engine.put_vectors(&shadow, &source, &[record(0, 10, "old")]).unwrap();
        engine.put_vectors(&shadow, &source, &[record(0, 20, "new")]).unwrap();

        let read = engine.get_vectors(&shadow, &source).unwrap();
        assert_eq!(read.len(), 1, "the chunk should be replaced, not duplicated");
        assert_eq!(read[0].text, "new");
        assert_eq!(read[0].source_hlc, Hlc::new(20, 0));
    }

    #[test]
    fn shortening_a_document_removes_its_orphaned_tail_chunks() {
        // Otherwise the removed text stays searchable forever, and a hit points
        // at content the document no longer contains.
        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);

        let long = vec![record(0, 10, "a"), record(1, 10, "b"), record(2, 10, "c")];
        engine.put_vectors(&shadow, &source, &long).unwrap();
        assert_eq!(engine.get_vectors(&shadow, &source).unwrap().len(), 3);

        engine.put_vectors(&shadow, &source, &[record(0, 20, "a")]).unwrap();
        let read = engine.get_vectors(&shadow, &source).unwrap();
        assert_eq!(read.len(), 1, "chunks 1 and 2 should be gone");
        assert_eq!(read[0].chunk, 0);
    }

    /// Every entry the change feed holds right now, as `(doc id, kind)`.
    fn drain(
        rx: &mut tokio::sync::broadcast::Receiver<std::sync::Arc<kimmy_core::OplogEntry>>,
    ) -> Vec<(DocId, kimmy_core::OpKind)> {
        let mut out = Vec::new();
        while let Ok(entry) = rx.try_recv() {
            out.push((entry.doc_id.clone().unwrap(), entry.kind));
        }
        out
    }

    /// ADR-149's promise for the vector write, in the shape of the ADR-119
    /// test: a document's chunks are one commit however many it holds, and
    /// they reach the change feed only after that commit. The loop this
    /// replaces committed once per chunk and once per stale tail chunk, and
    /// published each as it went.
    #[test]
    fn a_documents_chunks_are_one_commit_however_many_it_holds() {
        use kimmy_core::OpKind;

        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);
        let mut rx = engine.subscribe();

        let commits = engine.commits();
        let five: Vec<VectorRecord> = (0..5).map(|n| record(n, 10, "long")).collect();
        engine.put_vectors(&shadow, &source, &five).unwrap();
        assert_eq!(
            engine.commits() - commits,
            1,
            "five chunks over an empty shadow must be one commit, not five"
        );
        let published = drain(&mut rx);
        assert_eq!(published.len(), 5, "five entries, after the commit: {published:?}");
        assert!(
            published.iter().all(|(_, kind)| *kind == OpKind::Insert),
            "every chunk was new: {published:?}"
        );

        // Shorter now: three chunks replaced in place, two stale tails gone.
        let commits = engine.commits();
        let three: Vec<VectorRecord> = (0..3).map(|n| record(n, 20, "short")).collect();
        engine.put_vectors(&shadow, &source, &three).unwrap();
        assert_eq!(
            engine.commits() - commits,
            1,
            "three replaces and two tail deletes must be one commit, not five"
        );
        let published = drain(&mut rx);
        let replaces = published.iter().filter(|(_, k)| *k == OpKind::Replace).count();
        let deletes = published.iter().filter(|(_, k)| *k == OpKind::Delete).count();
        assert_eq!((replaces, deletes), (3, 2), "exactly 3 replaces and 2 deletes: {published:?}");
        assert_eq!(published.len(), 5, "and nothing else: {published:?}");

        let stored: Vec<u32> =
            engine.get_vectors(&shadow, &source).unwrap().iter().map(|r| r.chunk).collect();
        assert_eq!(stored, vec![0, 1, 2], "the stored chunk set is exactly 0..3");
    }

    /// The twin for the delete: one commit for the whole chunk set, the
    /// generation moved once, and a second delete of a document with no
    /// chunks costs nothing — no commit, no entry, no generation.
    #[test]
    fn deleting_a_documents_vectors_is_one_commit_however_many_it_holds() {
        use kimmy_core::OpKind;

        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);
        let five: Vec<VectorRecord> = (0..5).map(|n| record(n, 10, "text")).collect();
        engine.put_vectors(&shadow, &source, &five).unwrap();
        let mut rx = engine.subscribe();

        let commits = engine.commits();
        let generation = engine.vector_generation(shadow.id);
        assert_eq!(engine.delete_vectors(&shadow, &source).unwrap(), 5);
        assert_eq!(engine.commits() - commits, 1, "five deletes must be one commit, not five");
        let published = drain(&mut rx);
        assert_eq!(published.len(), 5, "five delete entries: {published:?}");
        assert!(published.iter().all(|(_, k)| *k == OpKind::Delete), "{published:?}");
        assert_eq!(engine.vector_generation(shadow.id), generation + 1, "bumped once");
        assert!(engine.get_vectors(&shadow, &source).unwrap().is_empty());

        // Nothing left to delete: nothing happens.
        let commits = engine.commits();
        let generation = engine.vector_generation(shadow.id);
        assert_eq!(engine.delete_vectors(&shadow, &source).unwrap(), 0);
        assert_eq!(engine.commits(), commits, "a delete of nothing must not commit");
        assert!(rx.try_recv().is_err(), "a delete of nothing must not publish");
        assert_eq!(engine.vector_generation(shadow.id), generation, "or move the generation");
    }

    /// The correctness half of one commit: a write that fails at the third
    /// of five chunks leaves the previous chunk set exactly as it was, not a
    /// document with two new chunks in front of its old ones. The failure
    /// is a unique violation on the shadow, which is an ordinary collection
    /// and takes an index like any other.
    #[test]
    fn a_failed_chunk_write_leaves_the_previous_chunk_set_intact() {
        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);
        let from = ChunkSource::from(&source);
        engine
            .create_index(
                "app",
                &shadow.name,
                vec![crate::meta::IndexField { path: "text".into(), descending: false }],
                true,
                None,
            )
            .unwrap();

        let before: Vec<VectorRecord> =
            (0..3).map(|n| record(n, 10, &format!("old {n}"))).collect();
        engine.put_vectors(&shadow, &source, &before).unwrap();
        let raw_before: Vec<Option<bson::Document>> =
            (0..3).map(|n| engine.get(&shadow, &VectorRecord::id(&from, n)).unwrap()).collect();
        let mut rx = engine.subscribe();

        let commits = engine.commits();
        let generation = engine.vector_generation(shadow.id);
        let tail = engine.read_arrival_from(0, 100).unwrap().len();

        // Chunk 2 repeats chunk 0's text, so the third write breaks the
        // unique index against a chunk written earlier in the same scope.
        let torn = vec![
            record(0, 20, "new 0"),
            record(1, 20, "new 1"),
            record(2, 20, "new 0"),
            record(3, 20, "new 3"),
            record(4, 20, "new 4"),
        ];
        let err = engine.put_vectors(&shadow, &source, &torn).unwrap_err();
        assert!(
            matches!(err, StorageError::Core(CoreError::UniqueViolation { .. })),
            "the write fails on the third chunk: {err}"
        );

        assert_eq!(engine.get_vectors(&shadow, &source).unwrap(), before, "the old set is intact");
        for n in 0..3 {
            let raw = engine.get(&shadow, &VectorRecord::id(&from, n)).unwrap();
            assert_eq!(raw, raw_before[n as usize], "chunk {n} is byte-for-byte what it was");
        }
        for n in 3..5 {
            assert!(
                engine.get(&shadow, &VectorRecord::id(&from, n)).unwrap().is_none(),
                "chunk {n} was never committed"
            );
        }
        assert_eq!(engine.commits(), commits, "a failed write must not commit");
        assert!(rx.try_recv().is_err(), "a failed write must not publish");
        assert_eq!(
            engine.read_arrival_from(0, 100).unwrap().len(),
            tail,
            "the oplog must not move"
        );
        assert_eq!(engine.vector_generation(shadow.id), generation, "the generation must not move");
    }

    /// An empty chunk set over a document that has none is not a write: no
    /// commit, and no generation, since nothing an index could see changed.
    #[test]
    fn an_empty_vector_write_over_nothing_costs_nothing() {
        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);
        let mut rx = engine.subscribe();

        let commits = engine.commits();
        let generation = engine.vector_generation(shadow.id);
        engine.put_vectors(&shadow, &source, &[]).unwrap();
        assert_eq!(engine.commits(), commits, "an empty write over nothing must not commit");
        assert!(rx.try_recv().is_err(), "or publish");
        assert_eq!(engine.vector_generation(shadow.id), generation, "or move the generation");
        assert!(engine.get_vectors(&shadow, &source).unwrap().is_empty());
    }

    #[test]
    fn vectors_are_scoped_to_their_source_document() {
        let (engine, shadow, _dir) = with_vectors();
        let a = DocId::Int64(1);
        let b = DocId::Int64(2);

        engine.put_vectors(&shadow, &a, &[record(0, 10, "from a")]).unwrap();
        let mut for_b = record(0, 10, "from b");
        for_b.source = b.clone();
        engine.put_vectors(&shadow, &b, &[for_b]).unwrap();

        assert_eq!(engine.get_vectors(&shadow, &a).unwrap()[0].text, "from a");
        assert_eq!(engine.get_vectors(&shadow, &b).unwrap()[0].text, "from b");

        // Deleting one must not touch the other.
        assert_eq!(engine.delete_vectors(&shadow, &a).unwrap(), 1);
        assert!(engine.get_vectors(&shadow, &a).unwrap().is_empty());
        assert_eq!(engine.get_vectors(&shadow, &b).unwrap().len(), 1);
    }

    #[test]
    fn staleness_is_derived_from_the_document_version() {
        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);
        let stale = |ms| {
            engine.vectors_are_stale(&shadow, &source, Hlc::new(ms, 0), 7, Unstamped::ByVersion)
        };

        // Never embedded: work is needed.
        assert!(stale(10).unwrap());

        engine.put_vectors(&shadow, &source, &[record(0, 10, "text")]).unwrap();
        assert!(!stale(10).unwrap());
        assert!(stale(11).unwrap());
    }

    /// A record's configuration fingerprint is stored with it and read back,
    /// and a chunk made under another configuration marks the document stale
    /// at the same version, where one with no fingerprint is left to the
    /// version (ADR-203).
    #[test]
    fn staleness_reads_the_configuration_each_chunk_carries() {
        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);
        let at = Hlc::new(10, 0);
        let stamped = |chunk, config| VectorRecord { config, ..record(chunk, 10, "t") };

        engine.put_vectors(&shadow, &source, &[stamped(0, Some(u64::MAX))]).unwrap();
        assert_eq!(engine.get_vectors(&shadow, &source).unwrap()[0].config, Some(u64::MAX));
        assert!(
            !engine.vectors_are_stale(&shadow, &source, at, u64::MAX, Unstamped::Stale).unwrap()
        );

        engine
            .put_vectors(&shadow, &source, &[stamped(0, Some(u64::MAX)), stamped(1, Some(7))])
            .unwrap();
        assert!(
            engine.vectors_are_stale(&shadow, &source, at, u64::MAX, Unstamped::ByVersion).unwrap(),
            "one chunk made under another configuration"
        );

        engine.put_vectors(&shadow, &source, &[stamped(0, None)]).unwrap();
        assert_eq!(engine.get_vectors(&shadow, &source).unwrap()[0].config, None);
        assert!(!engine.vectors_are_stale(&shadow, &source, at, 7, Unstamped::ByVersion).unwrap());
        assert!(engine.vectors_are_stale(&shadow, &source, at, 7, Unstamped::Stale).unwrap());
    }

    #[test]
    fn one_stale_chunk_marks_the_document_stale() {
        // A partial re-embed that failed halfway must not look complete.
        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);
        engine
            .put_vectors(&shadow, &source, &[record(0, 20, "fresh"), record(1, 10, "stale")])
            .unwrap();
        let at = Hlc::new(20, 0);
        assert!(engine.vectors_are_stale(&shadow, &source, at, 7, Unstamped::ByVersion).unwrap());
    }

    #[test]
    fn every_vector_can_be_visited_for_a_rebuild() {
        let (engine, shadow, _dir) = with_vectors();
        for i in 1..=3i64 {
            let mut r = record(0, 10, "t");
            r.source = DocId::Int64(i);
            engine.put_vectors(&shadow, &DocId::Int64(i), &[r]).unwrap();
        }

        let mut seen = 0;
        engine
            .for_each_vector(&shadow, crate::WalkScope::Request, |_, _| {
                seen += 1;
                Ok(true)
            })
            .unwrap();
        assert_eq!(seen, 3);
    }

    #[test]
    fn one_documents_chunks_are_read_as_a_run_of_keys() {
        // Ids `1`, `10` and `1#2` all begin with `1`, and `1#2`'s chunk key
        // `1#2#0` sits *inside* `1#`'s run, between `1#2` and `1#3`. The
        // per-document read must return exactly the document's own chunks:
        // pass over the interloper without ending the run, and end the run
        // before `10#0` rather than at the end of the collection.
        let (engine, shadow, _dir) = with_vectors();
        let put = |id: DocId, chunks: &[u32]| {
            let records: Vec<VectorRecord> = chunks
                .iter()
                .map(|&c| {
                    let mut r = record(c, 10, &format!("{id}/{c}"));
                    r.source = id.clone();
                    r
                })
                .collect();
            engine.put_vectors(&shadow, &id, &records).unwrap();
        };
        put(DocId::Int64(1), &[0, 1, 2, 10]);
        put(DocId::Int64(10), &[0]);
        put(DocId::String("1#2".into()), &[0]);
        put(DocId::Int64(2), &[0]);

        let texts = |id: DocId| -> Vec<String> {
            let mut out = Vec::new();
            engine
                .for_each_vector_of(&shadow, &id, |r| {
                    out.push(r.text);
                    Ok(true)
                })
                .unwrap();
            out
        };
        // Chunk order, not key order (which is textual: `#10` before `#2`).
        assert_eq!(texts(DocId::Int64(1)), vec!["1/0", "1/1", "1/2", "1/10"]);
        assert_eq!(texts(DocId::Int64(10)), vec!["10/0"]);
        assert_eq!(texts(DocId::String("1#2".into())), vec!["1#2/0"]);
        assert_eq!(texts(DocId::Int64(2)), vec!["2/0"]);
        assert!(texts(DocId::Int64(3)).is_empty(), "a document with no chunks reads as none");

        // `get_vectors` puts the same run in chunk order.
        let chunks: Vec<u32> = engine
            .get_vectors(&shadow, &DocId::Int64(1))
            .unwrap()
            .iter()
            .map(|r| r.chunk)
            .collect();
        assert_eq!(chunks, vec![0, 1, 2, 10]);

        // The visitor's stop is honoured.
        let mut seen = 0;
        engine
            .for_each_vector_of(&shadow, &DocId::Int64(1), |_| {
                seen += 1;
                Ok(false)
            })
            .unwrap();
        assert_eq!(seen, 1);

        // And a write of `1` leaves the interloper inside its prefix alone:
        // `1#2#0` is not in `1`'s run, so it is neither removed as `1`'s tail
        // nor read as debris at `1`'s ids (ADR-239).
        put(DocId::Int64(1), &[0]);
        assert_eq!(texts(DocId::String("1#2".into())), vec!["1#2/0"]);
        let at = Hlc::new(10, 0);
        let stale =
            engine.vectors_are_stale(&shadow, &DocId::Int64(1), at, 7, Unstamped::ByVersion);
        assert!(!stale.unwrap(), "only `1`'s own chunk is in its run");
    }

    #[test]
    fn a_documents_run_skips_records_that_are_not_vectors() {
        // Same rule as the full scan: a malformed record inside the run is
        // passed over, and a key inside the run that is not a chunk key at all
        // (`1#junk`) neither ends it nor surfaces.
        let (engine, shadow, _dir) = with_vectors();
        engine
            .put_vectors(
                &shadow,
                &DocId::Int64(1),
                &[record(0, 10, "first"), record(9, 10, "last")],
            )
            .unwrap();
        engine.insert(&shadow, bson::doc! { "_id": "1#5", "not": "a vector record" }).unwrap();
        engine.insert(&shadow, bson::doc! { "_id": "1#junk", "not": "a chunk key" }).unwrap();

        let mut seen = Vec::new();
        engine
            .for_each_vector_of(&shadow, &DocId::Int64(1), |r| {
                seen.push(r.text);
                Ok(true)
            })
            .unwrap();
        assert_eq!(seen, vec!["first".to_string(), "last".to_string()]);
        let texts: Vec<String> = engine
            .get_vectors(&shadow, &DocId::Int64(1))
            .unwrap()
            .into_iter()
            .map(|r| r.text)
            .collect();
        assert_eq!(texts, ["first", "last"], "and so does the read of the set");

        // The malformed record sits at one of the document's ids, so the
        // document reads as stale until its own put removes it (ADR-239).
        let at = Hlc::new(10, 0);
        let stale = |engine: &Engine| {
            engine
                .vectors_are_stale(&shadow, &DocId::Int64(1), at, 7, Unstamped::ByVersion)
                .unwrap()
        };
        assert!(stale(&engine));
        engine.put_vectors(&shadow, &DocId::Int64(1), &[record(0, 10, "first")]).unwrap();
        assert!(!stale(&engine));
        assert!(engine.get(&shadow, &DocId::String("1#5".into())).unwrap().is_none());
        assert!(engine.get(&shadow, &DocId::String("1#junk".into())).unwrap().is_some());
    }

    #[test]
    fn a_document_that_is_not_a_vector_record_is_skipped_not_fatal() {
        // A shadow collection is an ordinary collection, so anyone with write
        // access can insert an arbitrary document into one. If that failed the
        // scan, a single bad insert would turn every later search on the
        // collection into a 500 — a client could brick search with one write.
        let (engine, shadow, _dir) = with_vectors();
        engine.put_vectors(&shadow, &DocId::Int64(1), &[record(0, 10, "real")]).unwrap();

        let junk = bson::doc! { "_id": "junk", "not": "a vector record" };
        engine.insert(&shadow, junk).unwrap();

        let mut seen = Vec::new();
        engine
            .for_each_vector(&shadow, crate::WalkScope::Request, |_, r| {
                seen.push(r.text);
                Ok(true)
            })
            .unwrap();
        assert_eq!(seen, vec!["real".to_string()], "the good record must still be visible");
    }

    #[test]
    fn vectors_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let source = DocId::Int64(1);
        {
            let engine = Engine::open(&path).unwrap();
            engine.create_collection("app", "docs").unwrap();
            engine.configure_vectors("app", "docs", config(2)).unwrap();
            let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
            engine.put_vectors(&shadow, &source, &[record(0, 10, "durable")]).unwrap();
        }

        let engine = Engine::open(&path).unwrap();
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        let read = engine.get_vectors(&shadow, &source).unwrap();
        assert_eq!(read[0].text, "durable");
        assert_eq!(read[0].vector, vec![0.0, 1.0]);
    }

    #[test]
    fn the_config_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let engine = Engine::open(&path).unwrap();
            engine.create_collection("app", "docs").unwrap();
            engine.configure_vectors("app", "docs", config(384)).unwrap();
        }

        let engine = Engine::open(&path).unwrap();
        let meta = engine.get_collection("app", "docs").unwrap();
        assert_eq!(meta.vector.expect("config should persist").dim, 384);
    }

    /// A vector-enabled collection holding one document with one chunk.
    fn with_one_embedded_document(engine: &Engine) {
        engine.configure_vectors("app", "docs", config(8)).unwrap();
        let docs = engine.get_collection("app", "docs").unwrap();
        engine
            .insert(
                &docs,
                bson::doc! { "_id": "ghost", "body": "text that is about to be dropped" },
            )
            .unwrap();

        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        engine
            .put_vectors(
                &shadow,
                &DocId::String("ghost".into()),
                &[VectorRecord {
                    source: DocId::String("ghost".into()),
                    chunk: 0,
                    source_hlc: Hlc::ZERO,
                    vector: vec![0.5; 8],
                    text: "text that is about to be dropped".into(),
                    config: None,
                }],
            )
            .unwrap();
    }

    #[test]
    fn dropping_a_collection_takes_its_vectors_with_it() {
        // The shadow is an ordinary collection with its own id, so removing the
        // parent's rows never touched it. Its chunks outlived the documents they
        // described.
        let (engine, _dir) = engine();
        with_one_embedded_document(&engine);
        assert!(engine.get_collection("app", "docs.__vectors").is_ok());

        engine.drop_collection("app", "docs").unwrap();

        assert!(
            engine.get_collection("app", "docs.__vectors").is_err(),
            "the shadow collection must not outlive the collection it describes"
        );
    }

    #[test]
    fn a_collection_recreated_with_the_same_name_does_not_inherit_old_vectors() {
        // Why the leak mattered rather than merely wasted space. The shadow's
        // name is derived from the parent's, so a new collection with the same
        // name adopted the orphaned chunks -- and they were searchable. On a
        // live cluster a document from the dropped collection came back from
        // `vector_search` scoring 1.0, above the new collection's own document,
        // with an `_id` that resolved to nothing.
        let (engine, _dir) = engine();
        with_one_embedded_document(&engine);
        engine.drop_collection("app", "docs").unwrap();

        // The drop's purge finished: this test is about what comes after it.
        engine.finish_purges_now().unwrap();
        engine.create_collection("app", "docs").unwrap();
        engine.configure_vectors("app", "docs", config(8)).unwrap();

        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        let inherited = engine.count(&shadow, crate::WalkScope::Request).unwrap();
        assert_eq!(
            inherited, 0,
            "a new collection must not inherit the vectors of a dropped one with the same name"
        );
    }

    #[test]
    fn dropping_a_shadow_collection_directly_still_works() {
        // The cascade must not recurse: a shadow has no shadow of its own, and
        // `disable_vectors(drop_vectors: true)` drops one by name.
        let (engine, _dir) = engine();
        with_one_embedded_document(&engine);

        engine.drop_collection("app", "docs.__vectors").unwrap();

        assert!(engine.get_collection("app", "docs.__vectors").is_err());
        assert!(
            engine.get_collection("app", "docs").is_ok(),
            "dropping the shadow must not drop the collection it belongs to"
        );
    }

    // -----------------------------------------------------------------------
    // Chunk ids (ADR-239)
    // -----------------------------------------------------------------------

    fn chunk_of(source: &DocId, chunk: u32, hlc_ms: u64, text: &str) -> VectorRecord {
        VectorRecord { source: source.clone(), ..record(chunk, hlc_ms, text) }
    }

    /// A put as the embedding worker makes it, with the subtype the stored
    /// body names.
    fn put_as(
        engine: &Engine,
        shadow: &CollectionMeta,
        document: &DocId,
        source: &ChunkSource,
        records: &[VectorRecord],
    ) {
        let write = VectorWrite::encode(document, source, records).unwrap();
        engine
            .write_batch(WriterHolder::Embedding, |scope| scope.put_vectors(shadow, write))
            .unwrap();
    }

    /// A record stored at `id` whatever the id says, as debris or a member's
    /// racing write would leave it.
    fn stored_at(engine: &Engine, shadow: &CollectionMeta, id: &str, record: &VectorRecord) {
        let doc = bson::serialize_to_document(record).unwrap();
        engine.replace(shadow, &DocId::String(id.into()), doc, true).unwrap();
    }

    fn texts_of(engine: &Engine, shadow: &CollectionMeta, source: &DocId) -> Vec<String> {
        engine.get_vectors(shadow, source).unwrap().into_iter().map(|r| r.text).collect()
    }

    /// Every id the shadow holds, in key order.
    fn ids_in(engine: &Engine, shadow: &CollectionMeta) -> Vec<String> {
        let mut ids = Vec::new();
        engine
            .for_each_doc(shadow, crate::WalkScope::Request, |id, _| {
                ids.push(match id {
                    DocId::String(s) => s,
                    other => format!("{other:?}"),
                });
                Ok(true)
            })
            .unwrap();
        ids
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// X is the 16 bytes 0..15, Y is 100..115: two UUIDs of one length.
    fn x_and_y() -> (DocId, DocId) {
        (
            DocId::Binary { subtype: 4, bytes: (0..16).collect() },
            DocId::Binary { subtype: 4, bytes: (100..116).collect() },
        )
    }

    fn bytes_of(id: &DocId) -> Vec<u8> {
        match id {
            DocId::Binary { bytes, .. } => bytes.clone(),
            other => panic!("not a binary: {other:?}"),
        }
    }

    const AT: u64 = 10;

    fn stale(engine: &Engine, shadow: &CollectionMeta, source: &DocId) -> bool {
        engine.vectors_are_stale(shadow, source, Hlc::new(AT, 0), 7, Unstamped::ByVersion).unwrap()
    }

    /// Test 7, S8 inverted: two 16-byte binary ids keep their own chunks, each
    /// reads its own text, and deleting one leaves the other's. At 0.50.0
    /// both were `bin(16 bytes)#0`: one chunk, the last writer's, and a delete
    /// of either took it.
    #[test]
    fn s8_two_binary_ids_of_one_length_keep_their_own_chunks() {
        let (engine, shadow, _dir) = with_vectors();
        let (x, y) = x_and_y();
        put_as(&engine, &shadow, &x, &ChunkSource::from(&x), &[chunk_of(&x, 0, AT, "document X")]);
        put_as(&engine, &shadow, &y, &ChunkSource::from(&y), &[chunk_of(&y, 0, AT, "document Y")]);
        assert_eq!(
            ids_in(&engine, &shadow),
            vec![
                format!("~b:{}.04#0", hex(&bytes_of(&x))),
                format!("~b:{}.04#0", hex(&bytes_of(&y)))
            ],
            "two chunk sets, each id carrying its bytes and subtype 4"
        );
        assert_eq!(texts_of(&engine, &shadow, &x), ["document X"]);
        assert_eq!(texts_of(&engine, &shadow, &y), ["document Y"]);
        assert!(!stale(&engine, &shadow, &x) && !stale(&engine, &shadow, &y));

        assert_eq!(engine.delete_vectors(&shadow, &y).unwrap(), 1);
        assert_eq!(texts_of(&engine, &shadow, &x), ["document X"], "X keeps its vectors");
        assert!(texts_of(&engine, &shadow, &y).is_empty());
    }

    /// Test 8, one byte string and two subtypes: two documents, each with
    /// its own chunk ids, and a delete of one leaves the other's.
    #[test]
    fn two_subtypes_of_one_byte_string_are_two_documents() {
        let (engine, shadow, _dir) = with_vectors();
        let (x, _) = x_and_y();
        let generic = DocId::Binary { subtype: 0, bytes: bytes_of(&x) };
        put_as(
            &engine,
            &shadow,
            &x,
            &ChunkSource::from(&x),
            &[chunk_of(&x, 0, AT, "as uuid 0"), chunk_of(&x, 1, AT, "as uuid 1")],
        );
        put_as(
            &engine,
            &shadow,
            &generic,
            &ChunkSource::from(&generic),
            &[chunk_of(&generic, 0, AT, "as generic")],
        );
        let hx = hex(&bytes_of(&x));
        assert_eq!(
            ids_in(&engine, &shadow),
            vec![format!("~b:{hx}.00#0"), format!("~b:{hx}.04#0"), format!("~b:{hx}.04#1")]
        );
        assert_eq!(texts_of(&engine, &shadow, &generic), ["as generic"]);
        assert_eq!(texts_of(&engine, &shadow, &x), ["as uuid 0", "as uuid 1"]);
        assert_eq!(engine.delete_vectors(&shadow, &x).unwrap(), 2);
        assert_eq!(texts_of(&engine, &shadow, &generic), ["as generic"]);
    }

    /// A `Decimal128` `_id` has chunks of its own, under `~d:` and its canonical bytes: the
    /// String that is its `Display` text (the twin a display-text id would have shared
    /// chunks with) keeps separate ones, and deleting either leaves the other's.
    #[test]
    fn a_decimal128_id_keeps_its_own_chunks() {
        let (engine, shadow, _dir) = with_vectors();
        let decimal =
            DocId::try_from_bson(&bson::Bson::Decimal128("1.5".parse().unwrap())).unwrap();
        assert!(matches!(decimal, DocId::Decimal128(_)));
        let twin = DocId::String(decimal.to_string());
        put_as(
            &engine,
            &shadow,
            &decimal,
            &ChunkSource::from(&decimal),
            &[chunk_of(&decimal, 0, AT, "the decimal")],
        );
        put_as(
            &engine,
            &shadow,
            &twin,
            &ChunkSource::from(&twin),
            &[chunk_of(&twin, 0, AT, "the string")],
        );
        assert_eq!(
            ids_in(&engine, &shadow),
            vec!["1.5E+0#0".to_string(), "~d:00ffff0000000000000000000000000000000f#0".to_string()],
            "the String keeps its own id and the decimal has its own"
        );
        assert_eq!(texts_of(&engine, &shadow, &decimal), ["the decimal"]);
        assert_eq!(texts_of(&engine, &shadow, &twin), ["the string"]);
        assert_eq!(engine.delete_vectors(&shadow, &decimal).unwrap(), 1);
        assert_eq!(texts_of(&engine, &shadow, &twin), ["the string"], "the twin keeps its vectors");
    }

    /// Test 9, twins: the Int64 7 and the String `"7"`, an ObjectId and the
    /// String of its hex, and a String beginning with `~` and the String of
    /// its own encoding, keep separate chunks; deleting one leaves the other.
    #[test]
    fn twins_keep_separate_chunks() {
        let oid = bson::oid::ObjectId::parse_str("65f1a2b3c4d5e6f708192a3b").unwrap();
        let twins = [
            (DocId::Int64(7), DocId::String("7".into())),
            (DocId::ObjectId(oid), DocId::String(oid.to_hex())),
            (DocId::String("~x".into()), DocId::String("~s:~x".into())),
        ];
        for (a, b) in twins {
            let (engine, shadow, _dir) = with_vectors();
            engine.put_vectors(&shadow, &a, &[chunk_of(&a, 0, AT, "a")]).unwrap();
            engine.put_vectors(&shadow, &b, &[chunk_of(&b, 0, AT, "b")]).unwrap();
            assert_eq!(ids_in(&engine, &shadow).len(), 2, "{a:?} / {b:?}");
            assert_eq!(texts_of(&engine, &shadow, &a), ["a"], "{a:?}");
            assert_eq!(texts_of(&engine, &shadow, &b), ["b"], "{b:?}");
            assert!(!stale(&engine, &shadow, &a) && !stale(&engine, &shadow, &b));
            assert_eq!(engine.delete_vectors(&shadow, &b).unwrap(), 1);
            assert_eq!(texts_of(&engine, &shadow, &a), ["a"], "{a:?} kept");
            assert_eq!(engine.delete_vectors(&shadow, &a).unwrap(), 1);
            assert!(ids_in(&engine, &shadow).is_empty());
        }
    }

    /// Test 10, rule (a): nothing held is stale.
    #[test]
    fn staleness_a_nothing_held() {
        let (engine, shadow, _dir) = with_vectors();
        let (x, _) = x_and_y();
        assert!(stale(&engine, &shadow, &x));
        put_as(&engine, &shadow, &x, &ChunkSource::from(&x), &[chunk_of(&x, 0, AT, "x")]);
        assert!(!stale(&engine, &shadow, &x), "the control: X's own chunk");
    }

    /// Test 10, rule (b): a chunk older than the document, or made under
    /// another configuration, as before.
    #[test]
    fn staleness_b_behind_the_document_or_its_configuration() {
        let (engine, shadow, _dir) = with_vectors();
        let (x, _) = x_and_y();
        put_as(&engine, &shadow, &x, &ChunkSource::from(&x), &[chunk_of(&x, 0, AT - 1, "x")]);
        assert!(stale(&engine, &shadow, &x), "older than the document");
        let other = VectorRecord { config: Some(9), ..chunk_of(&x, 0, AT, "x") };
        put_as(&engine, &shadow, &x, &ChunkSource::from(&x), &[other]);
        assert!(stale(&engine, &shadow, &x), "made under another configuration");
        put_as(&engine, &shadow, &x, &ChunkSource::from(&x), &[chunk_of(&x, 0, AT, "x")]);
        assert!(!stale(&engine, &shadow, &x), "the control");
    }

    /// Test 10, rule (c), and test 12: another document's chunk at the
    /// document's ids is not returned for it, makes it stale, and goes with
    /// its next put, after which it reads as current: no loop.
    #[test]
    fn staleness_d_debris_at_the_documents_ids() {
        let (engine, shadow, _dir) = with_vectors();
        let (s, t) = (DocId::Int64(5), DocId::String("5".into()));
        engine.put_vectors(&shadow, &s, &[chunk_of(&s, 0, AT, "s0")]).unwrap();
        assert!(!stale(&engine, &shadow, &s), "the control");
        // The String "5"'s chunk 1, stored at the Int64 5's id.
        stored_at(&engine, &shadow, "5#1", &chunk_of(&t, 1, AT, "t1"));
        assert_eq!(texts_of(&engine, &shadow, &s), ["s0"], "not returned for S");
        assert!(stale(&engine, &shadow, &s), "debris at S's ids");

        engine.put_vectors(&shadow, &s, &[chunk_of(&s, 0, AT, "s0")]).unwrap();
        assert_eq!(ids_in(&engine, &shadow), ["5#0"], "S's put removed it");
        assert!(!stale(&engine, &shadow, &s), "and S is current: no loop");
    }

    /// Test 10: a put removes every id of its run it does not rewrite, whatever
    /// those hold, and nothing else.
    #[test]
    fn a_put_removes_every_id_of_the_run_it_does_not_rewrite() {
        let (engine, shadow, _dir) = with_vectors();
        let (x, y) = x_and_y();
        let (hx, hy) = (hex(&bytes_of(&x)), hex(&bytes_of(&y)));
        stored_at(&engine, &shadow, &format!("~b:{hx}.04#2"), &chunk_of(&x, 2, AT, "x2"));
        stored_at(&engine, &shadow, &format!("~b:{hx}.04#3"), &chunk_of(&y, 3, AT, "debris"));
        stored_at(
            &engine,
            &shadow,
            &format!("~b:{hx}.00#3"),
            &chunk_of(&y, 3, AT, "other subtype"),
        );
        stored_at(&engine, &shadow, &format!("~b:{hy}.04#0"), &chunk_of(&y, 0, AT, "y0"));

        put_as(&engine, &shadow, &x, &ChunkSource::from(&x), &[chunk_of(&x, 0, AT, "x0 new")]);
        assert_eq!(
            ids_in(&engine, &shadow),
            [format!("~b:{hx}.00#3"), format!("~b:{hx}.04#0"), format!("~b:{hy}.04#0")],
            "what is outside X's run stays"
        );
        assert_eq!(texts_of(&engine, &shadow, &x), ["x0 new"]);
        assert_eq!(texts_of(&engine, &shadow, &y), ["y0"]);
    }

    /// Test 10: a delete removes the chunks naming the document, in its run,
    /// and leaves another document's.
    #[test]
    fn a_delete_removes_only_the_chunks_naming_the_document() {
        let (engine, shadow, _dir) = with_vectors();
        let (x, y) = x_and_y();
        let hx = hex(&bytes_of(&x));
        let hy = hex(&bytes_of(&y));
        put_as(&engine, &shadow, &y, &ChunkSource::from(&y), &[chunk_of(&y, 0, AT, "y0")]);
        stored_at(&engine, &shadow, &format!("~b:{hx}.04#0"), &chunk_of(&x, 0, AT, "x0 new"));
        stored_at(&engine, &shadow, &format!("~b:{hx}.04#1"), &chunk_of(&y, 1, AT, "debris"));

        assert_eq!(engine.delete_vectors(&shadow, &x).unwrap(), 1);
        assert_eq!(ids_in(&engine, &shadow), [format!("~b:{hx}.04#1"), format!("~b:{hy}.04#0")]);
        assert_eq!(texts_of(&engine, &shadow, &y), ["y0"]);
        // And the refused-input removal judges and removes the same chunks.
        stored_at(&engine, &shadow, &format!("~b:{hx}.04#0"), &chunk_of(&x, 0, AT, "x0"));
        let removed =
            engine.delete_vectors_made_under_another(&shadow, &x, 7, Unstamped::Stale).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(ids_in(&engine, &shadow), [format!("~b:{hx}.04#1"), format!("~b:{hy}.04#0")]);
    }

    /// A write's chunk source must be its document's.
    #[test]
    fn a_chunk_source_of_another_document_is_refused() {
        let (x, y) = x_and_y();
        assert!(VectorWrite::encode(&x, &ChunkSource::from(&y), &[]).is_err());
        assert!(
            VectorWrite::encode(&DocId::Int64(7), &ChunkSource::String("7".into()), &[]).is_err()
        );
        assert!(VectorWrite::encode(&x, &ChunkSource::from(&x), &[]).is_ok());
    }

    /// A move of chunk ids is counted in the commit that makes it, and a write
    /// that only replaces in place counts none.
    #[test]
    fn moves_of_chunk_ids_are_counted() {
        let (engine, shadow, _dir) = with_vectors();
        engine.put_vectors(&shadow, &DocId::Int64(1), &[record(0, AT, "a")]).unwrap();
        engine.put_vectors(&shadow, &DocId::Int64(1), &[record(0, AT + 1, "b")]).unwrap();
        assert_eq!(engine.vector_renames(shadow.id).unwrap(), 0, "in place");
        let (x, _) = x_and_y();
        let two = [chunk_of(&x, 0, AT, "x0"), chunk_of(&x, 1, AT, "x1")];
        put_as(&engine, &shadow, &x, &ChunkSource::from(&x), &two);
        assert_eq!(engine.vector_renames(shadow.id).unwrap(), 0, "a first write removes nothing");
        put_as(&engine, &shadow, &x, &ChunkSource::from(&x), &[chunk_of(&x, 0, AT, "x0")]);
        assert_eq!(engine.vector_renames(shadow.id).unwrap(), 1, "a put that removed an id");
    }

    /// A record naming another document is refused in a debug build: in the
    /// document's run it would be debris for good.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "names that document")]
    fn a_record_of_another_document_is_refused_in_a_debug_build() {
        let (x, y) = x_and_y();
        let _ = VectorWrite::encode(&x, &ChunkSource::from(&x), &[chunk_of(&y, 0, AT, "y")]);
    }
}
