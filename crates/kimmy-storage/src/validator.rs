//! A collection's validator (ADR-257): the `$jsonSchema` a client write must
//! satisfy, held as collection metadata and replicated like a vector
//! configuration.
//!
//! **Two halves.** The definition is a stamped piece of metadata
//! ([`CollectionMeta::validator`], [`CollectionMeta::validator_changed`]) that
//! travels as a `ConfigureValidator` entry and on a snapshot page, last writer
//! winning on the stamp (ADR-226's rule, minus the shadow collection a vector
//! configuration needs). The enforcement is on the member that **accepts** the
//! write and nowhere else: the write bodies call [`Engine::check_validation`]
//! beside the size caps (ADR-250), and a replicated entry, a snapshot page, a
//! repair or a restore never reaches it. A member that has not yet received a
//! change therefore accepts a write the validator would refuse, and a document
//! a peer accepted before the validator reached it is kept; neither is rechecked.
//!
//! **Parsed once.** The stored form is the schema document. The parsed
//! schema is kept in a cache keyed by collection, valid for exactly the
//! encoding it was parsed from, so a changed validator is parsed again and an
//! unchanged one never is. A stored schema that no longer parses (only a build
//! that changed the dialect can leave one) is **not** a reason to refuse every
//! write to the collection: it is logged at `error`, counted, and the
//! collection behaves as if it had no validator.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bson::{Bson, Document};
use kimmy_core::json_schema::Schema;
use kimmy_core::{
    CollectionId, DocId, Error as CoreError, Stamp, ValidationAction, ValidationLevel,
    ValidatorSpec, vector_meta,
};
use parking_lot::{Mutex, RwLock};
use tracing::{error, info, warn};

use crate::engine::WriterHolder;
use crate::error::{Result, StorageError};
use crate::meta::CollectionMeta;
use crate::walk::WalkScope;

/// How a validator change reached this member, and so what it is judged by.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Validated {
    /// A client's change here: stamped under the writer, above the standing stamp,
    /// and logged.
    Locally,
    /// A peer's `ConfigureValidator` entry, stamped so.
    FromEntry(Stamp),
    /// A snapshot page's definition, with the stamp of the change it describes.
    FromSnapshot(Stamp),
    /// A late-drop sweep removing a validator of the life before (ADR-231), at the
    /// stamp given. Judged strictly, as for a vector configuration: a change above it
    /// since the sweep read it is the newer life's and stays.
    Swept(Stamp),
}

impl Validated {
    fn stamp(self) -> Option<Stamp> {
        match self {
            Self::Locally => None,
            Self::FromEntry(stamp) | Self::FromSnapshot(stamp) | Self::Swept(stamp) => Some(stamp),
        }
    }
}

/// The most validator violations one page of the report returns.
pub const VIOLATIONS_PAGE_MAX: usize = 10_000;

/// The most documents one page of the report examines, whether or not they
/// fail: the report is a scan, and a page of it is bounded work.
pub const VIOLATIONS_SCAN_PAGE: usize = 10_000;

/// A validator's schema parsed, with its two policies.
#[derive(Debug)]
pub struct Compiled {
    schema: Schema,
    pub action: ValidationAction,
    pub level: ValidationLevel,
}

impl Compiled {
    /// Why `doc` fails the schema, or `None` when it passes.
    pub fn explain(&self, doc: &Document) -> Option<String> {
        self.schema.explain_failure(doc)
    }
}

/// Parse a validator's schema, as setting it does: an `Err` is the `400` a bad
/// schema is, naming the keyword and where in the schema it stands.
pub fn check_validator_spec(spec: &ValidatorSpec) -> Result<()> {
    parse(&spec.schema).map(|_| ()).map_err(StorageError::Core)
}

fn parse(schema: &Document) -> std::result::Result<Schema, CoreError> {
    Schema::parse(&Bson::Document(schema.clone()))
}

struct Cached {
    /// The encoding of the spec this was parsed from: what makes the entry the right
    /// one for the metadata in hand. Compared as bytes, not as trees, for the reason
    /// `Engine::definition_is` gives (a NaN is not equal to itself).
    encoded: Vec<u8>,
    compiled: Option<Arc<Compiled>>,
}

#[derive(Default)]
struct Count {
    warnings: u64,
    refusals: u64,
    /// When the last `warn` line for the collection was written, and how many
    /// warnings it has stood for since.
    logged: Option<Instant>,
    unlogged: u64,
}

/// What a collection's validator has done on this member since it started, for the
/// scrape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidationCount {
    pub db: String,
    pub coll: String,
    /// Writes accepted under `validationAction: "warn"` that failed the schema.
    pub warnings: u64,
    /// Writes refused under `validationAction: "error"`.
    pub refusals: u64,
}

/// The engine's validator state: the parsed schemas, and the counters.
#[derive(Default)]
pub(crate) struct ValidatorState {
    compiled: RwLock<HashMap<CollectionId, Cached>>,
    counts: Mutex<BTreeMap<(String, String), Count>>,
    unparseable: AtomicU64,
}

/// One warning line per collection per this long; the lines in between are counted
/// into the next.
const WARN_EVERY: Duration = Duration::from_secs(1);

impl crate::Engine {
    /// Set, replace or (with `None`) remove a collection's validator.
    ///
    /// A schema that does not parse is refused here, with the parser's message. A
    /// removal of a validator that is not there is a change all the same: stamped and
    /// logged, so a snapshot page carrying it passes the receiver's coverage gate
    /// (ADR-163), as a disable of a vector configuration already off does (ADR-226).
    pub fn set_validator(
        &self,
        db: &str,
        collection: &str,
        validator: Option<ValidatorSpec>,
    ) -> Result<CollectionMeta> {
        if let Some(spec) = &validator {
            check_validator_spec(spec)?;
        }
        self.set_validator_inner(db, collection, validator, Validated::Locally, &|_| Ok(false))
    }

    /// The change itself, by whichever route it arrived. Never parses the schema of a
    /// change from elsewhere: a replicated write is not refused (ADR-257), and a
    /// schema this build cannot read is handled where it is read.
    pub(crate) fn set_validator_inner(
        &self,
        db: &str,
        collection: &str,
        validator: Option<ValidatorSpec>,
        by: Validated,
        history: &dyn Fn(&CollectionMeta) -> Result<bool>,
    ) -> Result<CollectionMeta> {
        if vector_meta::is_shadow(collection) {
            return Err(StorageError::Core(CoreError::InvalidName {
                name: collection.to_string(),
                reason: "a validator cannot be set on a shadow collection",
            }));
        }
        // A loop, not a self-call: see `configure_vectors_inner`.
        for _ in 0..crate::Engine::MAX_DEFINITION_RETRIES {
            let mut meta = self.get_collection(db, collection)?;
            let read = meta.clone();
            // A change of a life that has since been dropped and recreated here is
            // history, judged on the definition read here and held unchanged by
            // `definition_is` below.
            if history(&meta)? {
                return Ok(meta);
            }
            // Last writer wins, on the stamp: a change at or below the one standing is
            // the same change again, or an older one arriving late. A sweep is judged
            // strictly, so a change at the swept stamp itself goes.
            let swept = matches!(by, Validated::Swept(_));
            if by.stamp().is_some_and(|incoming| {
                meta.validator_changed
                    .is_some_and(|c| if swept { c > incoming } else { c >= incoming })
            }) {
                return Ok(meta);
            }
            meta.validator = validator.clone();
            let txn = self.begin_write(WriterHolder::Ddl)?;
            if !crate::Engine::definition_is(&txn, &read)? {
                txn.abort()?;
                continue;
            }
            let stamp = match by {
                Validated::Locally => Some(self.next_stamp_above(meta.validator_changed)),
                other => other.stamp(),
            };
            meta.validator_changed = stamp;
            crate::Engine::put_collection_meta(&txn, &meta)?;
            let logged = if matches!(by, Validated::Locally) {
                let entry = crate::engine::ddl_entry(
                    stamp.expect("a local change mints its stamp"),
                    kimmy_core::OpKind::ConfigureValidator,
                    meta.id,
                    &kimmy_core::ValidatorSet {
                        db: db.to_string(),
                        collection: collection.to_string(),
                        validator,
                    },
                )?;
                crate::engine::append_oplog(&txn, &entry)?;
                Some(entry)
            } else {
                None
            };
            txn.commit()?;
            if let Some(entry) = logged {
                self.publish(vec![entry]);
            }
            info!(db, collection, set = meta.validator.is_some(), "changed the validator");
            return Ok(meta);
        }
        Err(crate::Engine::retries_exhausted_error(db, collection))
    }

    /// The collection's validator, parsed, or `None` when it has none or the stored
    /// schema does not parse (logged and counted, once per change of it).
    pub fn validator_of(&self, coll: &CollectionMeta) -> Option<Arc<Compiled>> {
        let spec = coll.validator.as_ref()?;
        let encoded = match bson::serialize_to_vec(spec) {
            Ok(encoded) => encoded,
            Err(e) => {
                error!(db = %coll.db, collection = %coll.name, error = %e, "a validator that cannot be encoded; the collection is not validated");
                return None;
            }
        };
        if let Some(cached) = self.validators.compiled.read().get(&coll.id)
            && cached.encoded == encoded
        {
            return cached.compiled.clone();
        }
        let compiled = match parse(&spec.schema) {
            Ok(schema) => {
                Some(Arc::new(Compiled { schema, action: spec.action, level: spec.level }))
            }
            Err(e) => {
                self.validators.unparseable.fetch_add(1, Ordering::Relaxed);
                error!(
                    db = %coll.db,
                    collection = %coll.name,
                    error = %e,
                    "the stored validator does not parse in this build; the collection is not \
                     validated until it is replaced or removed (counted in \
                     kimmy_validator_unparseable_total)"
                );
                None
            }
        };
        self.validators
            .compiled
            .write()
            .insert(coll.id, Cached { encoded, compiled: compiled.clone() });
        compiled
    }

    /// The check every client write makes on the document it is about to store.
    ///
    /// `before` is the document as stored when the write replaces one, `None` for an
    /// insert (an upsert that creates the document included). At level `moderate` a
    /// replacement of a document that did not conform before is not checked. A
    /// failure is a [`CoreError::DocumentInvalid`] under action `error`; under `warn`
    /// the write passes, is logged and counted.
    ///
    /// Only the engine's client-facing write bodies call this. A replicated entry, a
    /// snapshot page and a repair write through other paths and are never refused
    /// (ADR-257).
    pub(crate) fn check_validation(
        &self,
        coll: &CollectionMeta,
        before: Option<&Document>,
        after: &Document,
    ) -> Result<()> {
        if coll.validator.is_none() {
            return Ok(());
        }
        let Some(compiled) = self.validator_of(coll) else {
            return Ok(());
        };
        if compiled.level == ValidationLevel::Moderate
            && before.is_some_and(|before| compiled.schema.explain_failure(before).is_some())
        {
            return Ok(());
        }
        let Some(reason) = compiled.explain(after) else {
            return Ok(());
        };
        match compiled.action {
            ValidationAction::Error => {
                self.count_validation(coll, |c| c.refusals += 1);
                Err(CoreError::DocumentInvalid { reason }.into())
            }
            ValidationAction::Warn => {
                let unlogged = self.count_validation(coll, |c| c.warnings += 1);
                if let Some(unlogged) = unlogged {
                    warn!(
                        db = %coll.db,
                        collection = %coll.name,
                        reason = %reason,
                        also = unlogged,
                        "accepted a write that fails the collection's validator \
                         (validationAction warn); counted in kimmy_validation_warnings_total"
                    );
                }
                Ok(())
            }
        }
    }

    /// Bump a collection's counters. Answers, for a warning, how many earlier warnings
    /// the line about to be written stands for, or `None` when the line is held back
    /// (one per collection per second).
    fn count_validation(
        &self,
        coll: &CollectionMeta,
        bump: impl FnOnce(&mut Count),
    ) -> Option<u64> {
        let mut counts = self.validators.counts.lock();
        let count = counts.entry((coll.db.clone(), coll.name.clone())).or_default();
        let warnings = count.warnings;
        bump(count);
        if count.warnings == warnings {
            return None;
        }
        let due = count.logged.is_none_or(|at| at.elapsed() >= WARN_EVERY);
        if !due {
            count.unlogged += 1;
            return None;
        }
        count.logged = Some(Instant::now());
        Some(std::mem::take(&mut count.unlogged))
    }

    /// The member's validator totals, as `(warnings, refusals)`, for the scrape: one
    /// series each, with no collection named.
    pub fn validation_totals(&self) -> (u64, u64) {
        let counts = self.validators.counts.lock();
        counts.values().fold((0, 0), |(w, r), c| (w + c.warnings, r + c.refusals))
    }

    /// Each collection's validator counters: those that have counted anything, in
    /// name order.
    pub fn validation_counts(&self) -> Vec<ValidationCount> {
        self.validators
            .counts
            .lock()
            .iter()
            .map(|((db, coll), count)| ValidationCount {
                db: db.clone(),
                coll: coll.clone(),
                warnings: count.warnings,
                refusals: count.refusals,
            })
            .collect()
    }

    /// Stored validators that did not parse in this build, counted when each was
    /// first read.
    pub fn validators_unparseable(&self) -> u64 {
        self.validators.unparseable.load(Ordering::Relaxed)
    }

    /// One page of the documents that fail the collection's current validator,
    /// scanning in `_id` order after `after`.
    ///
    /// **A scan, bounded per page**: at most [`VIOLATIONS_SCAN_PAGE`] documents are
    /// examined and at most `limit` failures returned, and the position where the
    /// page stopped is returned when the collection holds more. Nothing is persisted;
    /// a collection with no validator answers an empty page.
    pub fn validator_violations(
        &self,
        coll: &CollectionMeta,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ValidatorViolations> {
        let mut page = ValidatorViolations::default();
        let Some(compiled) = self.validator_of(coll) else {
            return Ok(page);
        };
        let limit = limit.clamp(1, VIOLATIONS_PAGE_MAX);
        let mut stopped_at: Option<Vec<u8>> = None;
        let mut more = false;
        self.for_each_doc_after(coll, after, WalkScope::Request, |id: DocId, doc| {
            if page.scanned == VIOLATIONS_SCAN_PAGE || page.rows.len() == limit {
                more = true;
                return Ok(false);
            }
            page.scanned += 1;
            stopped_at = Some(crate::docs::doc_key(&id)?);
            if let Some(reason) = compiled.explain(&doc) {
                page.rows.push((
                    doc.get(crate::docs::ID_FIELD).cloned().unwrap_or_else(|| id.to_bson()),
                    reason,
                ));
            }
            Ok(true)
        })?;
        page.next = if more { stopped_at } else { None };
        Ok(page)
    }
}

/// A page of [`Engine::validator_violations`].
#[derive(Debug, Default)]
pub struct ValidatorViolations {
    /// Each failing document's `_id` as stored, and why it fails.
    pub rows: Vec<(Bson, String)>,
    /// How many documents the page examined.
    pub scanned: usize,
    /// The key to resume after, when the collection holds more to examine.
    pub next: Option<Vec<u8>>,
}
