//! The definition repair (repair design section 12): a scoped snapshot's **first page
//! only**, applied under [`crate::SnapshotPolicy::Definitions`], to build one plain index
//! this member lacks by name and a peer holds. Documents are ignored; nothing else of the
//! page is applied; every other kind of definition difference stays a finding for the
//! operator.

use std::cell::Cell;

use kimmy_core::{CollectionId, Hlc, IndexMeta, Stamp};
use tracing::{debug, warn};

use crate::error::Result;
use crate::meta::CollectionMeta;
use crate::snapshot::{CollectionState, SnapshotApplied, SnapshotPage, SnapshotProgress};

/// What a definition repair did with a page's indexes: the `result` label of
/// `kimmy_sync_definition_repair_indexes_total`. `built` counts **new builds only**
/// (`IndexCreated::Built` is also returned for a held definition that was only relogged,
/// which the veto below makes unreachable; a test pins both).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DefinitionRestore {
    /// Indexes built.
    pub built: usize,
    /// The definition is one this build cannot apply (ADR-123): warned, counted in
    /// `kimmy_sync_ddl_refused_total`, and a refusal hint written, so the next comparison
    /// is `explained`.
    pub refused: usize,
    /// The name is held here under a different definition.
    pub rival: usize,
    /// The index is unique: building it can refuse documents this member holds.
    pub unique: usize,
    /// The index is a TTL index: the next expiry pass would delete documents.
    pub ttl: usize,
    /// The index belongs to a life this member has buried (no stamp, or at or below the
    /// standing life's incarnation floor).
    pub history: usize,
    /// This member dropped the index after the page's definition was made.
    pub tombstoned: usize,
    /// The index is at or below the collected-through horizon: a drop this member made
    /// may have been collected, so the index cannot be told from one it dropped.
    pub ambiguous: usize,
    /// The collection is gone here.
    pub gone: usize,
    /// The collection holds more live documents than the gate.
    pub too_large: usize,
}

impl DefinitionRestore {
    /// The result labels and counts, in the order of `kimmy_sync_definition_repair_indexes_total`.
    pub fn counts(&self) -> [usize; DEFINITION_RESULTS.len()] {
        [
            self.built,
            self.refused,
            self.rival,
            self.unique,
            self.ttl,
            self.history,
            self.tombstoned,
            self.ambiguous,
            self.gone,
            self.too_large,
        ]
    }
}

/// The labels of [`DefinitionRestore::counts`].
pub const DEFINITION_RESULTS: [&str; 10] = [
    "built",
    "refused",
    "rival",
    "unique",
    "ttl",
    "history",
    "tombstoned",
    "ambiguous",
    "gone",
    "too_large",
];

/// Whether two definitions of one name are the same definition.
fn same_definition(a: &IndexMeta, b: &IndexMeta) -> bool {
    a.fields == b.fields
        && a.unique == b.unique
        && a.enforcement == b.enforcement
        && a.expire_after_secs == b.expire_after_secs
        && a.partial_filter == b.partial_filter
}

impl crate::Engine {
    /// Apply the first page of a scoped pull under `Definitions`. The pull ends here: it
    /// never asks for page two, persists no cursor, forgets none, and grants no coverage.
    pub(crate) fn apply_definitions_page(
        &self,
        progress: &mut SnapshotProgress,
        page: &SnapshotPage,
    ) -> Result<SnapshotApplied> {
        debug_assert!(progress.scope().is_some(), "a Definitions pull is always scoped");
        if page.dropped.is_some() {
            return Ok(SnapshotApplied { sender_dropped: true, ..Default::default() });
        }
        let scope = progress.scope().expect("a Definitions pull is scoped");
        let limit = progress.max_index_documents();
        let definitions = match page
            .collections
            .iter()
            .find(|state| CollectionId::derive(&state.db, &state.name) == scope)
        {
            Some(state) => self.restore_definitions(state, page.tombstone_horizon, limit)?,
            // The page names no such collection: the sender holds it under no definition
            // (a sender that predates the scope serves its whole database, whose first page
            // need not carry it).
            None => DefinitionRestore { gone: 1, ..Default::default() },
        };
        progress.finish_after_one_page();
        Ok(SnapshotApplied { definitions, ..Default::default() })
    }

    /// Build at most one plain index `state` carries and this member lacks, if every
    /// clause of the table in repair design section 12 admits it.
    fn restore_definitions(
        &self,
        state: &CollectionState,
        sender_horizon: Option<Hlc>,
        limit: u64,
    ) -> Result<DefinitionRestore> {
        let mut out = DefinitionRestore::default();
        let id = CollectionId::derive(&state.db, &state.name);
        let Some(standing) = Self::unless_gone(self.get_collection(&state.db, &state.name))? else {
            out.gone += 1;
            return Ok(out);
        };
        // A vector shadow is its own collection and is not repaired here: embedding is not
        // started by a repair.
        if kimmy_core::vector_meta::is_shadow(&state.name) {
            return Ok(out);
        }
        let own_horizon = self.tombstone_horizon()?;
        let mut candidate: Option<&IndexMeta> = None;
        for index in &state.indexes {
            if let Some(held) = standing.indexes.iter().find(|i| i.name == index.name) {
                if !same_definition(held, index) {
                    out.rival += 1;
                }
                continue;
            }
            if index.unique {
                out.unique += 1;
                continue;
            }
            if index.expire_after_secs.is_some() {
                out.ttl += 1;
                continue;
            }
            let Some(created) = index.created else {
                out.history += 1;
                continue;
            };
            if standing.incarnation_floor.is_some_and(|floor| created.hlc <= floor) {
                out.history += 1;
                continue;
            }
            if self.index_dropped_at(id, index.id)?.is_some_and(|at| created < at) {
                out.tombstoned += 1;
                continue;
            }
            // The index analogue of a document row's admission: only an index made after
            // every tombstone cutoff either member has applied. An absent horizon on
            // either side fails closed.
            let admitted = match (own_horizon, sender_horizon) {
                (Some(own), Some(sender)) => created.hlc > own.max(sender),
                _ => false,
            };
            if !admitted {
                out.ambiguous += 1;
                continue;
            }
            // The one build of this repair is the first that passes, chosen before the
            // writer is taken.
            if candidate.is_none() {
                candidate = Some(index);
            }
        }
        let Some(index) = candidate else {
            return Ok(out);
        };
        let created = index.created.expect("a candidate carries its stamp");
        // The gate on the requester's own live count, re-read immediately before the build:
        // a catch-up or a write load can grow the collection between the plan and here.
        if self.count_by_id(id)?.unwrap_or(0) > limit {
            out.too_large += 1;
            return Ok(out);
        }
        warn!(
            db = %state.db,
            collection = %state.name,
            index = %index.name,
            documents = self.count_by_id(id)?.unwrap_or(0),
            "a definition repair is building an index and holds the writer"
        );
        #[cfg(test)]
        crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::DefinitionRepair);
        // Judged again under the writer, where a drop that landed since the check wins.
        let vetoed_by: Cell<Option<&'static str>> = Cell::new(None);
        let veto =
            |standing: &CollectionMeta, dropped: Option<Stamp>| -> bool {
                let reason =
                    if standing.incarnation_floor.is_some_and(|floor| created.hlc <= floor) {
                        Some("history")
                    } else if dropped.is_some_and(|at| created < at) {
                        Some("tombstoned")
                    } else if standing.indexes.iter().any(|i| i.name == index.name) {
                        // Held now (a rival create landed): the build would mutate a held index.
                        Some("rival")
                    } else if self.tombstone_horizon().ok().flatten().is_none_or(|own| {
                        created.hlc <= own.max(sender_horizon.unwrap_or(Hlc::MAX))
                    }) {
                        Some("ambiguous")
                    } else {
                        None
                    };
                vetoed_by.set(reason);
                reason.is_some()
            };
        let built = self.create_index_inner(
            &state.db,
            &state.name,
            index.fields.clone(),
            index.unique,
            index.enforcement,
            Some(index.name.clone()),
            index.expire_after_secs,
            index.partial_filter.clone(),
            crate::index::CreateOrigin::Restored(created),
            &veto,
        );
        match crate::sync::settle(built)? {
            crate::sync::Ddl::Applied((crate::index::IndexCreated::Built(_), violations)) => {
                out.built += 1;
                if !violations.is_empty() {
                    let current = self.get_collection(&state.db, &state.name)?;
                    self.report_index_backfill_violations(&current, &violations)?;
                }
            }
            crate::sync::Ddl::Applied((_, _)) => match vetoed_by.get() {
                Some("history") => out.history += 1,
                Some("tombstoned") => out.tombstoned += 1,
                Some("ambiguous") => out.ambiguous += 1,
                _ => out.rival += 1,
            },
            crate::sync::Ddl::Gone | crate::sync::Ddl::PurgePending(_) => out.gone += 1,
            crate::sync::Ddl::Refused(reason) => {
                out.refused += 1;
                warn!(
                    db = %state.db,
                    collection = %state.name,
                    index = %index.name,
                    reason = %reason,
                    "skipped an index in a definition repair this node cannot build; the \
                     definition stands on its peers and the divergence is counted in \
                     kimmy_sync_ddl_refused_total"
                );
                self.note_refused_index(id, index, created)?;
            }
        }
        debug!(collection = %id, ?out, "a definition repair applied");
        Ok(out)
    }
}
