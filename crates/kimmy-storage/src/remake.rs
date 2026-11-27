//! A collection that is gone is made again from the base options its name keeps when a fact of it is stamped above its floor (ADR-272,
//! unit P).
//!
//! WHAT IS RULED. A plain collection STANDS when anything of it is stamped above its floor, the later of its own tombstone and its
//! database's, and every member ends in the same life whatever the order its facts arrived in. Where the collection is gone (no collection
//! holds the name here, and a tombstone or the database's drop says why), a fact above the floor makes it again; a fact at or below the
//! floor is history, and nothing the drop removed comes back. The kinds that make it: a document, a delete, `CreateIndex`, `DropIndex`,
//! `ConfigureValidator` and `ConfigureVectors`: exactly what the keep test counts (`late_drop`). A Q7 `LossRecord` does not (it is parked
//! by the unit's next commit), a swap's `OutLoss` touches no collection, and a stage, a recreation and a vector shadow are never made
//! here: a stage's name is its swap's, and a shadow follows its parent's configuration (F2 decides it).
//!
//! WHAT IT MAKES. The options come from the ROW the name's id keeps (`register::BaseOptions`, unit N), not from the fact: the entry that
//! made the collection may be long gone. `created` is the floor's successor (what a re-dated life holds), `incarnation_floor` the floor,
//! `merge_policy_at` the row's `at`, and the collection takes its own tombstone at its database's drop when the floor is the database's
//! (`create_collection_in_txn`). The life carries the `Redated` marker, with `from` the row's `at` and the arrival position of the next
//! entry appended as `first_arrival` and `arrivals_from`: a remade life is the re-dated life reached in the other order, and the register's
//! seed reads the marker, so without it the same facts in two orders would seed different registers.
//!
//! WHAT IT WAITS FOR. Rows a drop left under the id are the purger's: made over them, the new life would inherit a life that ended. The
//! remake then makes nothing and reports [`Remade::Purging`]; the entries route leaves the entry UNTAKEN (`PurgePending`, never history),
//! and the page route waits whole.
//!
//! ONE FUNCTION, FOUR TRANSACTION SHAPES: the entries route before the writer (`remake_from_the_row` takes its own), the page route inside
//! its page transaction and the carry's merge inside the same one (`remake_in_txn`), and the purger's completion (a later commit).

use kimmy_core::{CollectionId, Hlc, Stamp};
use redb::{ReadableDatabase, ReadableTable};

use crate::engine::{Engine, InTxn, WriteTxn, WriterHolder};
use crate::error::Result;
use crate::meta::{CollectionMeta, Redated};
use crate::register::BaseOptions;
use crate::sync::Memo;
use crate::tables;

/// What a remake came to.
pub(crate) enum Remade {
    /// The collection was made again, as it stands now.
    Made(Box<CollectionMeta>),
    /// A collection of the name stands (it did all along, or was made in between): nothing to do.
    Held,
    /// A fact at or below the floor, a name that is never made again (a stage, a recreation, a shadow, a name this member swapped), no
    /// row, or no tombstone: nothing was made.
    Unwanted,
    /// Rows a drop left under the id are not purged yet: nothing was made, the purge was asked for, and the caller must wait for it.
    Purging { db: String, name: String, id: CollectionId },
}

/// What is known, without the writer, of a collection that is not held here: see [`Engine::gone_plan`].
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct GonePlan {
    /// This member keeps an options row for the name's id.
    has_row: bool,
    /// The row's `at` (the greater of the held row's and a page's carried one).
    row_at: Hlc,
    /// The drop of the name's database, if this member holds one.
    database_drop: Option<Hlc>,
    /// The collection could be made again: a row of a name that is not a stage, a recreation or a shadow, not swapped here, not held, and a
    /// floor (a tombstone of its own or its database's) to be above.
    remakable: bool,
    /// The floor: the later of the collection's own tombstone and its database's.
    floor: Option<Hlc>,
}

/// Which life a remake makes (ruled 3:33 PM).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Dating {
    /// The ROW's `at` is above the floor: a creation of the name stands above the drop, and the collection is that creation's life, which the
    /// creation's own entry would have made: `created` is the row's `at`, and no marker (it was never re-dated).
    Creation,
    /// Only a FACT is above the floor, and the row's `at` is at or below it: what is left of the life the drop ended. `created` is the floor's
    /// successor and the Redated marker is set, as a life a drop met later writes after is (the re-dated life the other order reaches).
    Remains,
}

impl GonePlan {
    /// The life a fact stamped `fact`, or with `None` the row alone, makes of a collection that could be made again, or `None` when nothing
    /// is above its floor. The row decides first: a creation above the floor is a creation's life whatever the fact is.
    pub(crate) fn dating(&self, fact: Option<Hlc>) -> Option<Dating> {
        let floor = self.floor?;
        if !self.remakable {
            return None;
        }
        if self.row_at > floor {
            Some(Dating::Creation)
        } else if fact.is_some_and(|fact| fact > floor) {
            Some(Dating::Remains)
        } else {
            None
        }
    }

    /// Whether a fact stamped `fact`, or with `None` the row alone, makes the collection again.
    pub(crate) fn wants(&self, fact: Option<Hlc>) -> bool {
        self.dating(fact).is_some()
    }

    /// Whether `stamp`, of a document addressed to the gone collection, is history by the DATABASE's drop for a member that holds the name's
    /// row: at once, with no fetch. Without the row a member cannot say the name was never swapped, and stops.
    pub(crate) fn is_history_by_the_database(&self, stamp: Hlc) -> bool {
        self.has_row && self.database_drop.is_some_and(|dropped| stamp <= dropped)
    }
}

/// What a Q7 loss record for a collection that is gone here comes to: see [`Engine::judge_a_loss_record_of_a_gone_name`].
pub(crate) enum GoneLoss {
    /// The rules that held before answer (a swapped, hidden or shadow name, or no floor to judge by).
    Old,
    /// History: at or below the floor, or the name's row says `document`.
    History,
    /// This member holds no row of the name: the window stops and a scoped page is asked for.
    Stops,
    /// Above the floor of a `field` name: appended and parked with the row's `R`, no collection made.
    Parks { retention_ms: u64 },
    /// Above the floor, but the drop's rows are not purged yet: left untaken; the `db.name`.
    Purging(String),
}

/// Whether a name's row allows it to be made again at all: never a stage, a recreation or a vector shadow.
fn is_remakable(row: &BaseOptions) -> bool {
    row.stage.is_none()
        && !kimmy_core::is_hidden_name(&row.name)
        && !kimmy_core::vector_meta::is_shadow(&row.name)
}

impl Engine {
    /// The floor of a gone collection: the later of its own tombstone and its database's, or `None` when this member holds neither.
    fn floor_of_a_gone_name(&self, id: CollectionId, db: &str) -> Result<Option<Stamp>> {
        let own = self.collection_dropped_at(id)?;
        let database = self.database_dropped_at(db)?;
        Ok([own, database].into_iter().flatten().max())
    }

    /// What this member can say of the gone collection `id` without the writer: whether a remake could be owed at all, and the floors that
    /// decide it. Read once per id in a batch or a page (`Memo`), so that an entry that is plainly history, however many of them a replay
    /// brings for a dropped collection, costs no read of its own.
    ///
    /// `carried` is a row a page brings and has not merged yet: the page route asks before it applies anything, and the row it will merge
    /// may be the one above the floor.
    pub(crate) fn gone_plan(
        &self,
        id: CollectionId,
        carried: Option<&BaseOptions>,
    ) -> Result<GonePlan> {
        let (row, swapped) = {
            let txn = self.db().begin_read()?;
            let held = crate::register::base_options_of(&txn, id)?;
            let Some(mut row) = held.or_else(|| carried.cloned()) else {
                return Ok(GonePlan::default());
            };
            if let Some(carried) = carried {
                row.at = row.at.max(carried.at);
            }
            // A name the register holds an event of (in effect or ahead of its position), never one that only holds a stage: a stage that is not
            // bound is no swap, and a fact above the floor makes the name stand whatever order the stage and the fact arrived in (decision 16)
            let swapped =
                is_remakable(&row) && crate::register::has_events(&txn, &row.db, &row.name)?;
            (row, swapped)
        };
        let database_drop = self.database_dropped_at(&row.db)?.map(|stamp| stamp.hlc);
        let mut plan =
            GonePlan { has_row: true, row_at: row.at, database_drop, ..GonePlan::default() };
        if swapped || !is_remakable(&row) || self.get_collection(&row.db, &row.name).is_ok() {
            return Ok(plan);
        }
        plan.floor = self.floor_of_a_gone_name(id, &row.db)?.map(|stamp| stamp.hlc);
        plan.remakable = plan.floor.is_some();
        Ok(plan)
    }

    /// Whether a fact stamped `fact` (or, with `None`, the row alone) would make the gone collection `id` again, read without the writer:
    /// the same decision [`Self::remake_in_txn`] takes again under the writer.
    pub(crate) fn remake_is_wanted(
        &self,
        id: CollectionId,
        fact: Option<Hlc>,
        carried: Option<&BaseOptions>,
    ) -> Result<bool> {
        Ok(self.gone_plan(id, carried)?.wants(fact))
    }

    /// [`Self::gone_plan`], read once per id per memo.
    pub(crate) fn memo_gone_plan(&self, memo: &mut Memo, id: CollectionId) -> Result<GonePlan> {
        if let Some(plan) = memo.gone.get(&id) {
            return Ok(*plan);
        }
        let plan = self.gone_plan(id, None)?;
        memo.gone.insert(id, plan);
        Ok(plan)
    }

    /// [`Self::remake_in_txn`] in a transaction of its own, for the entries route: the writer is taken here, the collection made and
    /// committed when it is made.
    pub(crate) fn remake_from_the_row(
        &self,
        id: CollectionId,
        fact: Option<Hlc>,
    ) -> Result<Remade> {
        let txn = self.begin_write(WriterHolder::Ddl)?;
        let remade = self.remake_in_txn(&txn, id, fact)?;
        match &remade {
            Remade::Made(meta) => {
                txn.commit()?;
                tracing::info!(
                    db = %meta.db,
                    collection = %meta.name,
                    id = %meta.id,
                    created = ?meta.created,
                    "made a collection again from its base options: a fact stamped above its floor reached it"
                );
            }
            _ => txn.abort()?,
        }
        Ok(remade)
    }

    /// Make the gone collection `id` again under the writer `txn` holds, if the row it keeps, or a fact stamped `fact`, is above its floor;
    /// the caller commits. Judged HERE, under the writer, and not only before it: a drop landing between the two would otherwise make a
    /// collection no member keeps.
    pub(crate) fn remake_in_txn(
        &self,
        txn: &WriteTxn<'_>,
        id: CollectionId,
        fact: Option<Hlc>,
    ) -> Result<Remade> {
        let Some(row) = crate::register::base_options_in(txn, id)? else {
            return Ok(Remade::Unwanted);
        };
        if !is_remakable(&row) {
            return Ok(Remade::Unwanted);
        }
        {
            let collections = txn.open_table(tables::COLLECTIONS)?;
            if collections.get((row.db.as_str(), row.name.as_str()))?.is_some() {
                return Ok(Remade::Held);
            }
        }
        let Some(floor) = self.floor_of_a_gone_name(id, &row.db)? else {
            return Ok(Remade::Unwanted);
        };
        // the one decision, the same as the read-only plan's
        let plan = GonePlan {
            has_row: true,
            row_at: row.at,
            remakable: true,
            floor: Some(floor.hlc),
            ..GonePlan::default()
        };
        let Some(dating) = plan.dating(fact) else {
            return Ok(Remade::Unwanted);
        };
        // the creation's life is dated by its creation; the remains of the life the drop ended, just above the drop
        let created = match dating {
            Dating::Creation => row.at,
            Dating::Remains => floor.hlc.successor(),
        };
        // A name the register holds an event of is the register's: never made again beside it
        if crate::register::has_events_in(txn, &row.db, &row.name)? {
            return Ok(Remade::Unwanted);
        }
        let made = self.create_collection_in_txn(
            txn,
            &row.db,
            &row.name,
            false,
            Some(created),
            &|_| false,
            || self.next_stamp(),
            None,
            row.merge_policy,
            row.join_retention_ms,
            row.loss_limits,
            None,
        );
        match made {
            Ok(InTxn::Created(mut meta, _)) => {
                // the options are the row's and the policy stamp is the row's `at`; the remains of a life the drop ended carry the marker the
                // other order reaches, naming where the life began (the row's `at`) and the arrival position of the next entry
                let next = crate::late_drop::next_arrival_in(txn)?;
                meta.merge_policy_at = Some(row.at);
                if dating == Dating::Remains {
                    meta.redated = Some(Redated {
                        from: row.at,
                        created: meta.created,
                        first_arrival: Some(next),
                        arrivals_from: Some(next),
                    });
                }
                Engine::put_collection_meta(txn, &meta)?;
                Ok(Remade::Made(meta))
            }
            Ok(InTxn::Exists) => Ok(Remade::Held),
            Ok(InTxn::History) => Ok(Remade::Unwanted),
            Err(crate::StorageError::CollectionPurging { db, name, id }) => {
                Ok(Remade::Purging { db, name, id })
            }
            Err(e) => Err(e),
        }
    }

    /// What the entries route does with a Q7 loss record for a collection that is not held here (unit P, ruled 1:07 PM): the one kind of
    /// fact that does not make the collection again.
    pub(crate) fn judge_a_loss_record_of_a_gone_name(
        &self,
        entry: &kimmy_core::OplogEntry,
    ) -> Result<GoneLoss> {
        let id = entry.collection;
        let row = {
            let txn = self.db().begin_read()?;
            crate::register::base_options_of(&txn, id)?
        };
        // No row: this member cannot say the name was never swapped, nor what its options are: the entry stops the window, and the scoped
        // page of the fetch brings the row.
        let Some(row) = row else { return Ok(GoneLoss::Stops) };
        let plan = self.gone_plan(id, None)?;
        // a name that is swapped, hidden or a shadow, or no floor to judge by: the old rules answer
        let Some(floor) = plan.floor.filter(|_| plan.remakable) else { return Ok(GoneLoss::Old) };
        if entry.stamp.hlc <= floor {
            return Ok(GoneLoss::History);
        }
        // a record of a collection that is not `field` is ignored, as `relay_loss_record` ignores it
        let (kimmy_core::MergePolicy::Field, Some(retention_ms)) =
            (row.merge_policy, row.join_retention_ms)
        else {
            return Ok(GoneLoss::History);
        };
        // parked over the drop's rows, a record would be removed with them or kept for a life that ended: left until the purger is done
        if self.purge_pending(id)? {
            self.prioritise_purge(id);
            return Ok(GoneLoss::Purging(format!("{}.{}", row.db, row.name)));
        }
        Ok(GoneLoss::Parks { retention_ms })
    }

    /// The remake the entries route owes a document or a definition: made, held out as a wait, or nothing. Returns the `db.name` of a
    /// collection whose purge the entry must wait for.
    pub(crate) fn remake_for_an_entry(
        &self,
        memo: &mut Memo,
        id: CollectionId,
        fact: Hlc,
    ) -> Result<Option<String>> {
        let remade = self.remake_from_the_row(id, Some(fact))?;
        // whatever the batch knew about the collection may be wrong now
        *memo = Memo::default();
        Ok(match remade {
            Remade::Purging { db, name, .. } => Some(format!("{db}.{name}")),
            Remade::Made(_) | Remade::Held | Remade::Unwanted => None,
        })
    }
}
