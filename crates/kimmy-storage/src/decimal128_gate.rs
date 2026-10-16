//! The gate on persisting a `Decimal128` where a member older than 0.51 could
//! meet it, and the durable marker that one exists here (ADR-238).
//!
//! # What is gated, and where
//!
//! A member before 0.51 cannot hold a document whose stored `_id` is a
//! `Decimal128`: its `DocId` refuses the type, so the stored body fails every
//! full scan and the snapshot sender there, and an entry or page carrying the
//! new `DocId` variant fails the whole frame, which is asked for again for
//! ever. It also refuses an index definition whose partial filter holds a
//! `Decimal128` operand, records a refusal hint, and never builds it, even
//! after it upgrades. So a **locally originated creation** of either is
//! refused unless the gate this module holds answers yes, at three
//! chokepoints and nowhere else:
//!
//! - `check_insert` (`docs.rs`): an insert, insert-many, bulk and scope
//!   insert, and an upsert through `modify.rs`, `$setOnInsert` included;
//! - `replace_in_txn` (`docs.rs`) when the replace **creates** the row (a
//!   replace of an existing document keeps the stored `_id`, creates nothing,
//!   and is never refused);
//! - `create_index_inner` (`index.rs`) for [`crate::index::CreateOrigin::Local`]
//!   only.
//!
//! The test is the **BSON type of the `_id` the row would store**, not the
//! `DocId` variant: an integral decimal in `i64` range is a `DocId::Int64`
//! whose body keeps `_id: Decimal128(..)`, which an older member decodes as an
//! id and fails on as a body. Replicated, restored and repaired writes are
//! **never** refused: refusing one would wedge replication on the member that
//! applies it, and the member that created it has already answered the gate.
//!
//! # Who answers
//!
//! `kimmy-storage` has no view of membership, so the gate is injected: a
//! predicate the node installs at start ([`crate::Engine::set_decimal128_gate`]),
//! which `kimmy-cluster` builds from the operator's declaration
//! (`cluster.decimal128`) and from what every live peer advertises
//! (`Facts.decimal128`). An engine with no predicate installed allows every
//! creation, which is the key's default with clustering off.
//!
//! # The marker
//!
//! A `META` row, [`tables::META_DECIMAL128_ID_PRESENT`], written in the
//! transaction that writes or applies the first document whose body `_id` has
//! BSON type `Decimal128` on this member, and never removed. A backup carries
//! it (every `META` row it does not single out is copied). It is what an
//! operator reads before a rollback: once it is set anywhere, wiping a member
//! and letting it resync on an older build cannot work. **It is keyed on the
//! BSON type and written on apply as well as on creation**, so an integral
//! decimal (`DocId::Int64`) arriving by replication marks it too; and on a
//! body-less entry (a delete) whose `DocId` is the `Decimal128` variant, which
//! carries such an id in this member's oplog.

use std::sync::Arc;

use bson::Bson;
use kimmy_core::{Decimal128Closed, Decimal128Use, Error as CoreError, Stamp};

use crate::engine::{Engine, WriteTxn};
use crate::error::Result;
use crate::tables;

/// The question the node answers for every gated creation: may this member
/// persist a `Decimal128` where a member older than 0.51 could meet it?
///
/// Called under the storage writer, so it must answer at once: the node's is a
/// lock-free read of the membership view.
pub type Decimal128Predicate =
    Arc<dyn Fn() -> std::result::Result<(), Decimal128Closed> + Send + Sync>;

/// The installed predicate, if any.
#[derive(Default)]
pub(crate) struct Decimal128State {
    gate: parking_lot::RwLock<Option<Decimal128Predicate>>,
}

/// Whether a stored `_id` value is one only a 0.51 member can hold.
pub(crate) fn id_value_is_decimal128(value: &Bson) -> bool {
    matches!(value, Bson::Decimal128(_))
}

/// Write the marker in `txn` unless it is already there. `stamp` is the write
/// that carries the document; it is kept for an operator to read, and nothing
/// reads more than whether the row is present.
pub(crate) fn mark_in_txn(txn: &WriteTxn<'_>, stamp: &Stamp) -> Result<()> {
    use redb::ReadableTable;
    let mut meta = txn.open_table(tables::META)?;
    if meta.get(tables::META_DECIMAL128_ID_PRESENT)?.is_none() {
        meta.insert(tables::META_DECIMAL128_ID_PRESENT, stamp.encode().as_bytes())?;
    }
    Ok(())
}

impl Engine {
    /// Install the predicate every gated creation asks (see the module
    /// documentation). Installed once at start, before anything serves; a
    /// later call replaces it.
    pub fn set_decimal128_gate(&self, gate: Decimal128Predicate) {
        *self.decimal128.gate.write() = Some(gate);
    }

    /// Refuse `what` unless the installed predicate allows it, or none is
    /// installed. Only the three local chokepoints call this.
    pub(crate) fn refuse_unless_decimal128_allowed(&self, what: Decimal128Use) -> Result<()> {
        let gate = self.decimal128.gate.read().clone();
        match gate.map_or(Ok(()), |gate| gate()) {
            Ok(()) => Ok(()),
            Err(closed) => Err(CoreError::Decimal128Gated { what, closed }.into()),
        }
    }

    /// Whether a document whose body `_id` is a `Decimal128` has ever been
    /// written or applied on this member: the durable marker, as
    /// `kimmy_decimal128_id_present` and `/readyz` report it.
    pub fn decimal128_id_present(&self) -> Result<bool> {
        use redb::ReadableDatabase;
        let txn = self.db().begin_read()?;
        let meta = txn.open_table(tables::META)?;
        Ok(meta.get(tables::META_DECIMAL128_ID_PRESENT)?.is_some())
    }
}

#[cfg(test)]
impl Engine {
    /// Remove the marker, as a store that met a decimal `_id` before it ran 0.51
    /// has none.
    pub(crate) fn test_remove_decimal128_marker(&self) {
        let txn = self.begin_write(crate::engine::WriterHolder::Ddl).unwrap();
        txn.open_table(tables::META).unwrap().remove(tables::META_DECIMAL128_ID_PRESENT).unwrap();
        txn.commit().unwrap();
    }
}
