//! The winner a `conflictResolved` event names, read when the event is delivered (Q7 Step 4, C8).
//!
//! A loss record names the write that lost, never the one that won: the winner is the cell now at the place the record is about, read
//! from the document's stored state at delivery. So a replayed event can show a different winner than its first delivery, and a record
//! that is stale beside a live register (design 7.2) can show the loser itself as the winner. A client that needs the document reads it.

use kimmy_core::CollectionId;
use kimmy_core::DocId;
use kimmy_core::merge_state::{LossAt, MStamp, Value};

use crate::engine::Engine;
use crate::error::Result;
use redb::ReadableDatabase;

use crate::{codec, tables};

/// The cell now at the place a loss record is about.
#[derive(Clone, Debug, PartialEq)]
pub struct ConflictWinner {
    /// The stamp of the cell (of the standing life, for a record about the whole document).
    pub stamp: MStamp,
    /// The value now at the path (`None` for a record about the whole document: read the document).
    pub value: Option<Value>,
}

impl Engine {
    /// The winner of the loss record about `at` of document `id` of `collection`, as the stored state has it now: `None` when the
    /// collection or the document is not held, the document is deleted, or no cell stands at the path.
    pub fn conflict_winner(
        &self,
        collection: CollectionId,
        id: &DocId,
        at: &LossAt,
    ) -> Result<Option<ConflictWinner>> {
        let Some(coll) = self.collection_by_id(collection)? else { return Ok(None) };
        let key = crate::docs::doc_key(id)?;
        let txn = self.db().begin_read()?;
        let docs = txn.open_table(tables::DOCS)?;
        let Some(raw) = docs.get((coll.id.0, key.as_slice()))? else { return Ok(None) };
        let record = codec::decode_doc_record_with_meta(raw.value())?;
        let Some(meta) = record.meta.as_deref() else { return Ok(None) };
        let state = crate::field_write::load(Some(meta))?;
        let options = self.field_writer(&coll)?.options();
        Ok(match at {
            LossAt::Document => {
                state.standing_life(&options).map(|stamp| ConflictWinner { stamp, value: None })
            }
            LossAt::Field(path) => state.winner(path, &options).map(|cell| {
                // A counter's value is its total, which the register under it does not hold.
                let value = state
                    .visible(&options)
                    .and_then(|read| read.get(path).cloned())
                    .unwrap_or_else(|| cell.value.clone());
                ConflictWinner { stamp: cell.stamp, value: Some(value) }
            }),
        })
    }
}
