//! The field path of a local write: what a `field` collection stores for an insert, a replace
//! and a delete.
//!
//! Pure functions, no transaction. A document of a `field` collection is stored as a record
//! whose `meta` is its merge state in the compact form (`MergeState::encode_compact`) and whose
//! body is the document that state renders. A write loads the state out of the stored record
//! (or starts from nothing), makes the write through the [`Writer`] with the stamp the engine
//! mints, and returns the new meta and the rendered body. The caller writes the record and the
//! oplog entry (the entry carries the same meta).
//!
//! * **One stamp.** The engine's `next_stamp` is called only when the writer needs a stamp, so a
//!   write that changes nothing mints nothing and moves no clock (ADR-208). The record's stamp is
//!   that stamp, which is `max(state.vv)`: the writer's version vector entry is set to the stamp
//!   it was given, and the stamp is strictly above everything the state has seen.
//! * **No loss records.** One member's writes are sequential, so the state of a document that
//!   only this member has written holds no loss record.
//! * **The document policy does not come here.** A `document` collection keeps its records
//!   and entries as they were (version 1, no meta); the one `if` is at each call site.

use bson::Document;
use kimmy_core::merge_state::bridge::{document_to_fields, ordered_to_document};
use kimmy_core::merge_state::{
    JoinOptions, MStamp, MergeState, StampError, Writer, WriterId, Written,
};
use kimmy_core::{Error as CoreError, Hlc, Stamp};

use crate::error::{Result, StorageError};

/// What a field write leaves for the caller to store.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Image {
    /// The compact bytes of the new merge state: the record's and the entry's `meta`.
    pub meta: Vec<u8>,
    /// The document the state renders, in the state's field order. Empty when no life is alive
    /// (a delete): the record is then a tombstone.
    pub body: Vec<u8>,
    /// Whether a life is alive in the new state.
    pub alive: bool,
    /// The stamp the write used, when it made one. `None` for a write that changed nothing.
    pub stamp: Option<Stamp>,
}

/// The join options of a `field` collection's documents. The retention is the library default
/// until the source of R is decided (a replicated, per-collection value fixed at creation).
pub(crate) fn options() -> JoinOptions {
    JoinOptions::default()
}

/// The merge state a stored record carries. A `field` record without meta is not one this code
/// writes: it is refused as corrupt, not guessed.
pub(crate) fn load(meta: Option<&[u8]>) -> Result<MergeState> {
    let meta = meta.ok_or_else(|| {
        StorageError::Corrupt("a record of a field collection carries no merge state".into())
    })?;
    MergeState::decode_compact(meta)
        .map_err(|e| StorageError::Corrupt(format!("the merge state of a record: {e:?}")))
}

fn m_stamp(s: Stamp) -> MStamp {
    MStamp { hlc: s.hlc, writer: WriterId { node: s.node, epoch: s.epoch } }
}

fn refused(what: &str, e: impl std::fmt::Display) -> StorageError {
    StorageError::Core(CoreError::Unsupported(format!("{what}: {e}")))
}

fn stamp_error(e: StampError) -> StorageError {
    refused("the write's stamp", e)
}

/// Run one write against `prior` (the stored state, or `None` for a document never stored) and
/// render the result. `mint` is called once per stamp the write needs.
fn write(
    writer: WriterId,
    prior: Option<MergeState>,
    mint: &mut dyn FnMut() -> Stamp,
    body: impl FnOnce(
        &mut Writer,
        &mut dyn FnMut() -> MStamp,
    ) -> std::result::Result<Option<Written>, StampError>,
) -> Result<Option<Image>> {
    let opts = options();
    let mut w = Writer::from_state(writer, prior.unwrap_or_default(), opts.clone());
    let mut minted: Option<Stamp> = None;
    let written = body(&mut w, &mut || {
        let s = mint();
        minted = Some(s);
        m_stamp(s)
    })
    .map_err(stamp_error)?;
    let Some(_written) = written else { return Ok(None) };
    let rendered = w.state.ordered_document(&opts);
    let alive = rendered.is_some();
    let body = match rendered {
        Some(level) => bson::serialize_to_vec(
            &ordered_to_document(&level).map_err(|e| refused("the document", e))?,
        )?,
        None => Vec::new(),
    };
    Ok(Some(Image { meta: w.state.encode_compact(), body, alive, stamp: minted }))
}

/// A stamp for a dry run: strictly above everything `prior` has seen, so the writer accepts it,
/// and never minted by the engine. A replace is run once with it to learn what it would store
/// (the validator and the indexes need the post-image before the engine mints the real stamp,
/// ADR-148), and once more with the real one.
pub(crate) fn provisional(writer: WriterId, prior: &MergeState) -> Stamp {
    let top = prior.vv.values().copied().max().unwrap_or(Hlc::ZERO);
    Stamp::new(Hlc::new(top.wall_ms.saturating_add(1), 0), writer.node, writer.epoch)
}

/// An insert: a new life, over `prior` when the key holds a tombstone. `None` when a life is
/// alive already (the caller has checked the key; this is the writer's own refusal).
pub(crate) fn insert(
    writer: WriterId,
    prior: Option<MergeState>,
    doc: &Document,
    mint: &mut dyn FnMut() -> Stamp,
) -> Result<Option<Image>> {
    let fields = document_to_fields(doc).map_err(|e| refused("the document", e))?;
    write(writer, prior, mint, |w, stamp| w.insert_stamped(&fields, stamp))
}

/// A replace: the fields that changed are set, the ones that are gone are unset, as one write.
/// `None` when nothing would change: no stamp is minted and nothing is stored.
pub(crate) fn replace(
    writer: WriterId,
    prior: MergeState,
    doc: &Document,
    mint: &mut dyn FnMut() -> Stamp,
) -> Result<Option<Image>> {
    let fields = document_to_fields(doc).map_err(|e| refused("the document", e))?;
    write(writer, Some(prior), mint, |w, stamp| w.write_replace_stamped(&fields, stamp))
}

/// A delete: the state keeps the delete and its context, so the record is a tombstone that
/// still carries the state. `None` when no life is alive.
pub(crate) fn delete(
    writer: WriterId,
    prior: MergeState,
    mint: &mut dyn FnMut() -> Stamp,
) -> Result<Option<Image>> {
    write(writer, Some(prior), mint, |w, stamp| w.write_delete_stamped(stamp))
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use kimmy_core::{Epoch, Hlc, NodeId};

    use super::*;

    fn wid(n: u8) -> WriterId {
        WriterId { node: NodeId::from_bytes([n; 16]), epoch: Epoch::ZERO }
    }

    /// A clock for the test: each call hands out the next HLC for `writer`.
    struct Clock {
        writer: WriterId,
        at: u64,
        calls: usize,
    }

    impl Clock {
        fn new(writer: WriterId) -> Self {
            Self { writer, at: 100, calls: 0 }
        }
        fn mint(&mut self) -> Stamp {
            self.calls += 1;
            self.at += 10;
            Stamp::new(Hlc::new(self.at, 0), self.writer.node, self.writer.epoch)
        }
    }

    fn bytes(d: &Document) -> Vec<u8> {
        bson::serialize_to_vec(d).unwrap()
    }

    fn state_of(i: &Image) -> MergeState {
        MergeState::decode_compact(&i.meta).unwrap()
    }

    #[test]
    fn an_insert_renders_the_document_in_its_field_order_and_stamps_it() {
        let w = wid(1);
        let mut c = Clock::new(w);
        let d = doc! { "_id": 1, "z": 1, "a": { "y": 2, "b": 3 }, "m": [1, 2] };
        let img = insert(w, None, &d, &mut || c.mint()).unwrap().expect("an insert writes");
        assert_eq!(img.body, bytes(&d), "the body is the document, bytes and order");
        assert!(img.alive);
        assert_eq!(c.calls, 1, "one stamp for an insert");
        let stamp = img.stamp.expect("a stamp");
        let state = state_of(&img);
        assert_eq!(state.vv.values().copied().max(), Some(stamp.hlc), "record stamp == max(vv)");
        assert!(state.losses.is_empty(), "sequential writes of one member make no loss record");
    }

    #[test]
    fn a_replace_that_changes_nothing_mints_nothing_and_one_that_changes_makes_one_stamp() {
        let w = wid(1);
        let mut c = Clock::new(w);
        let d = doc! { "_id": 1, "a": 1, "b": "x" };
        let first = insert(w, None, &d, &mut || c.mint()).unwrap().unwrap();
        let calls = c.calls;
        assert!(
            replace(w, state_of(&first), &d, &mut || c.mint()).unwrap().is_none(),
            "the same document is no write"
        );
        assert_eq!(c.calls, calls, "no stamp was minted for it (ADR-208)");

        let d2 = doc! { "_id": 1, "a": 2, "c": true };
        let second = replace(w, state_of(&first), &d2, &mut || c.mint()).unwrap().unwrap();
        assert_eq!(c.calls, calls + 1, "one stamp for a replace");
        assert_eq!(second.body, bytes(&d2));
        let state = state_of(&second);
        assert_eq!(state.vv.values().copied().max(), second.stamp.map(|s| s.hlc));
        assert!(state.losses.is_empty());
    }

    #[test]
    fn a_delete_leaves_a_tombstone_that_keeps_the_state_and_an_insert_over_it_is_a_new_life() {
        let w = wid(1);
        let mut c = Clock::new(w);
        let d = doc! { "_id": 1, "a": 1 };
        let first = insert(w, None, &d, &mut || c.mint()).unwrap().unwrap();
        let gone = delete(w, state_of(&first), &mut || c.mint()).unwrap().unwrap();
        assert!(!gone.alive && gone.body.is_empty());
        assert!(!gone.meta.is_empty(), "the tombstone keeps the state");
        assert!(
            delete(w, state_of(&gone), &mut || c.mint()).unwrap().is_none(),
            "nothing to delete"
        );

        let d2 = doc! { "_id": 1, "b": 2 };
        let back = insert(w, Some(state_of(&gone)), &d2, &mut || c.mint()).unwrap().unwrap();
        assert_eq!(back.body, bytes(&d2));
        assert!(back.alive);
        let (s1, s2) = (state_of(&gone), state_of(&back));
        assert!(s2.lives.len() > s1.lives.len(), "a new life, the old one kept for the join");
        assert!(s2.losses.is_empty());
    }

    #[test]
    fn a_live_document_refuses_an_insert_and_a_field_record_without_meta_is_corrupt() {
        let w = wid(1);
        let mut c = Clock::new(w);
        let d = doc! { "_id": 1 };
        let first = insert(w, None, &d, &mut || c.mint()).unwrap().unwrap();
        let calls = c.calls;
        assert!(insert(w, Some(state_of(&first)), &d, &mut || c.mint()).unwrap().is_none());
        assert_eq!(c.calls, calls, "a refused insert mints nothing");
        assert!(matches!(load(None), Err(StorageError::Corrupt(_))));
        assert!(matches!(load(Some(b"not a state")), Err(StorageError::Corrupt(_))));
    }

    #[test]
    fn a_stamp_not_above_the_state_is_refused_and_writes_nothing() {
        let w = wid(1);
        let mut c = Clock::new(w);
        let first = insert(w, None, &doc! { "_id": 1, "a": 1 }, &mut || c.mint()).unwrap().unwrap();
        let mut low = || Stamp::new(Hlc::new(5, 0), w.node, w.epoch);
        let r = replace(w, state_of(&first), &doc! { "_id": 1, "a": 2 }, &mut low);
        assert!(r.is_err(), "{r:?}");
    }
}
