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
use kimmy_core::effects::{CellTag, CreationCell, Effects, Outcome, UpsertEffects};
use kimmy_core::merge_state::bridge::{bson_to_value, document_to_fields, ordered_to_document};
use kimmy_core::merge_state::{
    JoinOptions, MStamp, MergeState, Path, StampError, UpdateError, UpdateRefused, Writer,
    WriterId, Written,
};
use kimmy_core::{Error as CoreError, Hlc, Origin, Stamp};

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

/// The default join retention `R` of a `field` collection created on a member whose tombstone retention
/// is not configured: 24 hours, in milliseconds, the default of `storage.tombstone_retention_secs`.
pub(crate) const DEFAULT_JOIN_RETENTION_MS: u64 = 24 * 60 * 60 * 1000;

/// Who writes, and under which join retention `R`: the collection's, fixed at its creation, replicated with
/// it and immutable, so that every member computes the same horizon `H = max(vv) - R`. Never read from a
/// local setting after the creation.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FieldWriter {
    pub writer: WriterId,
    pub retention_ms: u64,
}

impl FieldWriter {
    /// The join options of the collection's documents: the library's, with the collection's `R`.
    pub(crate) fn options(&self) -> JoinOptions {
        JoinOptions::default().with_retention(self.retention_ms)
    }
}

/// A writer with the default retention, for the tests of this module and the ones that need a writer and
/// not a collection; production passes the collection's own.
#[cfg(test)]
impl From<WriterId> for FieldWriter {
    fn from(writer: WriterId) -> Self {
        Self { writer, retention_ms: JoinOptions::default().retention }
    }
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

/// The newest wall time any origin of an entry's merged state has written: what U1 holds the entry's state to
/// (`max(vv)`). `None` for an empty version vector.
pub(crate) fn vv_max_wall_ms(meta: &[u8]) -> Result<Option<u64>> {
    let state = MergeState::decode_compact(meta)
        .map_err(|e| StorageError::Corrupt(format!("the merge state of an entry: {e:?}")))?;
    Ok(state.vv.values().map(|hlc| hlc.wall_ms).max())
}

/// The holes a state applied as state (a page document, a carried delete) leaves: the newest write each
/// writer of its version vector made, by the writer's node and epoch. This member's own live origin is left
/// out: every write of its own epoch is in its own oplog until retention collects it, which has its own
/// path, and a hole over them would send its peers to a snapshot for writes it can serve. An older epoch of
/// its own node is not its live origin and is held like any other.
pub(crate) fn state_holes(vv: &kimmy_core::merge_state::Vv, own: Origin) -> Vec<(Origin, Hlc)> {
    vv.iter()
        .map(|(writer, hlc)| (Origin::new(writer.node, writer.epoch), *hlc))
        .filter(|(origin, _)| *origin != own)
        .collect()
}

fn m_stamp(s: Stamp) -> MStamp {
    MStamp { hlc: s.hlc, writer: WriterId { node: s.node, epoch: s.epoch } }
}

fn refused(what: &str, e: impl std::fmt::Display) -> StorageError {
    StorageError::Core(CoreError::Unsupported(format!("{what}: {e}")))
}

/// The writer's refusal of an update or a creation, with its own text, as the request's error
/// (a 400, like any invalid update).
fn update_refused(r: UpdateRefused) -> StorageError {
    StorageError::Core(CoreError::InvalidUpdate(r.to_string()))
}

/// A stamp the writer refused is a defect of the caller, not of the request.
fn stamp_error(e: StampError) -> StorageError {
    StorageError::Corrupt(format!("the write's stamp was refused: {e}"))
}

/// Run one write against `prior` (the stored state, or `None` for a document never stored) and
/// render the result. `mint` is called once per stamp the write needs.
fn write(
    writer: impl Into<FieldWriter>,
    prior: Option<MergeState>,
    mint: &mut dyn FnMut() -> Stamp,
    body: impl FnOnce(
        &mut Writer,
        &mut dyn FnMut() -> MStamp,
    ) -> std::result::Result<Option<Written>, StampError>,
) -> Result<Option<Image>> {
    let writer = writer.into();
    let opts = writer.options();
    let mut w = Writer::from_state(writer.writer, prior.unwrap_or_default(), opts.clone());
    let mut minted: Option<Stamp> = None;
    let written = body(&mut w, &mut || {
        let s = mint();
        minted = Some(s);
        m_stamp(s)
    })
    .map_err(stamp_error)?;
    let Some(_written) = written else {
        // `None` is a write that changes nothing, or one the writer refused for the cap.
        return if w.would_hit_writer_cap() {
            Err(update_refused(UpdateRefused::WriterCap))
        } else {
            Ok(None)
        };
    };
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

/// What joining a remote entry's state into the stored one leaves for the caller to store.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Joined {
    /// The compact bytes of the merged state: the record's `meta`.
    pub meta: Vec<u8>,
    /// The document the merged state renders; empty when no life is alive.
    pub body: Vec<u8>,
    /// Whether a life is alive in the merged state.
    pub alive: bool,
    /// Whether the merged state differs from the stored one. A join that does not change it is an
    /// entry the member already covers: nothing is rewritten.
    pub changed: bool,
}

/// The apply of one replicated entry of a `field` collection: the join of the state it carries
/// into `prior` (the stored state, or `None` for a document never held). Pure, and a semilattice
/// join: the same entries in any order, with repeats, give the same state. Loss records the join
/// reports are not kept here (records are converged by Step 4).
pub(crate) fn join(
    writer: impl Into<FieldWriter>,
    prior: Option<MergeState>,
    other: &MergeState,
) -> Result<Joined> {
    let writer = writer.into();
    let opts = writer.options();
    let mut w = Writer::from_state(writer.writer, prior.unwrap_or_default(), opts.clone());
    let received = w.receive_report(other);
    let rendered = w.state.ordered_document(&opts);
    let alive = rendered.is_some();
    let body = match rendered {
        Some(level) => bson::serialize_to_vec(
            &ordered_to_document(&level).map_err(|e| refused("the document", e))?,
        )?,
        None => Vec::new(),
    };
    Ok(Joined { meta: w.state.encode_compact(), body, alive, changed: received.changed })
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
    writer: impl Into<FieldWriter>,
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
    writer: impl Into<FieldWriter>,
    prior: MergeState,
    doc: &Document,
    mint: &mut dyn FnMut() -> Stamp,
) -> Result<Option<Image>> {
    let fields = document_to_fields(doc).map_err(|e| refused("the document", e))?;
    write(writer, Some(prior), mint, |w, stamp| w.write_replace_stamped(&fields, stamp))
}

/// An operator update as effects: the writer applies them as one write at one stamp, so a
/// concurrent write of another field, or another member's increment, is kept by the join. `None`
/// when nothing changes (no stamp is minted). A refusal of the writer is an error here; W6 maps it.
pub(crate) fn update(
    writer: impl Into<FieldWriter>,
    prior: MergeState,
    effects: &Effects,
    mint: &mut dyn FnMut() -> Stamp,
) -> Result<Option<Image>> {
    let mut refusal = None;
    let image =
        write(writer, Some(prior), mint, |w, stamp| match effects.apply_stamped(w, stamp) {
            Ok(Outcome::Modified(written)) => Ok(Some(*written)),
            Ok(Outcome::Unchanged) => Ok(None),
            Ok(Outcome::Refused(r)) | Err(UpdateError::Refused(r)) => {
                refusal = Some(r);
                Ok(None)
            }
            Err(UpdateError::Stamp(e)) => Err(e),
        })?;
    match refusal {
        Some(r) => Err(update_refused(r)),
        None => Ok(image),
    }
}

/// The path of every field of `doc`, in its order, a document-valued field before its members:
/// the order `UpsertEffects::creation_list` asks for.
fn post_image_order(doc: &Document) -> Vec<Path> {
    fn walk(d: &Document, at: &mut Vec<String>, out: &mut Vec<Path>) {
        for (name, value) in d {
            at.push(name.clone());
            if let Ok(p) = Path::new(at.iter().cloned()) {
                out.push(p);
            }
            if let bson::Bson::Document(inner) = value {
                walk(inner, at, out);
            }
            at.pop();
        }
    }
    let mut out = Vec::new();
    walk(doc, &mut Vec::new(), &mut out);
    out
}

/// The creation of an upsert that matched nothing, as the writer's typed creation entry: the
/// filter's equalities, `$setOnInsert`, `$set` and the increments (typed, as counters) in the
/// order of `doc`, the document the engine would insert, which names the `_id`. One record, one
/// entry: the stamp is the later of the two the writer takes (the creation and, beside creation
/// cells, the increment write).
pub(crate) fn upsert(
    writer: impl Into<FieldWriter>,
    prior: Option<MergeState>,
    fx: &UpsertEffects,
    doc: &Document,
    mint: &mut dyn FnMut() -> Stamp,
) -> Result<Option<Image>> {
    let order = post_image_order(doc);
    // The effects never name `_id` (it is the upsert's key, the engine's); the document's own
    // `_id` is the first cell of the creation, as it is the first field of an insert.
    let mut cells = fx.creation_list(&order).map_err(update_refused)?;
    if let Some(id) = doc.get("_id") {
        let value = bson_to_value(id).map_err(|e| refused("the _id", e))?;
        let path = Path::flat("_id").map_err(|e| refused("the _id", e))?;
        cells.insert(0, CreationCell { path, value, tag: CellTag::Plain });
    }
    let mut refusal = None;
    let image =
        write(writer, prior, mint, |w, stamp| match w.upsert_create_stamped(&cells, stamp) {
            Ok((created, _)) => Ok(Some(created)),
            Err(UpdateError::Refused(r)) => {
                refusal = Some(r);
                Ok(None)
            }
            Err(UpdateError::Stamp(e)) => Err(e),
        })?;
    match refusal {
        Some(r) => Err(update_refused(r)),
        None => Ok(image),
    }
}

/// A delete: the state keeps the delete and its context, so the record is a tombstone that
/// still carries the state. `None` when no life is alive.
pub(crate) fn delete(
    writer: impl Into<FieldWriter>,
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

    /// An older epoch of the receiver's own node is not its live origin: its writes are held like any other's.
    #[test]
    fn an_older_epoch_of_the_receivers_own_node_is_not_skipped() {
        let node = NodeId::from_bytes([3; 16]);
        let own = Origin::new(node, Epoch::from_u64(2));
        let older = Origin::new(node, Epoch::from_u64(1));
        let other = Origin::new(NodeId::from_bytes([4; 16]), Epoch::from_u64(1));
        let mut vv = kimmy_core::merge_state::Vv::new();
        for (origin, wall) in [(own, 30u64), (older, 20), (other, 10)] {
            vv.insert(
                kimmy_core::merge_state::WriterId { node: origin.node, epoch: origin.epoch },
                Hlc::new(wall, 0),
            );
        }

        let holes = state_holes(&vv, own);

        let origins: Vec<Origin> = holes.iter().map(|(origin, _)| *origin).collect();
        assert!(!origins.contains(&own), "the live origin is skipped: {origins:?}");
        assert!(origins.contains(&older), "an older epoch of the same node is not: {origins:?}");
        assert!(origins.contains(&other), "nor is another node: {origins:?}");
    }
}
