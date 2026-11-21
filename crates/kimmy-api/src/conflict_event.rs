//! The `conflictResolved` change-stream event (Q7 Step 4, C8).
//!
//! A member tells a client of a loss record once, by rendering the loss entry of its OWN that it appended in the transaction in which the
//! record entered its state (the storage stream passes no other loss entry). The event names the lost write and what won it:
//!
//! ```json
//! { "operationType": "conflictResolved", "eventId": "cr1-…", "documentKey": {"_id": 1}, "path": "a.b", "kind": "clash",
//!   "loser": {"stamp": "…", "value": 3, "truncated": false}, "winner": {"stamp": "…", "value": 4},
//!   "resumeToken": "…", "clusterTime": "…" }
//! ```
//!
//! * `path` is `null` for a record about the whole document (`insertLost`), and then `winner` is `{stamp of the standing life, value: null}`:
//!   read the document.
//! * `winner` is read **at delivery** from the document's stored state, so a replayed event can show another winner than its first
//!   delivery, a record that is stale beside a live register can show the loser itself, and a deleted document has `winner: null`.
//! * `eventId` is the identity of this report: the same on every member and in every process and build, so a consumer that reads
//!   several members' streams (or one member's after a record re-entered) discards what it has. It is not the identity of the lost
//!   write: that is `(documentKey, path, loser.stamp)`, and one write can carry two kinds, so two ids.

use kimmy_core::merge_state::bridge::value_to_bson;
use kimmy_core::merge_state::{LossAt, LossKey, LossValue, MStamp, Num, Value as MValue};
use kimmy_core::{CollectionId, DocId, OplogEntry, ResumeToken};
use kimmy_storage::{ConflictWinner, Engine};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// What every `eventId` of a `conflictResolved` starts with. A webhook's `eventId` of a document event is a stamp (digits, dashes and
/// hex), so the two cannot be taken for one another.
pub const CONFLICT_EVENT_ID_PREFIX: &str = "cr1-";

/// The domain the digest is taken in; changing the encoding below changes this and the prefix.
const DOMAIN: &[u8] = b"kimmydb.conflictResolved.v1\0";

fn put_len_prefixed(h: &mut Sha256, bytes: &[u8]) {
    h.update((bytes.len() as u32).to_be_bytes());
    h.update(bytes);
}

/// The `eventId` of the report of the loss record `key` of document `id` of `collection`: SHA-256, never a seeded hasher, over a canonical
/// encoding, as lowercase hex after [`CONFLICT_EVENT_ID_PREFIX`].
///
/// The encoding, in order: the domain; the collection id (8 bytes, big endian); the document key (the BSON bytes of `{_id: <id>}`, so
/// the type of the `_id` is part of it, with a 4-byte length); the place (`0` for the whole document, or `1` then the segment count
/// (4 bytes) and each segment with a 4-byte length); the kind (one byte, `LossKind::index`); the loser's stamp (its 10-byte hybrid time,
/// its node's 16 bytes, its epoch's 18).
pub fn conflict_event_id(collection: CollectionId, id: &DocId, key: &LossKey) -> String {
    let document_key = bson::serialize_to_vec(&bson::doc! { "_id": id.to_bson() })
        .expect("a document key is a document");
    let mut h = Sha256::new();
    h.update(DOMAIN);
    h.update(collection.0.to_be_bytes());
    put_len_prefixed(&mut h, &document_key);
    match &key.at {
        LossAt::Document => h.update([0u8]),
        LossAt::Field(path) => {
            h.update([1u8]);
            h.update((path.segments().len() as u32).to_be_bytes());
            for segment in path.segments() {
                put_len_prefixed(&mut h, segment.as_bytes());
            }
        }
    }
    h.update([key.kind.index() as u8]);
    h.update(key.stamp.hlc.to_bytes());
    h.update(key.stamp.writer.node.to_bytes());
    h.update(key.stamp.writer.epoch.0);
    let mut out = String::with_capacity(CONFLICT_EVENT_ID_PREFIX.len() + 64);
    out.push_str(CONFLICT_EVENT_ID_PREFIX);
    for byte in h.finalize() {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// A stamp as a client reads it, in the form the webhooks' `eventId` of a document event uses: `hlc-node-epoch`.
fn stamp_text(stamp: &MStamp) -> String {
    format!("{}-{}-{}", stamp.hlc, stamp.writer.node, stamp.writer.epoch.to_hex())
}

/// A merge-state value as JSON (`Unset`, which has no BSON form, is `null`).
fn value_json(value: &MValue) -> Value {
    match value_to_bson(value) {
        Ok(bson) => crate::json::bson_to_json(&bson),
        Err(_) => Value::Null,
    }
}

fn path_text(at: &LossAt) -> Value {
    match at {
        LossAt::Document => Value::Null,
        LossAt::Field(path) => Value::String(path.to_string()),
    }
}

/// The lost value as JSON.
fn loser_value_json(value: &LossValue) -> Value {
    match value {
        LossValue::Value(v) | LossValue::CutValue(v) => value_json(v),
        LossValue::Total(Num::Decimal128(bytes)) => {
            crate::json::bson_to_json(&bson::Bson::Decimal128(bson::Decimal128::from_bytes(*bytes)))
        }
        LossValue::Total(n) => match n.int() {
            Some(total) => {
                i64::try_from(total).map_or_else(|_| Value::String(total.to_string()), |t| json!(t))
            }
            None => Value::Null,
        },
        LossValue::Fields(paths) | LossValue::CutFields(paths) => {
            Value::Array(paths.iter().map(|p| Value::String(p.to_string())).collect())
        }
    }
}

/// The winner with its value left out: its stamp stays, so a receiver still knows which write won.
fn winner_json_without_value(winner: Option<ConflictWinner>) -> Value {
    match winner {
        None => Value::Null,
        Some(winner) => json!({
            "stamp": stamp_text(&winner.stamp),
            "valueOmitted": true,
            "omittedReason": "value exceeds webhooks.max_payload_bytes; read the cell from the collection",
        }),
    }
}

fn winner_json(winner: Option<ConflictWinner>) -> Value {
    match winner {
        None => Value::Null,
        Some(winner) => json!({
            "stamp": stamp_text(&winner.stamp),
            "value": winner.value.as_ref().map_or(Value::Null, value_json),
        }),
    }
}

/// The event for `entry`, a loss entry, or `None` when it is not one this member can tell: its body or its document is missing or does
/// not decode. The winner is read now.
pub fn render(engine: &Engine, entry: &OplogEntry, token: &ResumeToken) -> Option<Value> {
    if !is_renderable(entry) {
        return None;
    }
    let id = entry.doc_id.as_ref()?;
    let (key, _) =
        kimmy_core::merge_state::MergeState::decode_loss_record(entry.body.as_deref()?).ok()?;
    let winner = kimmy_storage::blocking(|| engine.conflict_winner(entry.collection, id, &key.at))
        .ok()
        .flatten();
    let mut event = body(entry, winner, false)?;
    event["resumeToken"] = json!(token.encode());
    Some(event)
}

/// What a loss entry tells, without a resume token: the fields a change stream and a webhook share. `winner` is what the caller read at its
/// delivery. With `omit_winner_value` the winner's value is left out and says so (a webhook event that would not fit its payload cap,
/// where an event is never dropped for its size): `{"stamp": …, "valueOmitted": true, "omittedReason": …}`.
pub fn body(
    entry: &OplogEntry,
    winner: Option<ConflictWinner>,
    omit_winner_value: bool,
) -> Option<Value> {
    if !is_renderable(entry) {
        return None;
    }
    let id = entry.doc_id.as_ref()?;
    let (key, value) =
        kimmy_core::merge_state::MergeState::decode_loss_record(entry.body.as_deref()?).ok()?;
    let winner =
        if omit_winner_value { winner_json_without_value(winner) } else { winner_json(winner) };
    Some(json!({
        "operationType": "conflictResolved",
        "eventId": conflict_event_id(entry.collection, id, &key),
        "clusterTime": entry.stamp.hlc.to_string(),
        "documentKey": { "_id": crate::json::bson_to_json(&id.to_bson()) },
        "path": path_text(&key.at),
        "kind": key.kind.as_str(),
        "loser": {
            "stamp": stamp_text(&key.stamp),
            "value": loser_value_json(&value),
            "truncated": value.is_cut(),
        },
        "winner": winner,
    }))
}

/// Whether `entry` is a loss entry [`body`] can render: a document key and a body that decodes.
pub fn is_renderable(entry: &OplogEntry) -> bool {
    entry.kind.is_loss_record()
        && entry.doc_id.is_some()
        && entry
            .body
            .as_deref()
            .is_some_and(|b| kimmy_core::merge_state::MergeState::decode_loss_record(b).is_ok())
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use kimmy_core::merge_state::{LossKind, WriterId};
    use kimmy_core::{Epoch, Hlc, MergePolicy, NodeId, OpKind, Stamp};

    use super::*;

    fn writer() -> WriterId {
        WriterId { node: NodeId::from_bytes([0x11; 16]), epoch: Epoch([0x22; 18]) }
    }

    fn key(kind: LossKind, at: LossAt) -> LossKey {
        LossKey {
            kind,
            at,
            stamp: MStamp { hlc: Hlc::new(1_795_000_000_000, 3), writer: writer() },
        }
    }

    fn a_b() -> LossAt {
        LossAt::Field(kimmy_core::merge_state::Path::new(["a", "b"]).unwrap())
    }

    /// The format is a promise to every consumer that deduplicates on it: these strings were computed by an independent implementation
    /// (SHA-256 over the encoding in `conflict_event_id`'s doc, in Python) and must never change without a new version prefix.
    #[test]
    fn the_event_id_is_a_fixed_function_of_what_it_names() {
        let id = DocId::Int64(1);
        assert_eq!(
            conflict_event_id(CollectionId(7), &id, &key(LossKind::Clash, a_b())),
            "cr1-77950a6bcaad00ec377c161566cd2d6c65c96c265e6764de47cc75b43dd0c27b",
            "the id of a clash at a.b of document 1"
        );
        assert_eq!(
            conflict_event_id(CollectionId(7), &id, &key(LossKind::InsertLost, a_b())),
            "cr1-086d45fc757c9029a1987d2d7bd5a73131aad0697539b439e089cc361eec3e2e",
            "one write, another kind: another id"
        );
        assert_eq!(
            conflict_event_id(CollectionId(7), &id, &key(LossKind::InsertLost, LossAt::Document)),
            "cr1-6fe661f22c9493ac541e0e5a4cbc921aeadf6cc66b44946c95c02b055b363d4f",
            "the whole document is no field"
        );
    }

    #[test]
    fn every_part_of_what_an_event_id_names_changes_it_and_the_id_is_no_stamp() {
        let base = key(LossKind::Clash, a_b());
        let id = conflict_event_id(CollectionId(1), &DocId::Int64(1), &base);
        assert_ne!(
            id,
            conflict_event_id(CollectionId(2), &DocId::Int64(1), &base),
            "the collection"
        );
        assert_ne!(
            id,
            conflict_event_id(CollectionId(1), &DocId::Int64(2), &base),
            "the document key is part of the id"
        );
        assert_ne!(
            id,
            conflict_event_id(CollectionId(1), &DocId::String("1".into()), &base),
            "the type of the _id is part of the key"
        );
        let mut later = base.clone();
        later.stamp.hlc = Hlc::new(1_795_000_000_000, 4);
        assert_ne!(
            id,
            conflict_event_id(CollectionId(1), &DocId::Int64(1), &later),
            "the loser's stamp"
        );
        let two = key(
            LossKind::Clash,
            LossAt::Field(kimmy_core::merge_state::Path::new(["a", "b"]).unwrap()),
        );
        let one = key(
            LossKind::Clash,
            LossAt::Field(kimmy_core::merge_state::Path::new(["a.b"]).unwrap()),
        );
        assert_ne!(
            conflict_event_id(CollectionId(1), &DocId::Int64(1), &two),
            conflict_event_id(CollectionId(1), &DocId::Int64(1), &one),
            "two segments are not one segment that prints alike"
        );
        assert!(id.starts_with(CONFLICT_EVENT_ID_PREFIX) && id.len() == 4 + 64, "{id}");
        // A webhook's eventId of a document event is `hlc-node-epoch`: digits, then dashes inside it.
        assert!(id.matches('-').count() == 1, "one dash, after the prefix: {id}");
    }

    fn engine() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
    }

    fn entry(
        engine: &Engine,
        collection: CollectionId,
        id: i64,
        key: &LossKey,
        value: &LossValue,
    ) -> OplogEntry {
        OplogEntry {
            stamp: Stamp::new(
                Hlc::new(kimmy_storage::physical_now_ms() + 1, 0),
                engine.node_id(),
                engine.epoch(),
            ),
            kind: OpKind::LossRecord,
            collection,
            doc_id: Some(DocId::Int64(id)),
            body: Some(kimmy_core::merge_state::MergeState::encode_loss_record(key, value)),
            meta: None,
        }
    }

    fn token(entry: &OplogEntry) -> ResumeToken {
        ResumeToken::from_stamp(entry.stamp)
    }

    fn field(name: &str) -> LossAt {
        LossAt::Field(kimmy_core::merge_state::Path::flat(name).unwrap())
    }

    #[test]
    fn an_event_names_the_lost_write_and_the_winner_now() {
        let (e, _d) = engine();
        let coll =
            e.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
        e.insert(&coll, doc! { "_id": 1, "x": 4 }).unwrap();
        let winner = e
            .conflict_winner(coll.id, &DocId::Int64(1), &field("x"))
            .unwrap()
            .expect("a cell stands at x");
        let loser = MStamp { hlc: Hlc::new(1, 0), writer: writer() };
        let record = LossKey { kind: LossKind::Clash, at: field("x"), stamp: loser };
        let entry = entry(&e, coll.id, 1, &record, &LossValue::Value(MValue::Int64(3)));

        let event = render(&e, &entry, &token(&entry)).expect("an event");

        assert_eq!(event["operationType"], "conflictResolved", "{event}");
        assert_eq!(event["documentKey"]["_id"], 1, "{event}");
        assert_eq!(event["path"], "x", "{event}");
        assert_eq!(event["kind"], "clash", "{event}");
        assert_eq!(event["loser"]["stamp"], stamp_text(&loser), "{event}");
        assert_eq!(event["loser"]["value"], 3, "{event}");
        assert_eq!(event["loser"]["truncated"], false, "{event}");
        assert_eq!(
            event["winner"]["stamp"],
            stamp_text(&winner.stamp),
            "the winner is the cell now at the path: {event}"
        );
        assert_eq!(event["winner"]["value"], 4, "{event}");
        assert_eq!(
            event["eventId"],
            conflict_event_id(coll.id, &DocId::Int64(1), &record),
            "{event}"
        );
        assert!(event["resumeToken"].is_string() && event["clusterTime"].is_string(), "{event}");
    }

    #[test]
    fn a_cut_value_says_truncated_and_a_whole_document_record_has_no_path_and_no_winner_value() {
        let (e, _d) = engine();
        let coll =
            e.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
        e.insert(&coll, doc! { "_id": 1, "x": 4 }).unwrap();
        let loser = MStamp { hlc: Hlc::new(1, 0), writer: writer() };

        let cut = LossKey { kind: LossKind::Clash, at: field("x"), stamp: loser };
        let cut_entry =
            entry(&e, coll.id, 1, &cut, &LossValue::CutValue(MValue::String("abc".into())));
        let event = render(&e, &cut_entry, &token(&cut_entry)).unwrap();
        assert_eq!(
            (event["loser"]["value"].clone(), event["loser"]["truncated"].clone()),
            (json!("abc"), json!(true)),
            "a cut value is told as truncated"
        );

        let whole = LossKey { kind: LossKind::InsertLost, at: LossAt::Document, stamp: loser };
        let whole_entry = entry(
            &e,
            coll.id,
            1,
            &whole,
            &LossValue::Fields(vec![kimmy_core::merge_state::Path::flat("x").unwrap()]),
        );
        let event = render(&e, &whole_entry, &token(&whole_entry)).unwrap();
        assert_eq!(event["path"], Value::Null, "{event}");
        assert_eq!(event["kind"], "insertLost", "{event}");
        assert_eq!(event["loser"]["value"], json!(["x"]), "{event}");
        assert!(
            event["winner"]["stamp"].is_string() && event["winner"]["value"].is_null(),
            "the standing life, no value: {event}"
        );
    }

    /// Design 7.2: a record can name a write that is alive; the event is not suppressed, and its winner can be the loser itself.
    #[test]
    fn a_record_beside_a_live_register_is_told_and_its_winner_can_be_the_loser() {
        let (e, _d) = engine();
        let coll =
            e.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
        e.insert(&coll, doc! { "_id": 1, "x": 4 }).unwrap();
        let live = e.conflict_winner(coll.id, &DocId::Int64(1), &field("x")).unwrap().unwrap();
        let record = LossKey { kind: LossKind::InsertLost, at: field("x"), stamp: live.stamp };
        let entry = entry(&e, coll.id, 1, &record, &LossValue::Value(MValue::Int64(4)));

        let event = render(&e, &entry, &token(&entry)).expect("not suppressed");

        assert_eq!(
            event["loser"]["stamp"], event["winner"]["stamp"],
            "the winner at delivery is the write the record names: {event}"
        );
    }

    #[test]
    fn a_deleted_document_has_no_winner_and_what_is_no_loss_entry_or_does_not_decode_is_no_event() {
        let (e, _d) = engine();
        let coll =
            e.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
        e.insert(&coll, doc! { "_id": 1, "x": 4 }).unwrap();
        assert!(e.delete(&coll, &DocId::Int64(1)).unwrap());
        let record = LossKey {
            kind: LossKind::DeleteWon,
            at: field("x"),
            stamp: MStamp { hlc: Hlc::new(1, 0), writer: writer() },
        };
        let entry_ = entry(&e, coll.id, 1, &record, &LossValue::Value(MValue::Int64(4)));
        let event = render(&e, &entry_, &token(&entry_)).unwrap();
        assert_eq!(event["winner"], Value::Null, "{event}");

        let mut document_change = entry_.clone();
        document_change.kind = OpKind::Insert;
        assert!(
            render(&e, &document_change, &token(&document_change)).is_none(),
            "not a loss entry"
        );
        let mut bad = entry_.clone();
        bad.body = Some(vec![1, 2, 3]);
        assert!(render(&e, &bad, &token(&bad)).is_none(), "a body that does not decode");
        let mut nameless = entry_;
        nameless.doc_id = None;
        assert!(render(&e, &nameless, &token(&nameless)).is_none(), "no document named");
    }

    /// Seed 232 (`tests/merge_state_diff_nested.rs`): one write, `a.b 3:A:0`, is a `clash` in the route that met its concurrent winner
    /// first and an `insertLost` in the route that met the news of its life's supersession first. The two members tell it as two events
    /// about one lost write: the same documentKey, path and loser stamp, two kinds, two eventIds.
    #[test]
    fn seed_232_one_lost_write_is_two_events_with_two_ids_on_two_routes() {
        let (e, _d) = engine();
        let coll =
            e.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
        let write = MStamp { hlc: Hlc::new(3, 0), writer: writer() };
        let route_one = LossKey { kind: LossKind::Clash, at: a_b(), stamp: write };
        let route_two = LossKey { kind: LossKind::InsertLost, at: a_b(), stamp: write };
        let value = LossValue::Value(MValue::Int64(3));
        let first = entry(&e, coll.id, 1, &route_one, &value);
        let second = entry(&e, coll.id, 1, &route_two, &value);

        let one = render(&e, &first, &token(&first)).unwrap();
        let two = render(&e, &second, &token(&second)).unwrap();

        for same in ["documentKey", "path"] {
            assert_eq!(one[same], two[same], "{same}");
        }
        assert_eq!(one["loser"]["stamp"], two["loser"]["stamp"], "the lost write is the same");
        assert_eq!(
            (one["kind"].clone(), two["kind"].clone()),
            (json!("clash"), json!("insertLost"))
        );
        assert_ne!(one["eventId"], two["eventId"], "two kinds, two ids");
        // Each route counts its own kind: the labels the metric counts them under are two.
        assert_ne!(route_one.kind.index(), route_two.kind.index());
    }
}
