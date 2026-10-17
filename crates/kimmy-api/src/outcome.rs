//! What a document write answered, as plain data, and the one function per
//! route kind that turns it into the response body.
//!
//! **Why an outcome and not the response bytes** (B5). A keyed write keeps a
//! record of what it answered, so that a retry gets the first answer back. A
//! record that held the bytes would tie itself to the build that rendered
//! them: a later build that renders the same answer differently would replay
//! the old rendering, and a record that crosses a roll would replay another
//! release's. So the record holds the outcome, these structs are its shape at
//! the edge, and the function that renders a first answer is the function that
//! renders its replay. The same body goes to a REST client as JSON and to an
//! MCP client as the tool result's structured content, so both edges render
//! through here as well.
//!
//! Each `render` is pure: no state, no clock, no engine. An optional field
//! appears only when it is set, which is what keeps a write without a key
//! answering byte for byte as it did before keys existed: `stamp` when the
//! write produced one to name, and `skipped` and `resumed` only on a request
//! that resumed after it had stopped.

use bson::{Bson, Document};
use kimmy_core::Stamp;
use serde_json::{Value, json};

use crate::json::{bson_to_json, document_to_json};

/// `POST …/docs`: one document inserted.
#[derive(Clone, Debug, PartialEq)]
pub struct InsertOutcome {
    /// The `_id` as the answer reports it: the one the client sent, as sent,
    /// or else the one the server assigned.
    pub inserted_id: Bson,
    /// The version the insert produced.
    pub stamp: Stamp,
}

impl InsertOutcome {
    pub fn render(&self) -> Value {
        json!({
            "insertedId": bson_to_json(&self.inserted_id),
            "stamp": self.stamp.encode(),
        })
    }
}

/// `POST …/bulk`: a batch inserted in one commit. Positionally parallel, as
/// the answer is: the `i`th id was written at the `i`th stamp.
#[derive(Clone, Debug, PartialEq)]
pub struct BulkInsertOutcome {
    pub inserted_ids: Vec<Bson>,
    pub stamps: Vec<Stamp>,
}

impl BulkInsertOutcome {
    pub fn render(&self) -> Value {
        json!({
            "inserted": self.stamps.len(),
            "insertedIds": self.inserted_ids.iter().map(bson_to_json).collect::<Vec<_>>(),
            "stamps": self.stamps.iter().map(Stamp::encode).collect::<Vec<_>>(),
        })
    }
}

/// `PUT …/docs/{id}`: a replace by id, or its upsert.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplaceOutcome {
    pub matched: bool,
    /// `false` for a body identical to the stored one, which writes nothing
    /// (ADR-208), and for an upsert, which `upserted` says.
    pub modified: bool,
    pub upserted: bool,
    /// The version the write produced, when it wrote anything.
    pub stamp: Option<Stamp>,
}

impl ReplaceOutcome {
    /// Counts, not booleans, though a replace touches at most one document:
    /// `matched` is a count on `/update`, and one field name carrying two types
    /// on one protocol is what ADR-056 removed. `upserted` stays a boolean
    /// because it genuinely is one.
    pub fn render(&self) -> Value {
        let mut body = json!({
            "matched": u8::from(self.matched),
            "modified": u8::from(self.modified),
            "upserted": self.upserted,
        });
        if let Some(stamp) = self.stamp {
            body["stamp"] = json!(stamp.encode());
        }
        body
    }
}

/// `DELETE …/docs/{id}`: the tombstone's stamp, when there was a document to
/// delete. `deleted` is whether there was.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeleteByIdOutcome {
    pub stamp: Option<Stamp>,
}

impl DeleteByIdOutcome {
    pub fn render(&self) -> Value {
        let mut body = json!({ "deleted": u8::from(self.stamp.is_some()) });
        if let Some(stamp) = self.stamp {
            body["stamp"] = json!(stamp.encode());
        }
        body
    }
}

/// What a resumed keyed request adds to its counts (B5): the matches it left
/// because they changed after the request's first write, by this request on
/// another member or by another write. Cumulative across the request's
/// attempts, and counted here rather than in `matched`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Resumed {
    pub skipped: u64,
}

impl Resumed {
    /// `skipped` and `resumed: true`, after every other field, so a body
    /// without them is exactly the body of a request that never resumed.
    fn append(this: Option<&Self>, body: &mut Value) {
        if let Some(resumed) = this {
            body["skipped"] = json!(resumed.skipped);
            body["resumed"] = json!(true);
        }
    }
}

/// `POST …/update`, single or `multi: true`. The counts are cumulative over
/// the request's commits (ADR-086), and over its attempts once it resumes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UpdateOutcome {
    pub matched: u64,
    pub modified: u64,
    pub commits: u64,
    /// The version the single-document form wrote; never set for `multi`,
    /// which names no single version (ADR-084).
    pub stamp: Option<Stamp>,
    /// Set when an attempt of the request resumed (B5).
    pub resumed: Option<Resumed>,
}

impl UpdateOutcome {
    pub fn render(&self) -> Value {
        let mut body = json!({
            "matched": self.matched,
            "modified": self.modified,
            "commits": self.commits,
        });
        if let Some(stamp) = self.stamp {
            body["stamp"] = json!(stamp.encode());
        }
        Resumed::append(self.resumed.as_ref(), &mut body);
        body
    }
}

/// `POST …/delete`, single or `multi: true`. As [`UpdateOutcome`], with the
/// deleted count where an update has two.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeleteOutcome {
    pub deleted: u64,
    pub commits: u64,
    pub stamp: Option<Stamp>,
    pub resumed: Option<Resumed>,
}

impl DeleteOutcome {
    pub fn render(&self) -> Value {
        let mut body = json!({ "deleted": self.deleted, "commits": self.commits });
        if let Some(stamp) = self.stamp {
            body["stamp"] = json!(stamp.encode());
        }
        Resumed::append(self.resumed.as_ref(), &mut body);
        body
    }
}

/// `POST …/find_and_modify`, including its upsert.
#[derive(Clone, Debug, PartialEq)]
pub struct FindAndModifyOutcome {
    /// The image the caller asked for, already projected; `None` answers
    /// `null` (nothing matched, or the image asked for does not exist).
    pub document: Option<Document>,
    pub matched: bool,
    /// As `/update` counts it (ADR-208): an existing document changed or
    /// removed. Not a document the upsert created, which `upserted_id` says.
    pub modified: bool,
    /// The created document's own `_id`, as `insertedId` is.
    pub upserted_id: Option<Bson>,
    /// The version the write produced, only for a document that still exists:
    /// a removal's tombstone is nothing a conditional write can name.
    pub stamp: Option<Stamp>,
}

impl FindAndModifyOutcome {
    pub fn render(&self) -> Value {
        let mut body = json!({
            "document": self.document.as_ref().map_or(Value::Null, document_to_json),
            "matched": u64::from(self.matched),
            "modified": u64::from(self.modified),
            "upserted": self.upserted_id.is_some(),
        });
        if let Some(id) = &self.upserted_id {
            body["upsertedId"] = bson_to_json(id);
        }
        if let Some(stamp) = self.stamp {
            body["stamp"] = json!(stamp.encode());
        }
        body
    }
}

#[cfg(test)]
mod tests {
    //! **Golden**: each renderer against the response code it replaced.
    //!
    //! The `legacy_*` functions are the response sites of `exec.rs` as they
    //! stood at 0.51.0, copied with only their inputs turned into arguments, so
    //! the refactor is held to what it replaced rather than to a reading of it.
    //! Every fixture is compared as serialized bytes, key order included (the
    //! workspace builds `serde_json` with `preserve_order`, so the order a body
    //! is built in is the order it is sent in). A few literal bodies besides,
    //! so the shape is readable here and not only derivable.

    use super::*;
    use kimmy_core::{DocId, Hlc, NodeId};

    fn stamp(n: u64) -> Stamp {
        Stamp::new(Hlc::new(1_760_000_000_000 + n, (n % 3) as u16), NodeId::from_bytes([7; 16]))
    }

    fn bytes(value: &Value) -> String {
        serde_json::to_string(value).unwrap()
    }

    /// The `_id`s a write can report, every BSON shape `bson_to_json` treats
    /// differently among them.
    fn ids() -> Vec<Bson> {
        vec![
            Bson::ObjectId(bson::oid::ObjectId::from_bytes([1; 12])),
            Bson::String("6f1c0e52-8a1d-8f53-9b3e-2c1d6a7e4b90".into()),
            Bson::Int64(42),
            Bson::Int32(-7),
            Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Uuid,
                bytes: vec![9; 16],
            }),
            Bson::Document(bson::doc! { "a": 1, "b": "x" }),
        ]
    }

    // ---- the 0.51.0 response sites, verbatim but for their inputs ----------

    fn inserted_id(sent: Option<Bson>, id: &DocId) -> Bson {
        sent.unwrap_or_else(|| id.to_bson())
    }

    fn legacy_insert(sent: Option<Bson>, id: &DocId, stamp: Stamp) -> Value {
        json!({
            "insertedId": crate::json::bson_to_json(&inserted_id(sent, id)),
            "stamp": stamp.encode(),
        })
    }

    fn legacy_insert_many(stamped: &[(DocId, Stamp)], sent: Vec<Option<Bson>>) -> Value {
        json!({
            "inserted": stamped.len(),
            "insertedIds": stamped
                .iter()
                .zip(sent)
                .map(|((id, _), sent)| crate::json::bson_to_json(&inserted_id(sent, id)))
                .collect::<Vec<_>>(),
            "stamps": stamped.iter().map(|(_, stamp)| stamp.encode()).collect::<Vec<_>>(),
        })
    }

    fn legacy_replace(outcome: &kimmy_storage::WriteOutcome) -> Value {
        let mut body = json!({
            "matched": u8::from(outcome.matched),
            "modified": u8::from(outcome.modified),
            "upserted": outcome.upserted,
        });
        if let Some(stamp) = outcome.stamp {
            body["stamp"] = json!(stamp.encode());
        }
        body
    }

    fn legacy_delete_by_id(stamp: Option<Stamp>) -> Value {
        let mut body = json!({ "deleted": u8::from(stamp.is_some()) });
        if let Some(stamp) = stamp {
            body["stamp"] = json!(stamp.encode());
        }
        body
    }

    fn single_stamp(multi: bool, outcome: &kimmy_storage::ModifyManyOutcome) -> Option<Stamp> {
        (!multi && outcome.modified == 1).then_some(outcome.stamp).flatten()
    }

    fn legacy_update(multi: bool, outcome: &kimmy_storage::ModifyManyOutcome) -> Value {
        let mut body = json!({
            "matched": outcome.matched,
            "modified": outcome.modified,
            "commits": outcome.commits,
        });
        if let Some(stamp) = single_stamp(multi, outcome) {
            body["stamp"] = json!(stamp.encode());
        }
        body
    }

    fn legacy_delete(multi: bool, outcome: &kimmy_storage::ModifyManyOutcome) -> Value {
        let mut body = json!({ "deleted": outcome.modified, "commits": outcome.commits });
        if let Some(stamp) = single_stamp(multi, outcome) {
            body["stamp"] = json!(stamp.encode());
        }
        body
    }

    fn legacy_find_and_modify(
        outcome: &kimmy_storage::ModifyOutcome,
        after_wanted: bool,
        projection: Option<&kimmy_query::shape::Projection>,
    ) -> Value {
        let returned = if after_wanted { outcome.after.clone() } else { outcome.before.clone() };
        let document = match returned {
            Some(doc) => document_to_json(&kimmy_query::shape::project(projection, &doc)),
            None => Value::Null,
        };
        let mut body = json!({
            "document": document,
            "matched": u64::from(outcome.matched),
            "modified": u64::from(outcome.modified),
            "upserted": outcome.upserted.is_some(),
        });
        if let Some(id) = &outcome.upserted {
            let stored = outcome.after.as_ref().and_then(|doc| doc.get(kimmy_storage::ID_FIELD));
            body["upsertedId"] =
                crate::json::bson_to_json(&stored.map_or_else(|| id.to_bson(), Clone::clone));
        }
        if let Some(stamp) = outcome.stamp
            && outcome.after.is_some()
        {
            body["stamp"] = json!(stamp.encode());
        }
        body
    }

    // ---- the renderers against them ----------------------------------------

    #[test]
    fn insert_renders_what_the_insert_route_answered() {
        let assigned = DocId::ObjectId(bson::oid::ObjectId::from_bytes([3; 12]));
        for (i, sent) in ids().into_iter().map(Some).chain([None]).enumerate() {
            let at = stamp(i as u64);
            let outcome =
                InsertOutcome { inserted_id: inserted_id(sent.clone(), &assigned), stamp: at };
            assert_eq!(bytes(&outcome.render()), bytes(&legacy_insert(sent, &assigned, at)));
        }
        let literal = InsertOutcome { inserted_id: Bson::Int64(42), stamp: stamp(1) };
        assert_eq!(
            bytes(&literal.render()),
            format!(r#"{{"insertedId":42,"stamp":"{}"}}"#, stamp(1).encode())
        );
    }

    #[test]
    fn a_bulk_insert_renders_what_the_bulk_route_answered() {
        for n in [0usize, 1, 6] {
            let stamped: Vec<(DocId, Stamp)> =
                (0..n).map(|i| (DocId::Int64(i as i64), stamp(i as u64))).collect();
            // Some positions sent their own `_id`, some did not.
            let sent: Vec<Option<Bson>> = ids()
                .into_iter()
                .take(n)
                .enumerate()
                .map(|(i, id)| (i % 2 == 0).then_some(id))
                .collect();
            let outcome = BulkInsertOutcome {
                inserted_ids: stamped
                    .iter()
                    .zip(sent.clone())
                    .map(|((id, _), sent)| inserted_id(sent, id))
                    .collect(),
                stamps: stamped.iter().map(|(_, s)| *s).collect(),
            };
            assert_eq!(bytes(&outcome.render()), bytes(&legacy_insert_many(&stamped, sent)), "{n}");
        }
        let empty = BulkInsertOutcome { inserted_ids: Vec::new(), stamps: Vec::new() };
        assert_eq!(bytes(&empty.render()), r#"{"inserted":0,"insertedIds":[],"stamps":[]}"#);
    }

    #[test]
    fn replace_renders_what_the_replace_route_answered() {
        for matched in [false, true] {
            for modified in [false, true] {
                for upserted in [false, true] {
                    for at in [None, Some(stamp(5))] {
                        let storage =
                            kimmy_storage::WriteOutcome { matched, modified, upserted, stamp: at };
                        let outcome = ReplaceOutcome { matched, modified, upserted, stamp: at };
                        assert_eq!(bytes(&outcome.render()), bytes(&legacy_replace(&storage)));
                    }
                }
            }
        }
        let unchanged =
            ReplaceOutcome { matched: true, modified: false, upserted: false, stamp: None };
        assert_eq!(bytes(&unchanged.render()), r#"{"matched":1,"modified":0,"upserted":false}"#);
    }

    #[test]
    fn delete_by_id_renders_what_the_delete_by_id_route_answered() {
        for at in [None, Some(stamp(9))] {
            let outcome = DeleteByIdOutcome { stamp: at };
            assert_eq!(bytes(&outcome.render()), bytes(&legacy_delete_by_id(at)));
        }
        assert_eq!(bytes(&DeleteByIdOutcome { stamp: None }.render()), r#"{"deleted":0}"#);
    }

    fn modify_many(
        matched: u64,
        modified: u64,
        commits: u64,
        at: Option<Stamp>,
    ) -> kimmy_storage::ModifyManyOutcome {
        kimmy_storage::ModifyManyOutcome {
            matched,
            modified,
            commits,
            stamp: at,
            ..Default::default()
        }
    }

    #[test]
    fn update_and_delete_render_what_their_routes_answered() {
        for multi in [false, true] {
            for (matched, modified, commits) in [(0, 0, 0), (1, 0, 0), (1, 1, 1), (2500, 2400, 3)] {
                for at in [None, Some(stamp(4))] {
                    let storage = modify_many(matched, modified, commits, at);
                    let update = UpdateOutcome {
                        matched,
                        modified,
                        commits,
                        stamp: single_stamp(multi, &storage),
                        resumed: None,
                    };
                    assert_eq!(bytes(&update.render()), bytes(&legacy_update(multi, &storage)));
                    let delete = DeleteOutcome {
                        deleted: modified,
                        commits,
                        stamp: single_stamp(multi, &storage),
                        resumed: None,
                    };
                    assert_eq!(bytes(&delete.render()), bytes(&legacy_delete(multi, &storage)));
                }
            }
        }
    }

    #[test]
    fn a_resumed_request_adds_skipped_and_resumed_after_everything_else() {
        let update = UpdateOutcome {
            matched: 7,
            modified: 6,
            commits: 3,
            stamp: None,
            resumed: Some(Resumed { skipped: 2 }),
        };
        assert_eq!(
            bytes(&update.render()),
            r#"{"matched":7,"modified":6,"commits":3,"skipped":2,"resumed":true}"#
        );
        // Zero skipped still says it resumed: the field is the answer's, not a
        // count that happens to be nonzero.
        let delete = DeleteOutcome {
            deleted: 4,
            commits: 2,
            stamp: Some(stamp(1)),
            resumed: Some(Resumed { skipped: 0 }),
        };
        assert_eq!(
            bytes(&delete.render()),
            format!(
                r#"{{"deleted":4,"commits":2,"stamp":"{}","skipped":0,"resumed":true}}"#,
                stamp(1).encode()
            )
        );
        // And without it, neither field.
        let plain = UpdateOutcome { matched: 7, modified: 6, commits: 3, ..Default::default() };
        let text = bytes(&plain.render());
        assert!(!text.contains("skipped") && !text.contains("resumed"), "{text}");
    }

    #[test]
    fn find_and_modify_renders_what_its_route_answered() {
        let before = bson::doc! { "_id": 1i64, "n": 1, "tag": "a" };
        let after = bson::doc! { "_id": 1i64, "n": 2, "tag": "a" };
        let created = bson::doc! { "_id": "derived-6f1c0e52", "n": 1 };
        let projection = kimmy_query::shape::parse_projection(&bson::doc! { "n": 1 }).unwrap();
        // Every shape the route answers: a change, an unchanged match, a removal,
        // a miss, an upsert (its id from the stored image, or from the DocId when
        // the image is not there), with and without a projection.
        let cases = [
            kimmy_storage::ModifyOutcome {
                before: Some(before.clone()),
                after: Some(after.clone()),
                matched: true,
                modified: true,
                upserted: None,
                stamp: Some(stamp(2)),
            },
            kimmy_storage::ModifyOutcome {
                before: Some(before.clone()),
                after: Some(before.clone()),
                matched: true,
                modified: false,
                upserted: None,
                stamp: None,
            },
            kimmy_storage::ModifyOutcome {
                before: Some(before.clone()),
                after: None,
                matched: true,
                modified: true,
                upserted: None,
                stamp: Some(stamp(3)),
            },
            kimmy_storage::ModifyOutcome {
                before: None,
                after: None,
                matched: false,
                modified: false,
                upserted: None,
                stamp: None,
            },
            kimmy_storage::ModifyOutcome {
                before: None,
                after: Some(created.clone()),
                matched: false,
                modified: false,
                upserted: Some(DocId::String("derived-6f1c0e52".into())),
                stamp: Some(stamp(4)),
            },
            kimmy_storage::ModifyOutcome {
                before: None,
                after: None,
                matched: false,
                modified: false,
                upserted: Some(DocId::Int64(5)),
                stamp: Some(stamp(5)),
            },
        ];
        for storage in &cases {
            for after_wanted in [false, true] {
                for projection in [None, projection.as_ref()] {
                    let returned =
                        if after_wanted { storage.after.as_ref() } else { storage.before.as_ref() };
                    let outcome = FindAndModifyOutcome {
                        document: returned.map(|doc| kimmy_query::shape::project(projection, doc)),
                        matched: storage.matched,
                        modified: storage.modified,
                        upserted_id: storage.upserted.as_ref().map(|id| {
                            storage
                                .after
                                .as_ref()
                                .and_then(|doc| doc.get(kimmy_storage::ID_FIELD))
                                .map_or_else(|| id.to_bson(), Clone::clone)
                        }),
                        stamp: storage.stamp.filter(|_| storage.after.is_some()),
                    };
                    assert_eq!(
                        bytes(&outcome.render()),
                        bytes(&legacy_find_and_modify(storage, after_wanted, projection)),
                        "{storage:?} after={after_wanted} projected={}",
                        projection.is_some()
                    );
                }
            }
        }
        let miss = FindAndModifyOutcome {
            document: None,
            matched: false,
            modified: false,
            upserted_id: None,
            stamp: None,
        };
        assert_eq!(
            bytes(&miss.render()),
            r#"{"document":null,"matched":0,"modified":0,"upserted":false}"#
        );
    }
}
