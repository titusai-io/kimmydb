//! The update operators through a digit segment over elements that hold that digit as a
//! field name (q7s3j.jsonl cases 1-150, reference 6.0.5): a digit segment over an array is a
//! POSITION for the read as for the write, so an operator computes from the place it writes.
//! One row per recorded case; the case number is the line of q7s3j.jsonl.
//!
//! The date a `$currentDate` writes is the moment of the request, so those rows (141-150) are
//! checked for what the update does, not for the date.

use bson::{Bson, Document};

use crate::update::{apply, parse};

const NOW: i64 = 1_700_000_000_000;

enum Want {
    After(Document),
    /// Applies; the value written depends on the clock.
    Applies,
    /// Refused, the reference's code.
    Refused(i32),
}

fn doc_(pairs: Vec<(&str, Bson)>) -> Document {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}
fn d(pairs: Vec<(&str, Bson)>) -> Bson {
    Bson::Document(doc_(pairs))
}
fn a(items: Vec<Bson>) -> Bson {
    Bson::Array(items)
}
fn i(n: i32) -> Bson {
    Bson::Int32(n)
}
fn n() -> Bson {
    Bson::Null
}
fn s(text: &str) -> Bson {
    Bson::String(text.to_string())
}

#[rustfmt::skip]
fn rows() -> Vec<(u32, Document, Document, Want)> {
    vec![
        (1, doc_(vec![("$set", d(vec![("a.3", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))]), n(), n(), i(9)]))]))),
        (2, doc_(vec![("$set", d(vec![("a.1", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))]), i(9)]))]))),
        (3, doc_(vec![("$set", d(vec![("a.1", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]))),
        (4, doc_(vec![("$set", d(vec![("a.0.1", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))])), ("1", i(9))])]))]))),
        (5, doc_(vec![("$set", d(vec![("a.1", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), d(vec![("1", i(6))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]))),
        (6, doc_(vec![("$set", d(vec![("a.1", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![i(7), d(vec![("1", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![i(7), i(9)]))]))),
        (7, doc_(vec![("$set", d(vec![("a.0", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![i(9)]))]))),
        (8, doc_(vec![("$set", d(vec![("a.2", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(6)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(9)]))]))),
        (9, doc_(vec![("$set", d(vec![("a.1", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]))),
        (10, doc_(vec![("$set", d(vec![("a.0.1", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))]), i(9)])]))]))),
        (11, doc_(vec![("$unset", d(vec![("a.3", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]))),
        (12, doc_(vec![("$unset", d(vec![("a.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]))),
        (13, doc_(vec![("$unset", d(vec![("a.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), n()]))]))),
        (14, doc_(vec![("$unset", d(vec![("a.0.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]))),
        (15, doc_(vec![("$unset", d(vec![("a.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), d(vec![("1", i(6))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), n()]))]))),
        (16, doc_(vec![("$unset", d(vec![("a.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![i(7), d(vec![("1", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![i(7), n()]))]))),
        (17, doc_(vec![("$unset", d(vec![("a.0", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![n()]))]))),
        (18, doc_(vec![("$unset", d(vec![("a.2", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(6)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), n()]))]))),
        (19, doc_(vec![("$unset", d(vec![("a.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), n()]))]))),
        (20, doc_(vec![("$unset", d(vec![("a.0.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]))),
        (21, doc_(vec![("$inc", d(vec![("a.3", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))]), n(), n(), i(1)]))]))),
        (22, doc_(vec![("$inc", d(vec![("a.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))]), i(1)]))]))),
        (23, doc_(vec![("$inc", d(vec![("a.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(10)]))]))),
        (24, doc_(vec![("$inc", d(vec![("a.0.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))])), ("1", i(1))])]))]))),
        (25, doc_(vec![("$inc", d(vec![("a.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), d(vec![("1", i(6))])]))]), Want::Refused(14)),
        (26, doc_(vec![("$inc", d(vec![("a.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![i(7), d(vec![("1", i(5))])]))]), Want::Refused(14)),
        (27, doc_(vec![("$inc", d(vec![("a.0", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", i(5))])]))]), Want::Refused(14)),
        (28, doc_(vec![("$inc", d(vec![("a.2", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(6)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(7)]))]))),
        (29, doc_(vec![("$inc", d(vec![("a.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(10)]))]))),
        (30, doc_(vec![("$inc", d(vec![("a.0.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))]), i(1)])]))]))),
        (31, doc_(vec![("$mul", d(vec![("a.3", i(2))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))]), n(), n(), i(0)]))]))),
        (32, doc_(vec![("$mul", d(vec![("a.1", i(2))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))]), i(0)]))]))),
        (33, doc_(vec![("$mul", d(vec![("a.1", i(2))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(18)]))]))),
        (34, doc_(vec![("$mul", d(vec![("a.0.1", i(2))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))])), ("1", i(0))])]))]))),
        (35, doc_(vec![("$mul", d(vec![("a.1", i(2))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), d(vec![("1", i(6))])]))]), Want::Refused(14)),
        (36, doc_(vec![("$mul", d(vec![("a.1", i(2))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![i(7), d(vec![("1", i(5))])]))]), Want::Refused(14)),
        (37, doc_(vec![("$mul", d(vec![("a.0", i(2))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", i(5))])]))]), Want::Refused(14)),
        (38, doc_(vec![("$mul", d(vec![("a.2", i(2))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(6)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(12)]))]))),
        (39, doc_(vec![("$mul", d(vec![("a.1", i(2))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(18)]))]))),
        (40, doc_(vec![("$mul", d(vec![("a.0.1", i(2))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))]), i(0)])]))]))),
        (41, doc_(vec![("$min", d(vec![("a.3", i(3))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))]), n(), n(), i(3)]))]))),
        (42, doc_(vec![("$min", d(vec![("a.1", i(3))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))]), i(3)]))]))),
        (43, doc_(vec![("$min", d(vec![("a.1", i(3))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(3)]))]))),
        (44, doc_(vec![("$min", d(vec![("a.0.1", i(3))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))])), ("1", i(3))])]))]))),
        (45, doc_(vec![("$min", d(vec![("a.1", i(3))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), d(vec![("1", i(6))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(3)]))]))),
        (46, doc_(vec![("$min", d(vec![("a.1", i(3))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![i(7), d(vec![("1", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![i(7), i(3)]))]))),
        (47, doc_(vec![("$min", d(vec![("a.0", i(3))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![i(3)]))]))),
        (48, doc_(vec![("$min", d(vec![("a.2", i(3))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(6)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(3)]))]))),
        (49, doc_(vec![("$min", d(vec![("a.1", i(3))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(3)]))]))),
        (50, doc_(vec![("$min", d(vec![("a.0.1", i(3))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))]), i(3)])]))]))),
        (51, doc_(vec![("$max", d(vec![("a.3", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))]), n(), n(), i(7)]))]))),
        (52, doc_(vec![("$max", d(vec![("a.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))]), i(7)]))]))),
        (53, doc_(vec![("$max", d(vec![("a.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]))),
        (54, doc_(vec![("$max", d(vec![("a.0.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))])), ("1", i(7))])]))]))),
        (55, doc_(vec![("$max", d(vec![("a.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), d(vec![("1", i(6))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), d(vec![("1", i(6))])]))]))),
        (56, doc_(vec![("$max", d(vec![("a.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![i(7), d(vec![("1", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![i(7), d(vec![("1", i(5))])]))]))),
        (57, doc_(vec![("$max", d(vec![("a.0", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", i(5))])]))]))),
        (58, doc_(vec![("$max", d(vec![("a.2", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(6)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(7)]))]))),
        (59, doc_(vec![("$max", d(vec![("a.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]))),
        (60, doc_(vec![("$max", d(vec![("a.0.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))]), i(7)])]))]))),
        (61, doc_(vec![("$bit", d(vec![("a.3", d(vec![("or", i(1))]))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))]), n(), n(), i(1)]))]))),
        (62, doc_(vec![("$bit", d(vec![("a.1", d(vec![("or", i(1))]))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))]), i(1)]))]))),
        (63, doc_(vec![("$bit", d(vec![("a.1", d(vec![("or", i(1))]))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]))),
        (64, doc_(vec![("$bit", d(vec![("a.0.1", d(vec![("or", i(1))]))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))])), ("1", i(1))])]))]))),
        (65, doc_(vec![("$bit", d(vec![("a.1", d(vec![("or", i(1))]))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), d(vec![("1", i(6))])]))]), Want::Refused(2)),
        (66, doc_(vec![("$bit", d(vec![("a.1", d(vec![("or", i(1))]))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![i(7), d(vec![("1", i(5))])]))]), Want::Refused(2)),
        (67, doc_(vec![("$bit", d(vec![("a.0", d(vec![("or", i(1))]))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", i(5))])]))]), Want::Refused(2)),
        (68, doc_(vec![("$bit", d(vec![("a.2", d(vec![("or", i(1))]))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(6)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(7)]))]))),
        (69, doc_(vec![("$bit", d(vec![("a.1", d(vec![("or", i(1))]))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]))),
        (70, doc_(vec![("$bit", d(vec![("a.0.1", d(vec![("or", i(1))]))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))]), i(1)])]))]))),
        (71, doc_(vec![("$push", d(vec![("a.3", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))]), n(), n(), a(vec![i(7)])]))]))),
        (72, doc_(vec![("$push", d(vec![("a.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))]), a(vec![i(7)])]))]))),
        (73, doc_(vec![("$push", d(vec![("a.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]), Want::Refused(2)),
        (74, doc_(vec![("$push", d(vec![("a.0.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))])), ("1", a(vec![i(7)]))])]))]))),
        (75, doc_(vec![("$push", d(vec![("a.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), d(vec![("1", i(6))])]))]), Want::Refused(2)),
        (76, doc_(vec![("$push", d(vec![("a.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![i(7), d(vec![("1", i(5))])]))]), Want::Refused(2)),
        (77, doc_(vec![("$push", d(vec![("a.0", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", i(5))])]))]), Want::Refused(2)),
        (78, doc_(vec![("$push", d(vec![("a.2", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(6)]))]), Want::Refused(2)),
        (79, doc_(vec![("$push", d(vec![("a.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]), Want::Refused(2)),
        (80, doc_(vec![("$push", d(vec![("a.0.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))]), a(vec![i(7)])])]))]))),
        (81, doc_(vec![("$addToSet", d(vec![("a.3", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))]), n(), n(), a(vec![i(7)])]))]))),
        (82, doc_(vec![("$addToSet", d(vec![("a.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))]), a(vec![i(7)])]))]))),
        (83, doc_(vec![("$addToSet", d(vec![("a.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]), Want::Refused(2)),
        (84, doc_(vec![("$addToSet", d(vec![("a.0.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))])), ("1", a(vec![i(7)]))])]))]))),
        (85, doc_(vec![("$addToSet", d(vec![("a.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), d(vec![("1", i(6))])]))]), Want::Refused(2)),
        (86, doc_(vec![("$addToSet", d(vec![("a.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![i(7), d(vec![("1", i(5))])]))]), Want::Refused(2)),
        (87, doc_(vec![("$addToSet", d(vec![("a.0", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", i(5))])]))]), Want::Refused(2)),
        (88, doc_(vec![("$addToSet", d(vec![("a.2", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(6)]))]), Want::Refused(2)),
        (89, doc_(vec![("$addToSet", d(vec![("a.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]), Want::Refused(2)),
        (90, doc_(vec![("$addToSet", d(vec![("a.0.1", i(7))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))]), a(vec![i(7)])])]))]))),
        (91, doc_(vec![("$pull", d(vec![("a.3", i(5))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]))),
        (92, doc_(vec![("$pull", d(vec![("a.1", i(5))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]))),
        (93, doc_(vec![("$pull", d(vec![("a.1", i(5))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]), Want::Refused(2)),
        (94, doc_(vec![("$pull", d(vec![("a.0.1", i(5))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]))),
        (95, doc_(vec![("$pull", d(vec![("a.1", i(5))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), d(vec![("1", i(6))])]))]), Want::Refused(2)),
        (96, doc_(vec![("$pull", d(vec![("a.1", i(5))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![i(7), d(vec![("1", i(5))])]))]), Want::Refused(2)),
        (97, doc_(vec![("$pull", d(vec![("a.0", i(5))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", i(5))])]))]), Want::Refused(2)),
        (98, doc_(vec![("$pull", d(vec![("a.2", i(5))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(6)]))]), Want::Refused(2)),
        (99, doc_(vec![("$pull", d(vec![("a.1", i(5))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]), Want::Refused(2)),
        (100, doc_(vec![("$pull", d(vec![("a.0.1", i(5))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]))),
        (101, doc_(vec![("$pop", d(vec![("a.3", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]))),
        (102, doc_(vec![("$pop", d(vec![("a.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]))),
        (103, doc_(vec![("$pop", d(vec![("a.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]), Want::Refused(14)),
        (104, doc_(vec![("$pop", d(vec![("a.0.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]))),
        (105, doc_(vec![("$pop", d(vec![("a.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), d(vec![("1", i(6))])]))]), Want::Refused(14)),
        (106, doc_(vec![("$pop", d(vec![("a.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![i(7), d(vec![("1", i(5))])]))]), Want::Refused(14)),
        (107, doc_(vec![("$pop", d(vec![("a.0", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", i(5))])]))]), Want::Refused(14)),
        (108, doc_(vec![("$pop", d(vec![("a.2", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(6)]))]), Want::Refused(14)),
        (109, doc_(vec![("$pop", d(vec![("a.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]), Want::Refused(14)),
        (110, doc_(vec![("$pop", d(vec![("a.0.1", i(1))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]))),
        (111, doc_(vec![("$rename", d(vec![("a.3", s("dst"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]))),
        (112, doc_(vec![("$rename", d(vec![("a.1", s("dst"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]))),
        (113, doc_(vec![("$rename", d(vec![("a.1", s("dst"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]), Want::Refused(2)),
        (114, doc_(vec![("$rename", d(vec![("a.0.1", s("dst"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]))),
        (115, doc_(vec![("$rename", d(vec![("a.1", s("dst"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), d(vec![("1", i(6))])]))]), Want::Refused(2)),
        (116, doc_(vec![("$rename", d(vec![("a.1", s("dst"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![i(7), d(vec![("1", i(5))])]))]), Want::Refused(2)),
        (117, doc_(vec![("$rename", d(vec![("a.0", s("dst"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", i(5))])]))]), Want::Refused(2)),
        (118, doc_(vec![("$rename", d(vec![("a.2", s("dst"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(6)]))]), Want::Refused(2)),
        (119, doc_(vec![("$rename", d(vec![("a.1", s("dst"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]), Want::Refused(2)),
        (120, doc_(vec![("$rename", d(vec![("a.0.1", s("dst"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]))),
        (121, doc_(vec![("$rename", d(vec![("src", s("a.3"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])])), ("src", i(7))]), Want::Refused(2)),
        (122, doc_(vec![("$rename", d(vec![("src", s("a.1"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])])), ("src", i(7))]), Want::Refused(2)),
        (123, doc_(vec![("$rename", d(vec![("src", s("a.1"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)])), ("src", i(7))]), Want::Refused(2)),
        (124, doc_(vec![("$rename", d(vec![("src", s("a.0.1"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])])), ("src", i(7))]), Want::Refused(2)),
        (125, doc_(vec![("$rename", d(vec![("src", s("a.1"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), d(vec![("1", i(6))])])), ("src", i(7))]), Want::Refused(2)),
        (126, doc_(vec![("$rename", d(vec![("src", s("a.1"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![i(7), d(vec![("1", i(5))])])), ("src", i(7))]), Want::Refused(2)),
        (127, doc_(vec![("$rename", d(vec![("src", s("a.0"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", i(5))])])), ("src", i(7))]), Want::Refused(2)),
        (128, doc_(vec![("$rename", d(vec![("src", s("a.2"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(6)])), ("src", i(7))]), Want::Refused(2)),
        (129, doc_(vec![("$rename", d(vec![("src", s("a.1"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)])), ("src", i(7))]), Want::Refused(2)),
        (130, doc_(vec![("$rename", d(vec![("src", s("a.0.1"))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])])), ("src", i(7))]), Want::Refused(2)),
        (131, doc_(vec![("$setOnInsert", d(vec![("a.3", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]))),
        (132, doc_(vec![("$setOnInsert", d(vec![("a.1", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]))),
        (133, doc_(vec![("$setOnInsert", d(vec![("a.1", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]))),
        (134, doc_(vec![("$setOnInsert", d(vec![("a.0.1", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]))),
        (135, doc_(vec![("$setOnInsert", d(vec![("a.1", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), d(vec![("1", i(6))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), d(vec![("1", i(6))])]))]))),
        (136, doc_(vec![("$setOnInsert", d(vec![("a.1", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![i(7), d(vec![("1", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![i(7), d(vec![("1", i(5))])]))]))),
        (137, doc_(vec![("$setOnInsert", d(vec![("a.0", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", i(5))])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", i(5))])]))]))),
        (138, doc_(vec![("$setOnInsert", d(vec![("a.2", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(6)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(6)]))]))),
        (139, doc_(vec![("$setOnInsert", d(vec![("a.1", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]))),
        (140, doc_(vec![("$setOnInsert", d(vec![("a.0.1", i(9))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]), Want::After(doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]))),
        (141, doc_(vec![("$currentDate", d(vec![("a.3", Bson::Boolean(true))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("3", i(5))])]))]), Want::Applies),
        (142, doc_(vec![("$currentDate", d(vec![("a.1", Bson::Boolean(true))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(1)]))])]))]), Want::Applies),
        (143, doc_(vec![("$currentDate", d(vec![("a.1", Bson::Boolean(true))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), i(9)]))]), Want::Applies),
        (144, doc_(vec![("$currentDate", d(vec![("a.0.1", Bson::Boolean(true))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", d(vec![("1", i(5))]))])]))]), Want::Applies),
        (145, doc_(vec![("$currentDate", d(vec![("a.1", Bson::Boolean(true))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", i(5))]), d(vec![("1", i(6))])]))]), Want::Applies),
        (146, doc_(vec![("$currentDate", d(vec![("a.1", Bson::Boolean(true))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![i(7), d(vec![("1", i(5))])]))]), Want::Applies),
        (147, doc_(vec![("$currentDate", d(vec![("a.0", Bson::Boolean(true))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("0", i(5))])]))]), Want::Applies),
        (148, doc_(vec![("$currentDate", d(vec![("a.2", Bson::Boolean(true))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("2", i(1))]), i(5), i(6)]))]), Want::Applies),
        (149, doc_(vec![("$currentDate", d(vec![("a.1", Bson::Boolean(true))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![d(vec![("1", a(vec![i(5), i(6)]))]), i(9)]))]), Want::Applies),
        (150, doc_(vec![("$currentDate", d(vec![("a.0.1", Bson::Boolean(true))]))]), doc_(vec![("_id", i(1)), ("a", a(vec![a(vec![d(vec![("1", i(5))])])]))]), Want::Applies),
    ]
}

#[test]
fn the_digit_segment_cases_of_q7s3j_jsonl_1_to_150_are_as_recorded() {
    let mut failures = Vec::new();
    for (case, update, before, want) in rows() {
        let mut got = before.clone();
        let outcome = parse(&update).and_then(|u| apply(&u, &mut got, NOW));
        match (&want, &outcome) {
            (Want::After(want), Ok(())) if format!("{got:?}") == format!("{want:?}") => {}
            (Want::Applies, Ok(())) | (Want::Refused(_), Err(_)) => {}
            _ => failures.push(format!(
                "case {case}: {update:?} over {before:?}: got {got:?} ({outcome:?}), \
                 recorded {}",
                match &want {
                    Want::After(want) => format!("{want:?}"),
                    Want::Applies => "an update".to_string(),
                    Want::Refused(code) => format!("a refusal, code {code}"),
                }
            )),
        }
    }
    assert!(failures.is_empty(), "{} of 150 differ:\n{}", failures.len(), failures.join("\n"));
}
