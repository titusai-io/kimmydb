//! The update path against the reference's recorded behaviour.
//!
//! Each case names the recording line it is built from: `q7s2.out` and
//! `q7s2b.out` in the Q7 step 2 probe set, made on the reference server
//! (6.0.5). A document is compared by its text, which keeps the order of its
//! fields; `Document`'s own equality does not.

use bson::{Document, doc};
use kimmy_query::update;

const NOW: i64 = 1_700_000_000_000;

fn parsed(update: Document) -> Result<update::Update, String> {
    update::parse(&update).map_err(|e| e.to_string())
}

/// The update applied to an existing document.
fn updated(update: Document, mut stored: Document) -> Result<Document, String> {
    update::apply(&parsed(update)?, &mut stored, NOW).map_err(|e| e.to_string())?;
    Ok(stored)
}

/// The update applied to the seed an upsert inserts.
fn upserted(update: Document, mut seed: Document) -> Result<Document, String> {
    update::apply_on_insert(&parsed(update)?, &mut seed, NOW).map_err(|e| e.to_string())?;
    Ok(seed)
}

/// A document as text, fields in stored order.
fn shown(d: &Document) -> String {
    d.to_string()
}

#[track_caller]
fn same(got: Result<Document, String>, want: Document) {
    let got = got.unwrap_or_else(|e| panic!("refused: {e}"));
    assert_eq!(shown(&got), shown(&want));
}

// ---------------------------------------------------------------------------
// (A) New fields land in sorted order at every level
// ---------------------------------------------------------------------------

#[test]
fn a_new_fields_are_sorted_whichever_operator_adds_them() {
    // q7s2.out lines 143 and 148.
    let want = doc! { "_id": 1, "a": 2, "b": 1, "c": 1 };
    same(
        updated(doc! { "$set": { "b": 1, "a": 2 }, "$inc": { "c": 1 } }, doc! { "_id": 1 }),
        want.clone(),
    );
    same(updated(doc! { "$inc": { "c": 1 }, "$set": { "b": 1, "a": 2 } }, doc! { "_id": 1 }), want);
}

#[test]
fn a_new_fields_set_in_reverse_come_out_sorted() {
    // q7s2.out line 153.
    same(
        updated(doc! { "$set": { "z": 1, "a": 2, "m": 3 } }, doc! { "_id": 1 }),
        doc! { "_id": 1, "a": 2, "m": 3, "z": 1 },
    );
}

#[test]
fn a_new_fields_nested_by_dotted_paths_are_sorted_at_each_level() {
    // q7s2.out line 158.
    same(
        updated(doc! { "$set": { "b.y": 1, "b.x": 2, "a": 3 } }, doc! { "_id": 1 }),
        doc! { "_id": 1, "a": 3, "b": { "x": 2, "y": 1 } },
    );
}

#[test]
fn a_new_fields_of_every_operator_are_sorted_together() {
    // q7s2.out line 163 (`$currentDate` aside, whose value is the clock) and
    // 183, where `$setOnInsert` is left out of an update of a stored document.
    let out = updated(
        doc! { "$push": { "p": 1 }, "$set": { "a": 1 }, "$min": { "m": 1 }, "$mul": { "k": 2 } },
        doc! { "_id": 1 },
    );
    same(out, doc! { "_id": 1, "a": 1, "k": 0, "m": 1, "p": [1] });
    same(
        updated(
            doc! { "$set": { "s1": 1 }, "$inc": { "i1": 1 }, "$max": { "m1": 1 }, "$setOnInsert": { "t1": 1 } },
            doc! { "_id": 1 },
        ),
        doc! { "_id": 1, "i1": 1, "m1": 1, "s1": 1 },
    );
}

#[test]
fn a_renamed_field_is_a_new_field_and_sorts_with_the_rest() {
    // q7s2.out line 168.
    same(
        updated(doc! { "$rename": { "s": "z" }, "$set": { "a": 1 } }, doc! { "_id": 1, "s": 1 }),
        doc! { "_id": 1, "a": 1, "z": 1 },
    );
}

#[test]
fn a_existing_fields_keep_their_place_and_new_ones_follow() {
    // q7s2.out lines 173 and 178: the stored fields stay where they are (here
    // in an order that is not sorted), the new one comes after them.
    same(
        updated(doc! { "$set": { "c": 3, "b": 9, "a": 8 } }, doc! { "_id": 1, "b": 2, "a": 1 }),
        doc! { "_id": 1, "b": 9, "a": 8, "c": 3 },
    );
    same(
        updated(
            doc! { "$unset": { "a": "" }, "$set": { "z": 1 } },
            doc! { "_id": 1, "a": 1, "b": 2 },
        ),
        doc! { "_id": 1, "b": 2, "z": 1 },
    );
}

#[test]
fn a_new_fields_of_an_array_element_follow_its_own() {
    // q7s2.out line 66: items.1.k is new in the second element.
    same(
        updated(
            doc! { "$set": { "items.1.z": 1, "items.1.k": 6 } },
            doc! { "_id": 1, "items": [{ "tags": ["a"] }, { "tags": ["c"] }] },
        ),
        doc! { "_id": 1, "items": [{ "tags": ["a"] }, { "tags": ["c"], "k": 6, "z": 1 }] },
    );
}

#[test]
fn a_document_given_as_a_value_keeps_its_own_order() {
    // Not a recorded case: only the fields an update adds by its own paths
    // are placed, not the inside of a value it was given.
    same(
        updated(doc! { "$set": { "v": { "z": 1, "a": 2 }, "b": 0 } }, doc! { "_id": 1 }),
        doc! { "_id": 1, "b": 0, "v": { "z": 1, "a": 2 } },
    );
}

#[test]
fn a_upsert_puts_the_filter_fields_first_sorted_then_every_new_field_sorted() {
    // q7s2.out lines 189 and 194: filters {k, j} and {j, k} give one result.
    let update = doc! {
        "$set": { "b": 1, "a": 2 },
        "$setOnInsert": { "c": 1, "z": 2 },
        "$inc": { "n": 1, "m": 1 },
    };
    let want = doc! { "_id": 1, "j": 2, "k": 1, "a": 2, "b": 1, "c": 1, "m": 1, "n": 1, "z": 2 };
    same(upserted(update.clone(), doc! { "_id": 1, "k": 1, "j": 2 }), want.clone());
    same(upserted(update, doc! { "_id": 1, "j": 2, "k": 1 }), want);
}

#[test]
fn a_upsert_with_the_inc_first_and_the_set_on_insert_last_is_sorted_too() {
    // q7s2.out lines 199 and 204.
    same(
        upserted(
            doc! { "$inc": { "n": 1 }, "$set": { "b": 1 }, "$setOnInsert": { "c": 1 } },
            doc! { "_id": 1, "k": 1 },
        ),
        doc! { "_id": 1, "k": 1, "b": 1, "c": 1, "n": 1 },
    );
    same(
        upserted(
            doc! { "$set": { "zz": 1, "aa": 2 }, "$setOnInsert": { "s2": 3, "s1": 4 } },
            doc! { "_id": 1 },
        ),
        doc! { "_id": 1, "aa": 2, "s1": 4, "s2": 3, "zz": 1 },
    );
}

#[test]
fn a_upsert_nests_a_dotted_write_beside_the_filter_field() {
    // q7s2.out lines 209 and 219: a.c joins the a.b the filter seeded, and a
    // filter field the update also sets keeps its place.
    same(
        upserted(doc! { "$set": { "a.c": 2 } }, doc! { "_id": 1, "a": { "b": 1 } }),
        doc! { "_id": 1, "a": { "b": 1, "c": 2 } },
    );
    same(
        upserted(doc! { "$set": { "a": 9, "c": 3 } }, doc! { "_id": 1, "a": 1, "b": 2 }),
        doc! { "_id": 1, "a": 9, "b": 2, "c": 3 },
    );
}

#[test]
fn a_upsert_sorts_the_fields_the_register_operators_create() {
    // q7s2.out line 224 (`$currentDate` left to its own test: its value is
    // the clock).
    same(
        upserted(
            doc! {
                "$mul": { "m": 3 }, "$min": { "lo": 2 }, "$max": { "hi": 7 },
                "$bit": { "b": { "or": 6 } }, "$push": { "arr": 1 }, "$addToSet": { "st": "x" },
            },
            doc! { "_id": 1 },
        ),
        doc! { "_id": 1, "arr": [1], "b": 6, "hi": 7, "lo": 2, "m": 0, "st": ["x"] },
    );
}
