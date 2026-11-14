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

// ---------------------------------------------------------------------------
// (B) Numeric segments address array elements at any depth; no write below
// a scalar
// ---------------------------------------------------------------------------

#[track_caller]
fn refused(got: Result<Document, String>, needle: &str) {
    match got {
        Ok(d) => panic!("applied, giving {d}; wanted a refusal naming {needle:?}"),
        Err(e) => assert!(e.contains(needle), "refused as {e:?}, which lacks {needle:?}"),
    }
}

fn grid() -> Document {
    doc! { "_id": 1, "grid": [[1, 2], [3, 4]] }
}

#[test]
fn b_a_numeric_segment_writes_an_element_of_a_nested_array() {
    // q7s2.out lines 6, 11, 16, 21, 26, 31, 36.
    same(
        updated(doc! { "$set": { "grid.0.1": 9 } }, grid()),
        doc! { "_id": 1, "grid": [[1, 9], [3, 4]] },
    );
    same(
        updated(doc! { "$set": { "grid.1.0": 9 } }, grid()),
        doc! { "_id": 1, "grid": [[1, 2], [9, 4]] },
    );
    same(
        updated(doc! { "$set": { "grid.0.5": 9 } }, grid()),
        doc! { "_id": 1, "grid": [[1, 2, null, null, null, 9], [3, 4]] },
    );
    same(
        updated(doc! { "$set": { "grid.2.0": 9 } }, grid()),
        doc! { "_id": 1, "grid": [[1, 2], [3, 4], { "0": 9 }] },
    );
    same(
        updated(doc! { "$inc": { "grid.0.0": 10 } }, grid()),
        doc! { "_id": 1, "grid": [[11, 2], [3, 4]] },
    );
    same(
        updated(doc! { "$unset": { "grid.0.0": "" } }, grid()),
        doc! { "_id": 1, "grid": [[null, 2], [3, 4]] },
    );
    same(
        updated(doc! { "$push": { "grid.0": 9 } }, grid()),
        doc! { "_id": 1, "grid": [[1, 2, 9], [3, 4]] },
    );
}

#[test]
fn b_a_numeric_segment_reaches_three_levels_down() {
    // q7s2.out lines 41, 46, 51.
    let d = doc! { "_id": 1, "d": [[[1, 2], [3]], [[4]]] };
    same(
        updated(doc! { "$set": { "d.0.0.1": 9 } }, d.clone()),
        doc! { "_id": 1, "d": [[[1, 9], [3]], [[4]]] },
    );
    same(
        updated(doc! { "$set": { "d.1.0.0": 9 } }, d.clone()),
        doc! { "_id": 1, "d": [[[1, 2], [3]], [[9]]] },
    );
    same(
        updated(doc! { "$set": { "d.0.1.0": 9 } }, d),
        doc! { "_id": 1, "d": [[[1, 2], [9]], [[4]]] },
    );
}

#[test]
fn b_a_numeric_segment_mixed_with_field_names_in_documents_and_arrays() {
    // q7s2.out lines 56, 61, 71, 76, 81, 86.
    let items = doc! { "_id": 1, "items": [{ "tags": ["a", "b"], "n": 1 }, { "tags": ["c"] }] };
    same(
        updated(doc! { "$set": { "items.0.tags.1": "z" } }, items.clone()),
        doc! { "_id": 1, "items": [{ "tags": ["a", "z"], "n": 1 }, { "tags": ["c"] }] },
    );
    same(
        updated(doc! { "$push": { "items.1.tags": "d" } }, items),
        doc! { "_id": 1, "items": [{ "tags": ["a", "b"], "n": 1 }, { "tags": ["c", "d"] }] },
    );
    same(
        updated(doc! { "$set": { "o.0": 6 } }, doc! { "_id": 1, "o": { "0": 5 } }),
        doc! { "_id": 1, "o": { "0": 6 } },
    );
    same(
        updated(doc! { "$set": { "x.0": 1 } }, doc! { "_id": 1 }),
        doc! { "_id": 1, "x": { "0": 1 } },
    );
    same(
        updated(doc! { "$set": { "x.0.y": 1 } }, doc! { "_id": 1 }),
        doc! { "_id": 1, "x": { "0": { "y": 1 } } },
    );
    same(
        updated(doc! { "$set": { "a.0.1": 9 } }, doc! { "_id": 1, "a": [{ "k": 1 }] }),
        doc! { "_id": 1, "a": [{ "k": 1, "1": 9 }] },
    );
}

#[test]
fn b_a_write_below_a_scalar_is_refused() {
    // q7s2.out lines 90, 95: an array element, and a field, that is a scalar.
    refused(
        updated(doc! { "$set": { "a.0.1": 9 } }, doc! { "_id": 1, "a": [7] }),
        "cannot create field '1' in element {0: 7}",
    );
    refused(
        updated(doc! { "$set": { "a.1": 9 } }, doc! { "_id": 1, "a": 5 }),
        "cannot create field '1' in element {a: 5}",
    );
    // Lines 561, 566, 576, 591: `$set`, `$inc` and `$push` below a scalar and
    // below a null.
    refused(
        updated(doc! { "$set": { "a.y": 1 } }, doc! { "_id": 1, "a": 5 }),
        "cannot create field 'y' in element {a: 5}",
    );
    refused(
        updated(doc! { "$set": { "a.y": 1 } }, doc! { "_id": 1, "a": bson::Bson::Null }),
        "cannot create field 'y' in element {a: null}",
    );
    refused(
        updated(doc! { "$inc": { "a.y": 1 } }, doc! { "_id": 1, "a": 5 }),
        "cannot create field 'y' in element {a: 5}",
    );
    refused(
        updated(doc! { "$push": { "a.y": 1 } }, doc! { "_id": 1, "a": 5 }),
        "cannot create field 'y' in element {a: 5}",
    );
    // Line 571: a field name into an array.
    refused(
        updated(doc! { "$set": { "a.y": 1 } }, doc! { "_id": 1, "a": [1] }),
        "non-numeric segment",
    );
}

#[test]
fn b_an_unset_below_a_scalar_changes_nothing() {
    // q7s2.out line 556.
    same(
        updated(doc! { "$unset": { "a.y": "" } }, doc! { "_id": 1, "a": 5 }),
        doc! { "_id": 1, "a": 5 },
    );
}

#[test]
fn b_an_upsert_writing_below_a_filter_scalar_is_refused() {
    // q7s2.out lines 449 and 464.
    refused(
        upserted(doc! { "$inc": { "a.x": 1 } }, doc! { "_id": 1, "a": 5 }),
        "cannot create field 'x' in element {a: 5}",
    );
    refused(
        upserted(doc! { "$set": { "a.y": 1 } }, doc! { "_id": 1, "a": 5 }),
        "cannot create field 'y' in element {a: 5}",
    );
}

#[test]
fn b_two_spellings_of_one_element_are_a_path_conflict() {
    // No recorded answer. The product refuses the pair: `a.1` and `a.01` are
    // one element, written twice.
    for update in [
        doc! { "$set": { "a.1": 5 }, "$inc": { "a.01": 1 } },
        doc! { "$inc": { "a.01": 1 }, "$set": { "a.1": 5 } },
    ] {
        let Err(err) = parsed(update) else { panic!("applied") };
        assert!(err.contains("conflict"), "{err}");
    }
}

// ---------------------------------------------------------------------------
// (C) $rename and array elements
// ---------------------------------------------------------------------------

fn renamed_doc() -> Document {
    doc! {
        "_id": 1, "items": [{ "sku": "a", "qty": 1 }, { "sku": "b" }], "s": "x",
        "arr": [1, 2], "o": { "p": 1 },
    }
}

#[test]
fn c_a_rename_out_of_an_array_element_is_refused() {
    // q7s2.out lines 101 and 121.
    refused(
        updated(doc! { "$rename": { "items.0.sku": "code" } }, renamed_doc()),
        "source field cannot be an array element",
    );
    refused(
        updated(doc! { "$rename": { "arr.0": "z" } }, renamed_doc()),
        "source field cannot be an array element",
    );
}

#[test]
fn c_a_rename_into_an_array_element_is_refused() {
    // q7s2.out lines 106 and 111.
    refused(
        updated(doc! { "$rename": { "s": "arr.1" } }, renamed_doc()),
        "destination field cannot be an array element",
    );
    refused(
        updated(doc! { "$rename": { "s": "items.0.s" } }, renamed_doc()),
        "destination field cannot be an array element",
    );
}

#[test]
fn c_a_rename_through_an_array_without_an_index_is_refused() {
    // q7s2.out line 116, and line 586 for a scalar in the way.
    refused(
        updated(doc! { "$rename": { "items.sku": "code" } }, renamed_doc()),
        "cannot use the part (items of items.sku) to traverse the element",
    );
    refused(
        updated(doc! { "$rename": { "a.y": "b" } }, doc! { "_id": 1, "a": 5 }),
        "cannot use the part (a of a.y) to traverse the element",
    );
}

#[test]
fn c_a_rename_of_a_whole_array_or_a_subdocument_field_still_applies() {
    // q7s2.out lines 126, 131, 136.
    let mut want = renamed_doc();
    let items = want.remove("items").unwrap();
    want.insert("lines", items);
    same(updated(doc! { "$rename": { "items": "lines" } }, renamed_doc()), want);

    let mut want = renamed_doc();
    want.insert("o", doc! { "q": 1 });
    same(updated(doc! { "$rename": { "o.p": "o.q" } }, renamed_doc()), want);

    same(
        updated(doc! { "$rename": { "s": "s2" }, "$set": { "arr.0": 9 } }, renamed_doc()),
        doc! {
            "_id": 1, "items": [{ "sku": "a", "qty": 1 }, { "sku": "b" }], "arr": [9, 2],
            "o": { "p": 1 }, "s2": "x",
        },
    );
}

// ---------------------------------------------------------------------------
// (D) Replacement upserts and `_id`
// ---------------------------------------------------------------------------

#[test]
fn d_a_replacement_upsert_does_not_carry_the_filter_fields() {
    // q7s2b.out lines 113, 118, 123, 128, 148: the filter's fields are not in
    // the document, and `_id` is left for the store to generate.
    same(upserted(doc! {}, doc! {}), doc! {});
    same(upserted(doc! {}, doc! { "_id": 1 }), doc! { "_id": 1 });
    same(upserted(doc! { "v": 2 }, doc! { "k": 1 }), doc! { "v": 2 });
    same(upserted(doc! { "x": 1 }, doc! { "a": { "b": 1 } }), doc! { "x": 1 });
    same(upserted(doc! { "x": 1 }, doc! { "a": { "b": 1 } }), doc! { "x": 1 });
}

#[test]
fn d_a_replacement_upsert_takes_the_id_of_the_filter_or_the_body() {
    // q7s2b.out lines 133, 138.
    same(upserted(doc! { "v": 2 }, doc! { "_id": 7, "k": 1 }), doc! { "_id": 7, "v": 2 });
    same(upserted(doc! { "_id": 9, "v": 2 }, doc! { "k": 1 }), doc! { "_id": 9, "v": 2 });
    // The same `_id` in both is not a difference.
    same(upserted(doc! { "_id": 7, "v": 2 }, doc! { "_id": 7 }), doc! { "_id": 7, "v": 2 });
}

#[test]
fn d_a_replacement_upsert_whose_body_id_differs_from_the_filter_id_is_refused() {
    // q7s2b.out line 142 (code 66, the immutable field).
    refused(upserted(doc! { "_id": 8, "v": 2 }, doc! { "_id": 7 }), "immutable");
}

#[test]
fn d_a_replacement_of_a_stored_document_keeps_only_its_id() {
    // q7s2b.out line 153.
    same(updated(doc! { "v": 2 }, doc! { "_id": 1, "k": 1, "z": 9 }), doc! { "_id": 1, "v": 2 });
}

#[test]
fn d_an_operator_upsert_keeps_the_filter_field() {
    // q7s2b.out line 158.
    same(upserted(doc! { "$set": { "v": 2 } }, doc! { "k": 1 }), doc! { "k": 1, "v": 2 });
}

// ---------------------------------------------------------------------------
// (E) The replacement of a stored document and its `_id`
// ---------------------------------------------------------------------------

#[test]
fn e_a_replacement_naming_a_different_id_is_refused() {
    // q7s2d.out lines 122, 143, 153, 158: code 66, the document unchanged.
    refused(updated(doc! { "_id": 2, "v": 1 }, doc! { "_id": 1, "k": 1 }), "_id is immutable");
}

#[test]
fn e_a_replacement_naming_the_same_id_replaces_everything_else() {
    // q7s2d.out lines 128, 149, 163.
    same(updated(doc! { "_id": 1, "v": 1 }, doc! { "_id": 1, "k": 1 }), doc! { "_id": 1, "v": 1 });
}

#[test]
fn e_an_equal_id_of_another_numeric_type_is_stored_as_written() {
    // q7s2d.out line 134: NumberLong(1) over the Int32 _id 1.
    let out = updated(doc! { "_id": 1_i64, "v": 1 }, doc! { "_id": 1, "k": 1 }).unwrap();
    assert!(matches!(out.get("_id"), Some(bson::Bson::Int64(1))), "{out}");
    assert_eq!(shown(&out), shown(&doc! { "_id": 1_i64, "v": 1 }));
}

#[test]
fn e_a_replacement_without_an_id_keeps_the_stored_one() {
    // q7s2d.out lines 139, 168.
    same(updated(doc! { "v": 1 }, doc! { "_id": 1, "k": 1 }), doc! { "_id": 1, "v": 1 });
}
