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
    // q7s2.out lines 170-178 record a stored `a, b` and a stored `a, b` again;
    // these start from `b, a` and `a, b` (not recorded) to show that the
    // stored fields stay where they are, in an order that is not sorted, and
    // that the new one comes after them.
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
    // q7s2.out lines 62-66 record items.0.n and items.1.k; this adds a second
    // new field (z, written before k) to the second element, which is not
    // recorded, and asks for the same order the recording shows.
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
    // The seed arrives sorted, as the API builds it from the filter (the
    // end-to-end test in kimmy-api drives both filter orders); the update's own
    // fields follow it, sorted together.
    let update = doc! {
        "$set": { "b": 1, "a": 2 },
        "$setOnInsert": { "c": 1, "z": 2 },
        "$inc": { "n": 1, "m": 1 },
    };
    let want = doc! { "_id": 1, "j": 2, "k": 1, "a": 2, "b": 1, "c": 1, "m": 1, "n": 1, "z": 2 };
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
    // q7s2.out lines 56, 61, 71, 76, 81, 86. The last case differs from line
    // 86 in the order of two fields: the recording lists the numeric-looking
    // key "1" before "k", which says nothing a client can rely on (the
    // reference orders integer-like keys by their own rules), so this pins the
    // order the engine gives (the new field after the stored one).
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
        "cannot create field 'y' in element {a: [ 1 ]}",
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

// ---------------------------------------------------------------------------
// (G) $rename through an array by a non-numeric segment, and below a scalar
// ---------------------------------------------------------------------------

fn pair_items() -> Document {
    doc! { "_id": 1, "items": [{ "s": 1 }, { "s": 2 }], "x": 1 }
}

fn nested_items() -> Document {
    doc! { "_id": 1, "items": [[{ "s": 1 }], [{ "s": 2 }]], "x": 1 }
}

#[test]
fn g_a_rename_destination_through_an_array_by_a_field_name_is_refused() {
    // q7s2d.out lines 76, 81, 106.
    let dest = "destination field cannot be an array element";
    refused(updated(doc! { "$rename": { "x": "items.s" } }, pair_items()), dest);
    refused(updated(doc! { "$rename": { "x": "items.s" } }, nested_items()), dest);
    refused(updated(doc! { "$rename": { "x": "items.s.t" } }, pair_items()), dest);
}

#[test]
fn g_a_rename_source_through_an_array_by_a_field_name_is_refused() {
    // q7s2d.out lines 86, 91.
    let part = "cannot use the part (items of items.s) to traverse the element";
    refused(updated(doc! { "$rename": { "items.s": "y" } }, pair_items()), part);
    refused(updated(doc! { "$rename": { "items.s": "y" } }, nested_items()), part);
}

#[test]
fn g_a_rename_below_a_scalar_is_refused_and_changes_nothing() {
    // q7s2d.out lines 96 and 101.
    let stored = doc! { "_id": 1, "a": 5, "x": 1 };
    refused(
        updated(doc! { "$rename": { "x": "a.y" } }, stored.clone()),
        "cannot create field 'y' in element {a: 5}",
    );
    refused(
        updated(doc! { "$rename": { "a.y": "b" } }, doc! { "_id": 1, "a": 5 }),
        "cannot use the part (a of a.y) to traverse the element ({a: 5})",
    );
    // The refusal comes before anything moves, so a caller that applied the
    // update in place keeps the field it was renaming.
    let mut held = stored.clone();
    let update = parsed(doc! { "$rename": { "x": "a.y" } }).unwrap();
    assert!(update::apply(&update, &mut held, NOW).is_err());
    assert_eq!(shown(&held), shown(&stored));
}

#[test]
fn g_a_rename_onto_a_document_key_spelled_as_a_number_applies() {
    // q7s2d.out lines 111-112.
    same(
        updated(doc! { "$rename": { "x": "o.0" } }, doc! { "_id": 1, "o": { "0": 1 }, "x": 1 }),
        doc! { "_id": 1, "o": { "0": 1 } },
    );
}

#[test]
fn g_a_numeric_source_inside_an_array_is_refused() {
    // q7s2d.out line 116.
    refused(
        updated(doc! { "$rename": { "items.0.s": "items.0.t" } }, pair_items()),
        "source field cannot be an array element",
    );
}

// ---------------------------------------------------------------------------
// The `_id` of a replacement is a document id
// ---------------------------------------------------------------------------

fn decimal(text: &str) -> bson::Bson {
    bson::Bson::Decimal128(text.parse().expect("a decimal"))
}

#[test]
fn h_a_body_id_that_is_no_document_id_is_refused() {
    // Not recorded: a double is equal to the stored 1 by value, but it is no
    // `DocId`, and a document stored under it could not be read back by key.
    refused(updated(doc! { "_id": 1.0, "v": 1 }, doc! { "_id": 1, "k": 1 }), "_id is immutable");
    refused(
        updated(doc! { "_id": decimal("7.5"), "v": 1 }, doc! { "_id": 7, "k": 1 }),
        "_id is immutable",
    );
    // A replacement upsert: the filter's `_id` against the body's, and a body
    // `_id` with no filter `_id`.
    refused(upserted(doc! { "_id": 1.0, "v": 1 }, doc! { "_id": 1 }), "_id is immutable");
    assert!(upserted(doc! { "_id": 1.0, "v": 1 }, doc! {}).is_err());
    // A non-integral Decimal128 is a document id, so it is a valid body `_id`
    // when the filter names none, and differs from a stored integer id.
    assert!(upserted(doc! { "_id": decimal("7.5"), "v": 1 }, doc! {}).is_ok());
}

#[test]
fn h_equal_document_ids_are_stored_as_written() {
    // q7s2d.out line 134 for Int64; an integral Decimal128 is the same id.
    let out = updated(doc! { "_id": decimal("1"), "v": 1 }, doc! { "_id": 1, "k": 1 }).unwrap();
    assert!(matches!(out.get("_id"), Some(bson::Bson::Decimal128(_))), "{out}");
    let out = upserted(doc! { "_id": 1_i64, "v": 1 }, doc! { "_id": 1 }).unwrap();
    assert!(matches!(out.get("_id"), Some(bson::Bson::Int64(1))), "{out}");
}

#[test]
fn h_a_replacement_cannot_give_a_stored_document_an_id() {
    // A stored document has its `_id`; one that does not (a fixture of the
    // fuzz harness) is not given one by a replacement, which is a change of
    // `_id`. Only the document an upsert inserts takes the body's.
    refused(updated(doc! { "_id": 1, "v": 1 }, doc! { "k": 1 }), "_id is immutable");
    same(upserted(doc! { "_id": 1, "v": 1 }, doc! { "k": 1 }), doc! { "_id": 1, "v": 1 });
}

// ---------------------------------------------------------------------------
// A $rename whose source is missing moves nothing
// ---------------------------------------------------------------------------

#[test]
fn i_a_rename_of_a_missing_source_is_no_change_and_checks_nothing_else() {
    // q7s2e.out lines 5, 10, 15, 20, 25, 30, 35, 40, 45: every one is matched
    // and not modified, the document as it was, whatever the destination.
    let stored = || doc! { "_id": 1, "items": [{ "sku": "a" }, { "sku": "b" }], "arr": [1, 2] };
    for (from, to) in [
        ("items.0.nope", "code"),
        ("items.5.sku", "code"),
        ("items.0.nope", "items.1.x"),
        ("arr.5", "y"),
        ("nope", "items.0.t"),
        ("nope", "items.s"),
        ("nope", "missing.deep"),
    ] {
        same(updated(doc! { "$rename": { from: to } }, stored()), stored());
    }
    same(
        updated(doc! { "$rename": { "nope": "a.b" } }, doc! { "_id": 1, "a": 5 }),
        doc! { "_id": 1, "a": 5 },
    );
    same(
        updated(doc! { "$rename": { "items.0.nope": "code" } }, doc! { "_id": 1 }),
        doc! { "_id": 1 },
    );
}

// ---------------------------------------------------------------------------
// The source of a $rename: what it goes through
// ---------------------------------------------------------------------------

const TRAVERSE: &str = "cannot use the part (";

#[test]
fn j_a_rename_source_through_a_scalar_element_is_refused_whatever_follows() {
    // q7s2f.out lines 5, 21: `arr.0.x` over [1, 2], with a plain destination
    // and with `arr.1`, which would be refused for itself; the destination is
    // never looked at.
    for to in ["y", "arr.1"] {
        refused(
            updated(doc! { "$rename": { "arr.0.x": to } }, doc! { "_id": 1, "arr": [1, 2] }),
            "cannot use the part (0 of arr.0.x) to traverse the element ({0: 1})",
        );
    }
    // Lines 48, 53: `items.0.x` over a scalar element.
    refused(
        updated(doc! { "$rename": { "items.0.x": "y" } }, doc! { "_id": 1, "items": [7] }),
        "cannot use the part (0 of items.0.x) to traverse the element ({0: 7})",
    );
    refused(
        updated(
            doc! { "$rename": { "items.0.x": "items.1.t" } },
            doc! { "_id": 1, "items": [7, 8] },
        ),
        "cannot use the part (0 of items.0.x) to traverse the element ({0: 7})",
    );
}

#[test]
fn j_a_rename_source_through_a_scalar_field_of_an_element_is_refused() {
    // q7s2f.out lines 10, 26: `items.0.sku.x` over a document element whose
    // `sku` is a string; the part named is `sku`, not the array.
    let items = || doc! { "_id": 1, "items": [{ "sku": "a" }, { "sku": "b" }] };
    for to in ["y", "items.1.z"] {
        refused(
            updated(doc! { "$rename": { "items.0.sku.x": to } }, items()),
            "cannot use the part (sku of items.0.sku.x) to traverse the element",
        );
    }
    // Lines 58, 63: `items.0.y.z` over [{y: 5}].
    let ys = || doc! { "_id": 1, "items": [{ "y": 5 }, { "y": 6 }] };
    for to in ["w", "items.1.t"] {
        refused(
            updated(doc! { "$rename": { "items.0.y.z": to } }, ys()),
            "cannot use the part (y of items.0.y.z) to traverse the element ({y: 5})",
        );
    }
}

#[test]
fn j_a_rename_source_through_an_inner_array_by_a_name_is_refused() {
    // q7s2f.out lines 15, 31: `grid.0.s` over an array of arrays.
    let grid = || doc! { "_id": 1, "grid": [[1, 2], [3, 4]] };
    for to in ["y", "grid.1.0"] {
        refused(
            updated(doc! { "$rename": { "grid.0.s": to } }, grid()),
            "cannot use the part (0 of grid.0.s) to traverse the element",
        );
    }
}

#[test]
fn j_a_rename_of_a_source_that_is_not_there_checks_nothing() {
    // q7s2f.out lines 37, 42: a missing key, whatever the destination.
    let items = || doc! { "_id": 1, "items": [{ "sku": "a" }, { "sku": "b" }] };
    for to in ["y", "items.1.t"] {
        same(updated(doc! { "$rename": { "nope.x": to } }, items()), items());
    }
    // Line 69: an index past the end, then a path a scalar could not follow.
    same(
        updated(doc! { "$rename": { "items.5.x.y": "y" } }, doc! { "_id": 1, "items": [7] }),
        doc! { "_id": 1, "items": [7] },
    );
}

#[test]
fn j_a_rename_source_that_is_there_inside_an_array_cannot_be_moved() {
    // q7s2f.out lines 74, 79 (code 2).
    refused(
        updated(doc! { "$rename": { "arr.0": "y" } }, doc! { "_id": 1, "arr": [1, 2] }),
        "source field cannot be an array element",
    );
    refused(
        updated(
            doc! { "$rename": { "items.0.sku": "y" } },
            doc! { "_id": 1, "items": [{ "sku": "a" }, { "sku": "b" }] },
        ),
        "source field cannot be an array element",
    );
}

#[test]
fn j_the_traverse_refusal_uses_the_engines_wording() {
    // The recorded message begins `cannot use the part (X of path)`; ours
    // names the operator in front of it, as the other `$rename` refusals do.
    let Err(err) = updated(doc! { "$rename": { "arr.0.x": "y" } }, doc! { "_id": 1, "arr": [1] })
    else {
        panic!("applied")
    };
    assert!(err.starts_with("invalid update: $rename ") && err.contains(TRAVERSE), "{err}");
}

#[test]
fn j_a_rename_source_below_a_null_is_a_traverse_refusal_and_a_number_on_a_document_is_a_key() {
    // Not recorded; derived from the walk rule: a null is a value a path
    // cannot enter, and names the part that holds it.
    refused(
        updated(doc! { "$rename": { "a.b": "c" } }, doc! { "a": bson::Bson::Null }),
        "cannot use the part (a of a.b) to traverse the element ({a: null})",
    );
    // Not recorded; derived from the walk rule: a numeric segment on a
    // document is a key, so the source is found and moves.
    same(
        updated(doc! { "$rename": { "o.0.x": "y" } }, doc! { "o": { "0": { "x": 1 } } }),
        doc! { "o": { "0": {} }, "y": 1 },
    );
}

// ---------------------------------------------------------------------------
// (K) A write through an index far past the end of an array
// ---------------------------------------------------------------------------

const BACKFILL: &str = "can't backfill more than 1500000 elements";

fn arr_of_one() -> Document {
    doc! { "_id": 1, "arr": [7] }
}

#[test]
fn k_a_write_more_than_the_limit_past_the_end_is_refused_and_changes_nothing() {
    // q7s2g.out lines 59-76 (1500002 and 2000000 over `arr: [7]`; the length is
    // still 1 after). The index here, 5000000, is a modest one for the test:
    // the refusal is the same, and a build that does not refuse allocates it.
    for update in [
        doc! { "$set": { "arr.5000000": 1 } },
        doc! { "$inc": { "arr.5000000": 1 } },
        doc! { "$set": { "arr.5000000.x": 1 } },
    ] {
        let mut stored = arr_of_one();
        let compiled = parsed(update.clone()).unwrap();
        let err = update::apply(&compiled, &mut stored, NOW).unwrap_err().to_string();
        assert!(err.contains(BACKFILL), "{update}: {err}");
        assert_eq!(stored.get_array("arr").unwrap().len(), 1, "{update}: nothing was pushed");
    }
}

#[test]
fn k_the_limit_is_a_gap_of_exactly_one_and_a_half_million() {
    // q7s2g.out lines 52-58: over `arr: [7]` (length 1) the index 1500001 is
    // written (length 1500002) and 1500002 is refused (lines 59-61).
    let at = |index: usize| {
        let key = format!("arr.{index}");
        updated(doc! { "$set": { key: 1 } }, arr_of_one())
    };
    let written = at(1_500_001).expect("a gap of exactly 1500000 is allowed");
    assert_eq!(written.get_array("arr").unwrap().len(), 1_500_002);
    refused(at(1_500_002), BACKFILL);
}

#[test]
fn k_a_missing_parent_makes_a_key_never_a_backfill() {
    // q7s2g.out lines 326-385: over a document with no `arr` the digits are a
    // field name, however large.
    same(
        updated(doc! { "$set": { "arr.1500001": 1 } }, doc! { "_id": 1 }),
        doc! { "_id": 1, "arr": { "1500001": 1 } },
    );
    same(
        updated(doc! { "$set": { "arr.1500001.x": 1 } }, doc! { "_id": 1 }),
        doc! { "_id": 1, "arr": { "1500001": { "x": 1 } } },
    );
}

#[test]
fn k_a_segment_too_large_to_be_an_index_is_a_field_that_cannot_be_created() {
    // q7s2g.out lines 76-116: `arr.99999999999` over `arr: [7]` is
    // `Cannot create field '99999999999' in element {arr: [ 7 ]}` for `$set`,
    // `$inc` and a subfield, and the array is unchanged.
    let want = "cannot create field '99999999999' in element {arr: [ 7 ]}";
    refused(updated(doc! { "$set": { "arr.99999999999": 1 } }, arr_of_one()), want);
    refused(updated(doc! { "$inc": { "arr.99999999999": 1 } }, arr_of_one()), want);
    refused(updated(doc! { "$set": { "arr.99999999999.x": 1 } }, arr_of_one()), want);
    // UNRECORDED: where the reference draws the line between this refusal and
    // the backfill one. Any value that does not fit an i32 is treated as the
    // first; 2147483647 fits, so it is a backfill refusal (the gap is huge).
    refused(updated(doc! { "$set": { "arr.2147483647": 1 } }, arr_of_one()), BACKFILL);
    refused(
        updated(doc! { "$set": { "arr.2147483648": 1 } }, arr_of_one()),
        "cannot create field '2147483648' in element {arr: [ 7 ]}",
    );
}

#[test]
fn k_a_missing_parent_with_a_huge_segment_is_still_a_key() {
    // q7s2g.out lines 326-385: `arr.99999999999` over a document with no `arr`.
    same(
        updated(doc! { "$set": { "arr.99999999999": 1 } }, doc! { "_id": 1 }),
        doc! { "_id": 1, "arr": { "99999999999": 1 } },
    );
}

// ---------------------------------------------------------------------------
// (L) What spells an array index: a run of digits, and nothing with a sign
// ---------------------------------------------------------------------------

fn arr_of_two() -> Document {
    doc! { "_id": 1, "arr": [7, 8] }
}

#[test]
fn l_a_signed_segment_is_a_field_name_that_cannot_be_created_in_an_array() {
    // q7s2g.out lines 118-122 (`+5`) and 133-137 (`+0`).
    for field in ["+5", "+0"] {
        let key = format!("arr.{field}");
        refused(
            updated(doc! { "$set": { key.clone(): 1 } }, arr_of_two()),
            &format!("cannot create field '{field}' in element {{arr: [ 7, 8 ]}}"),
        );
        refused(
            updated(doc! { "$inc": { key: 1 } }, arr_of_two()),
            &format!("cannot create field '{field}' in element {{arr: [ 7, 8 ]}}"),
        );
    }
}

#[test]
fn l_an_unset_of_a_signed_segment_on_an_array_changes_nothing() {
    // q7s2g.out lines 123-127 (`+5`) and 138-142 (`+0`): matched 1, modified 0.
    for key in ["arr.+5", "arr.+0"] {
        same(updated(doc! { "$unset": { key: "" } }, arr_of_two()), arr_of_two());
    }
}

#[test]
fn l_a_rename_source_with_a_signed_segment_on_an_array_is_the_traverse_refusal() {
    // q7s2g.out lines 128-132 and 143-147.
    for (key, part) in [("arr.+5", "arr of arr.+5"), ("arr.+0", "arr of arr.+0")] {
        refused(
            updated(doc! { "$rename": { key: "y" } }, arr_of_two()),
            &format!("cannot use the part ({part}) to traverse the element"),
        );
    }
}

#[test]
fn l_leading_zeros_spell_an_index() {
    // q7s2g.out lines 148-175: `00` is index 0 and `01` is index 1, for `$set`
    // and `$unset`; a `$rename` of one is the array-element refusal.
    same(updated(doc! { "$set": { "arr.00": 1 } }, arr_of_two()), doc! { "_id": 1, "arr": [1, 8] });
    same(updated(doc! { "$set": { "arr.01": 1 } }, arr_of_two()), doc! { "_id": 1, "arr": [7, 1] });
    same(
        updated(doc! { "$unset": { "arr.00": "" } }, arr_of_two()),
        doc! { "_id": 1, "arr": [bson::Bson::Null, 8] },
    );
    same(
        updated(doc! { "$unset": { "arr.01": "" } }, arr_of_two()),
        doc! { "_id": 1, "arr": [7, bson::Bson::Null] },
    );
    for key in ["arr.00", "arr.01"] {
        refused(
            updated(doc! { "$rename": { key: "y" } }, arr_of_two()),
            "source field cannot be an array element",
        );
    }
}

#[test]
fn l_over_a_document_every_spelling_is_a_key() {
    // q7s2g.out lines 176-255 and the top-level `+0` after them.
    let keyed = || doc! { "_id": 1, "o": { "+0": 5, "+5": 5, "00": 5, "01": 5 } };
    same(
        updated(doc! { "$set": { "o.+5": 1 } }, keyed()),
        doc! { "_id": 1, "o": { "+0": 5, "+5": 1, "00": 5, "01": 5 } },
    );
    same(
        updated(doc! { "$unset": { "o.+0": "" } }, keyed()),
        doc! { "_id": 1, "o": { "+5": 5, "00": 5, "01": 5 } },
    );
    same(
        updated(doc! { "$rename": { "o.+0": "y" } }, keyed()),
        doc! { "_id": 1, "o": { "+5": 5, "00": 5, "01": 5 }, "y": 5 },
    );
    same(
        updated(doc! { "$rename": { "o.01": "y" } }, keyed()),
        doc! { "_id": 1, "o": { "+0": 5, "+5": 5, "00": 5 }, "y": 5 },
    );
    // An array beside a key of the same spelling is still an array.
    refused(
        updated(doc! { "$set": { "arr.+0": 1 } }, doc! { "_id": 1, "arr": [7, 8], "+0": 5 }),
        "cannot create field '+0' in element {arr: [ 7, 8 ]}",
    );
}

#[test]
fn l_a_field_name_that_is_not_an_index_on_an_array_is_the_same_refusal() {
    // q7s2.out line 571: `a.y` over `a: [1]`.
    refused(
        updated(doc! { "$set": { "a.y": 1 } }, doc! { "_id": 1, "a": [1] }),
        "cannot create field 'y' in element {a: [ 1 ]}",
    );
}
