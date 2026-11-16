//! The filter, sort and limit rules of the reference's recorded query sweep (q7s3a to q7s3e,
//! reference 6.0.5), one test per root cause.
//!
//! Every case names its label and the `.out` line it is recorded at. A case here is the
//! reference's answer, not ours: `ids` are the ids it returned, and a refusal is a case it
//! refused. A behaviour that was not recorded is marked UNRECORDED where it is asserted.

use bson::{Bson, Document, doc};
use kimmy_query::filter::{self, matches};
use kimmy_query::shape;

fn refused(query: &Document) -> String {
    match filter::parse(query) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("{query} must be refused"),
    }
}

/// The ids a filter selects among `docs`, in order.
fn ids_over(docs: &[Document], query: &Document) -> Vec<i32> {
    let parsed = filter::parse(query).unwrap_or_else(|e| panic!("{query}: refused: {e}"));
    docs.iter()
        .filter(|d| matches(&parsed, d).unwrap_or_else(|e| panic!("{query}: {e}")))
        .map(|d| d.get_i32("_id").unwrap())
        .collect()
}

mod b_null_bounds;
mod common;
mod f_end_keys;
mod r13_type_codes;
mod r15_options;
mod r17_sorts;
mod r1_nan;
mod r2_end_keys;
mod r4_null_bounds;
mod r8_all;
mod ranges_a;
mod resolver;
mod resolver_bound_cases;
mod resolver_cases;
mod resolver_element_cases;
mod resolver_elements;
mod resolver_order_cases;
mod resolver_sort_cases;
mod resolver_sorts;

// ---------------------------------------------------------------------------
// R15: `$options` with no `$regex`
// ---------------------------------------------------------------------------

/// q7s3b0216 (q7s3b.out:6240): `{a: {$options: "i"}}` is refused, code 2, "$options needs a
/// $regex", where it matched all 24 documents. A multi update or a delete by it took all of
/// them.
#[test]
fn r15_an_options_with_no_regex_is_refused() {
    let message = refused(&doc! { "a": { "$options": "i" } });
    assert!(message.contains("$options needs a $regex"), "{message}");
    // As a condition among others, and with a bad flag or a non-string (still refused).
    refused(&doc! { "a": { "$options": "i", "$exists": true } });
    refused(&doc! { "a": { "$options": "I" } });
    refused(&doc! { "a": { "$options": 1 } });
    // Every position it can stand in is recorded in `r15_options.rs`.
}

#[test]
fn r15_an_options_beside_a_regex_still_reads_as_a_pattern() {
    let docs = [doc! { "_id": 1, "a": "Abc" }, doc! { "_id": 2, "a": "abc" }, doc! { "_id": 3 }];
    assert_eq!(ids_over(&docs, &doc! { "a": { "$regex": "^a", "$options": "i" } }), [1, 2]);
    assert_eq!(ids_over(&docs, &doc! { "a": { "$options": "i", "$regex": "^a" } }), [1, 2]);
    assert_eq!(ids_over(&docs, &doc! { "a": { "$regex": "^a" } }), [2]);
}

// ---------------------------------------------------------------------------
// R3: a regular expression as a range operand
// ---------------------------------------------------------------------------

fn regex(pattern: &str) -> Bson {
    Bson::RegularExpression(bson::Regex {
        pattern: pattern.to_string().try_into().expect("a pattern"),
        options: String::new().try_into().expect("options"),
    })
}

/// q7s3a0137 `$gt` (q7s3a.out:5527), a0172 `$gte` (7452), a0207 `$lt` (9377) and a0242 `$lte`
/// (11302): code 2, "Can't have RegEx as arg to predicate over field 'v'.", where they were
/// answered through the group a regex shares with `MinKey` and `MaxKey`.
#[test]
fn r3_a_regex_is_not_a_range_operand() {
    for (label, line, op) in [
        ("q7s3a0137", 5527, "$gt"),
        ("q7s3a0172", 7452, "$gte"),
        ("q7s3a0207", 9377, "$lt"),
        ("q7s3a0242", 11302, "$lte"),
    ] {
        let message = refused(&doc! { "v": { op: regex("a") } });
        assert!(
            message.contains("Can't have RegEx as arg to predicate over field 'v'."),
            "{label} (q7s3a.out:{line}): {message}"
        );
    }
    // Nested under `$not`, in an `$elemMatch` and as a partial index's filter: the same
    // refusal (UNRECORDED there, the same parse).
    refused(&doc! { "v": { "$not": { "$gt": regex("a") } } });
    refused(&doc! { "v": { "$elemMatch": { "$lt": regex("a") } } });
    assert!(kimmy_core::PartialFilter::parse(&doc! { "v": { "$gte": regex("a") } }).is_err());
    // `$eq` and a regex as a field's value are not ranges, and stay as they were.
    filter::parse(&doc! { "v": { "$eq": regex("a") } }).expect("$eq of a regex");
    filter::parse(&doc! { "v": regex("a") }).expect("a regex as a field's value");
}

// ---------------------------------------------------------------------------
// R11: an operator document inside `$all`
// ---------------------------------------------------------------------------

/// q7s3a0021 `find {v:{$all:[{$eq:1}]}}` (q7s3a.out:302) and a0022, its `count` (317): code 2,
/// "no $ expressions in $all", where `{$eq: 1}` was compared as a literal document and matched
/// nothing. `$elemMatch` members and regex members are read as filter-patterns reads them.
#[test]
fn r11_an_operator_document_inside_all_is_refused() {
    for query in [
        doc! { "v": { "$all": [{ "$eq": 1 }] } },
        doc! { "v": { "$all": [1, { "$gt": 0 }] } },
        doc! { "v": { "$all": [{ "$in": [1] }, 1] } },
        doc! { "v": { "$not": { "$all": [{ "$eq": 1 }] } } },
        doc! { "v": { "$elemMatch": { "w": { "$all": [{ "$eq": 1 }] } } } },
    ] {
        let message = refused(&query);
        assert!(message.contains("no $ expressions in $all"), "{query}: {message}");
    }
    // The members that are not operator documents stay what they were: a value, a document of
    // plain fields, a regex in its shapes, and `$elemMatch` documents.
    for query in [
        doc! { "v": { "$all": [1, { "a": 1 }] } },
        doc! { "v": { "$all": [regex("^a"), { "$regex": "b" }] } },
        doc! { "v": { "$all": [{ "$elemMatch": { "a": 1 } }] } },
        doc! { "v": { "$all": [{ "$elemMatch": { "a": 1 }, "b": 2 }] } },
    ] {
        filter::parse(&query).unwrap_or_else(|e| panic!("{query}: {e}"));
    }
}

// ---------------------------------------------------------------------------
// R14: a negative `$size`
// ---------------------------------------------------------------------------

/// q7s3b0132 `{a:{$size:-1}}` (q7s3b.out:3802): code 2, "Failed to parse $size. Expected a
/// non-negative number in: $size: -1", where it matched nothing. b0133 (`Double(2)`, 3831) and
/// b0136 (`Int64(2)`, 3919) are lengths; b0134 (`Double(2.5)`, 3860) and b0135 (`"2"`, 3889)
/// are refused, as before.
#[test]
fn r14_a_negative_size_is_refused() {
    for size in [Bson::Int32(-1), Bson::Int64(-1), Bson::Double(-1.0), Bson::Int32(i32::MIN)] {
        let message = refused(&doc! { "a": { "$size": size.clone() } });
        assert!(message.contains("Expected a non-negative number"), "{size}: {message}");
    }
    refused(&doc! { "a": { "$not": { "$size": -1 } } });
    refused(&doc! { "a": { "$elemMatch": { "$size": -3 } } });
    let docs = [
        doc! { "_id": 1, "a": [1, 2] },
        doc! { "_id": 2, "a": [] },
        doc! { "_id": 3, "a": [[1, 2]] },
        doc! { "_id": 4, "a": 5 },
    ];
    for (size, want) in [
        (Bson::Int32(0), vec![2]),
        (Bson::Double(2.0), vec![1]),
        (Bson::Int64(2), vec![1]),
        (Bson::Int32(1), vec![3]),
    ] {
        assert_eq!(ids_over(&docs, &doc! { "a": { "$size": size.clone() } }), want, "{size}");
    }
    refused(&doc! { "a": { "$size": 2.5 } });
    refused(&doc! { "a": { "$size": "2" } });
}

// ---------------------------------------------------------------------------
// R26: malformed projection field names
// ---------------------------------------------------------------------------

/// q7s3d0047 `{"":1}` (q7s3d.out:416, 40352), d0048 `"a."` (425, 40353), d0049 `".a"` (434,
/// 15998), d0050 `"$a"` (443, 16410) and d0051 `"a..b"` (452, 15998): refused, where they
/// answered `_id` alone (`a:[]` for `"a."` and `"a..b"` over `a:[1,2]`).
#[test]
fn r26_a_malformed_projection_field_name_is_refused() {
    for (label, line, name, why) in [
        ("q7s3d0047", 416, "", "FieldPath cannot be constructed with empty string"),
        ("q7s3d0048", 425, "a.", "FieldPath must not end with a '.'."),
        ("q7s3d0049", 434, ".a", "FieldPath field names may not be empty strings."),
        ("q7s3d0050", 443, "$a", "FieldPath field names may not start with '$'."),
        ("q7s3d0051", 452, "a..b", "FieldPath field names may not be empty strings."),
    ] {
        let spec = doc! { name: 1 };
        let message = match shape::parse_find_projection(&spec) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("{label} (q7s3d.out:{line}): {spec} must be refused"),
        };
        assert!(message.contains(why), "{label}: {message}");
    }
}

/// The three forms the reference was probed in, per name: `{name: 0}`, `{name: 1}` and
/// `{name: 0, d: 0}`, one row per name (q7s3i.jsonl lines 187 to 228, three lines each).
/// `refused` rows are refused in all three; `a.$` is refused with 0 (lines 205 and 207) and is
/// the positional projection with 1 (206); `a.$ref` and `a.b$` are accepted in all three.
#[test]
fn r26_every_row_of_the_exclusion_and_inclusion_forms() {
    let forms = |name: &str| [doc! { name: 0 }, doc! { name: 1 }, doc! { name: 0, "d": 0 }];
    let refused_rows = [
        (187, "a."),
        (190, ""),
        (193, "a..b"),
        (196, "$a"),
        (199, ".a"),
        (202, "a.b."),
        (211, "$"),
        (214, "a.$a"),
        (220, "_id."),
        (223, "a.0."),
        (226, "$$a"),
    ];
    for (first, name) in refused_rows {
        for (i, spec) in forms(name).iter().enumerate() {
            assert!(
                shape::parse_find_projection(spec).is_err(),
                "q7s3i.jsonl line {}: {spec} must be refused",
                first + i
            );
        }
    }
    for (first, name) in [(208, "a.$ref"), (217, "a.b$")] {
        for (i, spec) in forms(name).iter().enumerate() {
            assert!(
                shape::parse_find_projection(spec).is_ok(),
                "q7s3i.jsonl line {}: {spec} must be accepted",
                first + i
            );
        }
    }
    // `a.$`: refused with 0 (lines 205 and 207), the positional projection with 1 (206).
    assert!(shape::parse_find_projection(&doc! { "a.$": 0 }).is_err());
    assert!(shape::parse_find_projection(&doc! { "a.$": 0, "d": 0 }).is_err());
    let positional = shape::parse_find_projection(&doc! { "a.$": 1 }).unwrap().unwrap();
    assert!(positional.is_positional());
    // The accepted names project: a field named `$ref` and one ending in `$` are kept by an
    // inclusion and removed by an exclusion.
    let d = doc! { "_id": 1, "a": { "$ref": 5, "b$": 6, "c": 7 } };
    let included = shape::parse_find_projection(&doc! { "a.$ref": 1 }).unwrap().unwrap();
    assert_eq!(shape::project(included.plain(), &d), doc! { "_id": 1, "a": { "$ref": 5 } });
    let removed = shape::parse_find_projection(&doc! { "a.b$": 0 }).unwrap().unwrap();
    assert_eq!(shape::project(removed.plain(), &d), doc! { "_id": 1, "a": { "$ref": 5, "c": 7 } });
}

/// What stays a path: dotted and numeric names, `_id`, a `$` that is not the first character of a
/// segment, and the positional `$` as the last segment of a path.
#[test]
fn r26_the_names_that_are_paths_are_unchanged() {
    for spec in [
        doc! { "a": 1 },
        doc! { "a.b.c": 1 },
        doc! { "a.0": 1 },
        doc! { "a$": 1, "b.c$d": 1 },
        doc! { "_id": 0 },
        doc! { "items.$": 1 },
    ] {
        assert!(shape::parse_find_projection(&spec).is_ok(), "{spec}");
    }
}

// ---------------------------------------------------------------------------
// A member of `$in` or `$nin` with a `$`-led key
// ---------------------------------------------------------------------------

/// q7s3f0299 `{a: {$in: [{$options: "i"}]}}` (q7s3f.out:5247): code 2, "cannot nest $ under $in",
/// where the member was compared as a document and matched a stored document of that shape.
/// Refused for any `$`-led key in a member document, in `$in` and in `$nin`: UNRECORDED beyond
/// `$options`. A regex in one of its shapes stays a pattern. `$eq` of the whole document is not
/// refused, and finds a stored document with a `$`-led key as it did.
#[test]
fn a_member_document_of_in_or_nin_with_a_dollar_key_is_refused() {
    let message = refused(&doc! { "a": { "$in": [{ "$options": "i" }] } });
    assert!(message.contains("cannot nest $ under $in"), "{message}");
    // UNRECORDED generalisation.
    for query in [
        doc! { "a": { "$nin": [{ "$options": "i" }] } },
        doc! { "a": { "$in": [1, { "$gt": 1 }] } },
        doc! { "a": { "$nin": [{ "x": 1, "$y": 2 }] } },
        doc! { "a": { "$not": { "$in": [{ "$z": 1 }] } } },
    ] {
        let message = refused(&query);
        assert!(message.contains("cannot nest $ under"), "{query}: {message}");
    }
    // Not refused: a document of plain keys, and a regex in its shapes.
    filter::parse(&doc! { "a": { "$in": [{ "x": 1 }, { "$regex": "^a" }, { "$regex": "a", "$options": "i" }] } })
        .expect("members that are values or patterns");
    // `$eq` of the whole document finds the stored document.
    let stored = [doc! { "_id": 1, "a": { "$options": "i" } }, doc! { "_id": 2, "a": 5 }];
    assert_eq!(ids_over(&stored, &doc! { "a": { "$eq": { "$options": "i" } } }), [1]);
}
