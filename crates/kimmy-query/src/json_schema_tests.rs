//! `$jsonSchema`: every keyword's accepting and rejecting cases, the parse
//! refusals, and the failure reasons.

use bson::{Bson, Decimal128, Document, doc};
use proptest::prelude::*;

use crate::json_schema::{MAX_DEPTH, Schema};

fn schema(s: Document) -> Schema {
    Schema::parse(&Bson::Document(s)).unwrap_or_else(|e| panic!("should parse: {e}"))
}

fn refusal(s: Document) -> String {
    match Schema::parse(&Bson::Document(s)) {
        Ok(_) => panic!("should have been refused"),
        Err(e) => {
            assert!(matches!(e, kimmy_core::Error::InvalidQuery(_)), "not a 400: {e:?}");
            e.to_string()
        }
    }
}

fn dec(s: &str) -> Bson {
    Bson::Decimal128(s.parse::<Decimal128>().unwrap())
}

/// The documents `s` accepts and rejects.
fn check(s: Document, accepts: &[Document], rejects: &[Document]) {
    let parsed = schema(s.clone());
    for d in accepts {
        assert!(parsed.is_valid(d), "{s} should accept {d}");
        assert_eq!(parsed.explain_failure(d), None, "{s} accepts {d}");
    }
    for d in rejects {
        assert!(!parsed.is_valid(d), "{s} should reject {d}");
        assert!(parsed.explain_failure(d).is_some(), "{s} rejects {d}");
    }
}

/// The reason `s` gives for rejecting `d`.
fn reason(s: Document, d: Document) -> String {
    let parsed = schema(s);
    assert!(!parsed.is_valid(&d));
    parsed.explain_failure(&d).expect("a reason")
}

// -- the type keywords -------------------------------------------------------

#[test]
fn every_bson_type_alias_accepts_its_type_and_only_it() {
    let samples: Vec<(&str, Bson)> = vec![
        ("int", Bson::Int32(1)),
        ("long", Bson::Int64(1)),
        ("double", Bson::Double(1.5)),
        ("decimal", dec("1.5")),
        ("string", Bson::String("a".into())),
        ("object", Bson::Document(doc! {"a": 1})),
        ("array", Bson::Array(vec![])),
        ("bool", Bson::Boolean(true)),
        ("date", Bson::DateTime(bson::DateTime::from_millis(0))),
        ("objectId", Bson::ObjectId(bson::oid::ObjectId::new())),
        ("null", Bson::Null),
        (
            "binData",
            Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: vec![1],
            }),
        ),
        ("timestamp", Bson::Timestamp(bson::Timestamp { time: 1, increment: 1 })),
        (
            "regex",
            Bson::RegularExpression(bson::Regex {
                pattern: "a".to_string().try_into().unwrap(),
                options: String::new().try_into().unwrap(),
            }),
        ),
    ];
    for (alias, _) in &samples {
        let s = schema(doc! {"properties": {"v": {"bsonType": *alias}}});
        for (name, value) in &samples {
            let d = doc! {"v": value.clone()};
            assert_eq!(s.is_valid(&d), name == alias, "bsonType {alias} against a {name}");
        }
    }
    let number = schema(doc! {"properties": {"v": {"bsonType": "number"}}});
    for (name, value) in &samples {
        let numeric = matches!(*name, "int" | "long" | "double" | "decimal");
        assert_eq!(number.is_valid(&doc! {"v": value.clone()}), numeric, "number vs {name}");
    }
}

#[test]
fn bson_type_int_does_not_accept_a_double_of_whole_value() {
    check(
        doc! {"properties": {"v": {"bsonType": "int"}}},
        &[doc! {"v": 1}],
        &[doc! {"v": 1.0}, doc! {"v": 1_i64}],
    );
}

#[test]
fn bson_type_takes_an_array_of_names_any_of_which_holds() {
    check(
        doc! {"properties": {"v": {"bsonType": ["int", "null"]}}},
        &[doc! {"v": 1}, doc! {"v": Bson::Null}, doc! {}],
        &[doc! {"v": "x"}],
    );
}

#[test]
fn type_names_map_to_their_bson_types() {
    let s = |t: &str| schema(doc! {"properties": {"v": {"type": t}}});
    assert!(s("object").is_valid(&doc! {"v": {"a": 1}}));
    assert!(!s("object").is_valid(&doc! {"v": [1]}));
    assert!(s("array").is_valid(&doc! {"v": [1]}));
    assert!(!s("array").is_valid(&doc! {"v": 1}));
    for n in [Bson::Int32(1), Bson::Int64(1), Bson::Double(1.5), dec("2")] {
        assert!(s("number").is_valid(&doc! {"v": n}));
    }
    assert!(!s("number").is_valid(&doc! {"v": "1"}));
    assert!(s("boolean").is_valid(&doc! {"v": true}));
    assert!(!s("boolean").is_valid(&doc! {"v": 1}));
    assert!(s("string").is_valid(&doc! {"v": "a"}));
    assert!(!s("string").is_valid(&doc! {"v": Bson::Null}));
    assert!(s("null").is_valid(&doc! {"v": Bson::Null}));
    assert!(!s("null").is_valid(&doc! {"v": false}));
    check(
        doc! {"properties": {"v": {"type": ["string", "number"]}}},
        &[doc! {"v": "a"}, doc! {"v": 1}],
        &[doc! {"v": true}],
    );
}

#[test]
fn type_and_bson_type_on_one_level_must_both_hold() {
    check(
        doc! {"properties": {"v": {"type": "number", "bsonType": "int"}}},
        &[doc! {"v": 1}],
        &[doc! {"v": 1.5}, doc! {"v": "a"}],
    );
}

#[test]
fn the_document_is_an_object() {
    check(doc! {"bsonType": "object"}, &[doc! {}], &[]);
    check(doc! {"type": "object"}, &[doc! {"a": 1}], &[]);
    check(doc! {"bsonType": "string"}, &[], &[doc! {}]);
}

// -- enum --------------------------------------------------------------------

#[test]
fn enum_compares_by_the_total_order() {
    check(
        doc! {"properties": {"v": {"enum": [1, "b", Bson::Null]}}},
        &[
            doc! {"v": 1},
            doc! {"v": 1.0},
            doc! {"v": 1_i64},
            doc! {"v": dec("1.00")},
            doc! {"v": "b"},
        ],
        &[doc! {"v": 2}, doc! {"v": "1"}, doc! {"v": true}],
    );
    check(
        doc! {"properties": {"v": {"enum": [{"a": 1}, [1, 2]]}}},
        &[doc! {"v": {"a": 1.0}}, doc! {"v": [1.0, 2]}],
        &[doc! {"v": {"a": 2}}, doc! {"v": [2, 1]}],
    );
}

#[test]
fn enum_on_the_document_itself() {
    check(doc! {"enum": [{"a": 1}]}, &[doc! {"a": 1.0}], &[doc! {"a": 2}, doc! {}]);
}

// -- numbers -----------------------------------------------------------------

#[test]
fn minimum_and_maximum_are_inclusive_unless_flagged() {
    let p = |k: Document| doc! {"properties": {"v": k}};
    check(
        p(doc! {"minimum": 5}),
        &[doc! {"v": 5}, doc! {"v": 6}, doc! {"v": 5.0}, doc! {}, doc! {"v": "a"}],
        &[doc! {"v": 4}, doc! {"v": 4.99}],
    );
    check(
        p(doc! {"minimum": 5, "exclusiveMinimum": true}),
        &[doc! {"v": 6}, doc! {"v": 5.01}],
        &[doc! {"v": 5}, doc! {"v": 5.0}, doc! {"v": 4}],
    );
    check(p(doc! {"minimum": 5, "exclusiveMinimum": false}), &[doc! {"v": 5}], &[doc! {"v": 4}]);
    check(
        p(doc! {"maximum": 5}),
        &[doc! {"v": 5}, doc! {"v": 4}, doc! {"v": 5.0}],
        &[doc! {"v": 6}, doc! {"v": 5.01}],
    );
    check(
        p(doc! {"maximum": 5, "exclusiveMaximum": true}),
        &[doc! {"v": 4}, doc! {"v": 4.99}],
        &[doc! {"v": 5}, doc! {"v": 6}],
    );
}

#[test]
fn bounds_compare_numbers_of_every_type_by_value() {
    let s = schema(doc! {"properties": {"v": {"minimum": 1, "maximum": 2}}});
    for ok in
        [Bson::Int32(1), Bson::Int64(2), Bson::Double(1.5), dec("1.0"), dec("2.00"), dec("1.5")]
    {
        assert!(s.is_valid(&doc! {"v": ok.clone()}), "{ok}");
    }
    for bad in
        [dec("0.9999999999999999999999999999999999"), dec("2.0000000000000000000000000000001")]
    {
        assert!(!s.is_valid(&doc! {"v": bad.clone()}), "{bad}");
    }
    // A decimal bound is a bound, and Decimal128 0.1 is below the double 0.1.
    let d = schema(doc! {"properties": {"v": {"minimum": dec("0.1")}}});
    assert!(d.is_valid(&doc! {"v": 0.1}));
    assert!(!d.is_valid(&doc! {"v": dec("0.09")}));
    let below = schema(doc! {"properties": {"v": {"maximum": dec("0.1")}}});
    assert!(!below.is_valid(&doc! {"v": 0.1}), "the double 0.1 is above the decimal 0.1");
    assert!(below.is_valid(&doc! {"v": dec("0.1")}));
    // Past what a double holds exactly.
    let big = schema(doc! {"properties": {"v": {"maximum": 9_007_199_254_740_993_i64}}});
    assert!(big.is_valid(&doc! {"v": 9_007_199_254_740_993_i64}));
    assert!(!big.is_valid(&doc! {"v": 9_007_199_254_740_994_i64}));
}

#[test]
fn multiple_of_is_exact_for_integers() {
    check(
        doc! {"properties": {"v": {"multipleOf": 3}}},
        &[doc! {"v": 9}, doc! {"v": 0}, doc! {"v": -6}, doc! {"v": 9_i64}, doc! {"v": "x"}],
        &[doc! {"v": 10}, doc! {"v": -7}],
    );
    // i64::MIN is a multiple of 2 and not of 3; no overflow on the way.
    let two = schema(doc! {"properties": {"v": {"multipleOf": 2_i64}}});
    assert!(two.is_valid(&doc! {"v": i64::MIN}));
    let three = schema(doc! {"properties": {"v": {"multipleOf": 3_i64}}});
    assert!(!three.is_valid(&doc! {"v": i64::MIN}));
    let big = schema(doc! {"properties": {"v": {"multipleOf": 7_i64}}});
    assert!(big.is_valid(&doc! {"v": 7_i64 * 1_000_000_000_000}));
    assert!(!big.is_valid(&doc! {"v": 7_i64 * 1_000_000_000_000 + 1}));
}

#[test]
fn multiple_of_is_exact_for_decimals() {
    let tenth = schema(doc! {"properties": {"v": {"multipleOf": dec("0.1")}}});
    for ok in [dec("0.3"), dec("0.30"), dec("12.7"), dec("0"), dec("-0.5")] {
        assert!(tenth.is_valid(&doc! {"v": ok.clone()}), "{ok}");
    }
    for bad in [dec("0.35"), dec("0.05"), dec("0.30000000000000000000000000000001")] {
        assert!(!tenth.is_valid(&doc! {"v": bad.clone()}), "{bad}");
    }
    // An integer value against a decimal divisor, and the reverse.
    assert!(tenth.is_valid(&doc! {"v": 3}));
    assert!(tenth.is_valid(&doc! {"v": 3_i64}));
    let three = schema(doc! {"properties": {"v": {"multipleOf": 3}}});
    assert!(three.is_valid(&doc! {"v": dec("9")}));
    assert!(three.is_valid(&doc! {"v": dec("9.000")}));
    assert!(three.is_valid(&doc! {"v": dec("3E+20")}));
    assert!(!three.is_valid(&doc! {"v": dec("10")}));
    assert!(!three.is_valid(&doc! {"v": dec("4.5")}));
    let quarter = schema(doc! {"properties": {"v": {"multipleOf": dec("0.25")}}});
    assert!(quarter.is_valid(&doc! {"v": dec("1.75")}));
    assert!(!quarter.is_valid(&doc! {"v": dec("1.8")}));
    let wide = schema(doc! {"properties": {"v": {"multipleOf": dec("1E+30")}}});
    assert!(wide.is_valid(&doc! {"v": dec("5E+30")}));
    assert!(!wide.is_valid(&doc! {"v": dec("5E+29")}));
    // Non-finite decimals are a multiple of nothing.
    for odd in ["NaN", "Infinity", "-Infinity"] {
        assert!(!tenth.is_valid(&doc! {"v": dec(odd)}), "{odd}");
    }
}

#[test]
fn multiple_of_on_doubles_is_the_floating_point_remainder() {
    let half = schema(doc! {"properties": {"v": {"multipleOf": 0.5}}});
    assert!(half.is_valid(&doc! {"v": 2.5}));
    assert!(half.is_valid(&doc! {"v": 3}));
    assert!(!half.is_valid(&doc! {"v": 2.6}));
    // Neither 0.3 nor 0.1 is stored exactly, so the double test says no.
    let tenth = schema(doc! {"properties": {"v": {"multipleOf": 0.1}}});
    assert!(!tenth.is_valid(&doc! {"v": 0.3}));
    assert!(!tenth.is_valid(&doc! {"v": 0.5}));
    let quarter = schema(doc! {"properties": {"v": {"multipleOf": 0.25}}});
    assert!(quarter.is_valid(&doc! {"v": 0.75}));
    let int_divisor = schema(doc! {"properties": {"v": {"multipleOf": 2}}});
    assert!(int_divisor.is_valid(&doc! {"v": 4.0}));
    assert!(!int_divisor.is_valid(&doc! {"v": 4.5}));
    for odd in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(!half.is_valid(&doc! {"v": odd}), "{odd}");
    }
}

// -- strings -----------------------------------------------------------------

#[test]
fn length_counts_characters() {
    let p = |k: Document| doc! {"properties": {"v": k}};
    check(
        p(doc! {"minLength": 2}),
        &[doc! {"v": "ab"}, doc! {"v": "abc"}, doc! {"v": "éé"}, doc! {"v": 1}, doc! {}],
        &[doc! {"v": "a"}, doc! {"v": ""}],
    );
    check(
        p(doc! {"maxLength": 2}),
        &[doc! {"v": "ab"}, doc! {"v": ""}, doc! {"v": "éé"}, doc! {"v": "😀😀"}],
        &[doc! {"v": "abc"}, doc! {"v": "😀😀😀"}],
    );
    check(p(doc! {"minLength": 0, "maxLength": 0}), &[doc! {"v": ""}], &[doc! {"v": "a"}]);
}

#[test]
fn pattern_is_a_search_not_a_full_match() {
    let p = |pat: &str| doc! {"properties": {"v": {"pattern": pat}}};
    check(p("b"), &[doc! {"v": "abc"}, doc! {"v": "b"}, doc! {"v": 5}], &[doc! {"v": "xyz"}]);
    check(p("^b"), &[doc! {"v": "bc"}], &[doc! {"v": "abc"}]);
    check(p("c$"), &[doc! {"v": "abc"}], &[doc! {"v": "cab"}]);
    check(p("^[a-c]+$"), &[doc! {"v": "abc"}], &[doc! {"v": "abd"}, doc! {"v": ""}]);
}

// -- arrays ------------------------------------------------------------------

#[test]
fn item_counts() {
    let p = |k: Document| doc! {"properties": {"v": k}};
    check(
        p(doc! {"minItems": 2}),
        &[doc! {"v": [1, 2]}, doc! {"v": [1, 2, 3]}, doc! {"v": "a"}, doc! {}],
        &[doc! {"v": [1]}, doc! {"v": []}],
    );
    check(p(doc! {"maxItems": 2}), &[doc! {"v": [1, 2]}, doc! {"v": []}], &[doc! {"v": [1, 2, 3]}]);
}

#[test]
fn items_as_one_schema_tests_every_element() {
    check(
        doc! {"properties": {"v": {"items": {"bsonType": "int"}}}},
        &[doc! {"v": [1, 2]}, doc! {"v": []}, doc! {"v": "x"}],
        &[doc! {"v": [1, "a"]}, doc! {"v": [1.5]}],
    );
}

#[test]
fn items_as_an_array_tests_by_position_and_additional_items_the_rest() {
    let items = || bson::bson!([{"bsonType": "int"}, {"bsonType": "string"}]);
    // additionalItems unset: the rest are free.
    check(
        doc! {"properties": {"v": {"items": items()}}},
        &[doc! {"v": [1, "a", true, null]}, doc! {"v": [1]}, doc! {"v": []}],
        &[doc! {"v": ["a", "a"]}, doc! {"v": [1, 2]}],
    );
    check(
        doc! {"properties": {"v": {"items": items(), "additionalItems": false}}},
        &[doc! {"v": [1, "a"]}, doc! {"v": [1]}],
        &[doc! {"v": [1, "a", 3]}],
    );
    check(
        doc! {"properties": {"v": {"items": items(), "additionalItems": {"bsonType": "bool"}}}},
        &[doc! {"v": [1, "a", true, false]}, doc! {"v": [1, "a"]}],
        &[doc! {"v": [1, "a", true, 3]}, doc! {"v": [1, "a", 3]}],
    );
    // additionalItems means nothing beside a single `items` schema.
    check(
        doc! {"properties": {"v": {"items": {"bsonType": "int"}, "additionalItems": false}}},
        &[doc! {"v": [1, 2, 3]}],
        &[],
    );
    check(doc! {"properties": {"v": {"additionalItems": false}}}, &[doc! {"v": [1, 2]}], &[]);
}

#[test]
fn unique_items_uses_the_total_order() {
    let s = doc! {"properties": {"v": {"uniqueItems": true}}};
    check(
        s.clone(),
        &[
            doc! {"v": []},
            doc! {"v": [1]},
            doc! {"v": [1, 2, 3]},
            doc! {"v": [1, "1"]},
            doc! {"v": [[1], [2]]},
            doc! {"v": [{"a": 1}, {"a": 2}]},
            doc! {"v": "x"},
        ],
        &[
            doc! {"v": [1, 1]},
            doc! {"v": [1, 2, 1]},
            doc! {"v": [1, 1.0]},
            doc! {"v": [1, dec("1.0")]},
            doc! {"v": [[1], [1.0]]},
            doc! {"v": [{"a": 1}, {"a": 1}]},
            doc! {"v": [Bson::Null, Bson::Null]},
            doc! {"v": ["a", "b", "c", "a"]},
        ],
    );
    check(doc! {"properties": {"v": {"uniqueItems": false}}}, &[doc! {"v": [1, 1]}], &[]);
    // Many items, the duplicate far from its twin.
    let many: Vec<Bson> = (0..5000).map(Bson::Int32).chain([Bson::Int32(17)]).collect();
    assert!(!schema(s.clone()).is_valid(&doc! {"v": many}));
    let distinct: Vec<Bson> = (0..5000).map(Bson::Int32).collect();
    assert!(schema(s).is_valid(&doc! {"v": distinct}));
}

// -- objects -----------------------------------------------------------------

#[test]
fn required_names_properties_that_must_be_present() {
    check(
        doc! {"required": ["a", "b"]},
        &[doc! {"a": 1, "b": 2}, doc! {"a": Bson::Null, "b": 1, "c": 3}],
        &[doc! {"a": 1}, doc! {}, doc! {"c": 1}],
    );
    // Nested, and beside a type that is not an object.
    check(
        doc! {"properties": {"v": {"required": ["a"]}}},
        &[doc! {"v": {"a": 1}}, doc! {"v": 5}, doc! {}],
        &[doc! {"v": {}}],
    );
}

#[test]
fn properties_test_only_the_properties_that_are_there() {
    check(
        doc! {"properties": {"a": {"bsonType": "int"}, "b": {"bsonType": "string"}}},
        &[doc! {"a": 1, "b": "x"}, doc! {"a": 1}, doc! {}, doc! {"c": [1]}],
        &[doc! {"a": "x"}, doc! {"a": 1, "b": 2}],
    );
    // Nested objects.
    check(
        doc! {"properties": {"a": {"properties": {"b": {"properties": {"c": {"bsonType": "int"}}}}}}},
        &[doc! {"a": {"b": {"c": 1}}}, doc! {"a": {"b": {}}}, doc! {"a": {}}],
        &[doc! {"a": {"b": {"c": "x"}}}],
    );
}

#[test]
fn additional_properties_as_false_allows_only_the_named() {
    check(
        doc! {"properties": {"a": {}}, "additionalProperties": false},
        &[doc! {"a": 1}, doc! {}],
        &[doc! {"a": 1, "b": 2}, doc! {"b": 2}],
    );
    check(
        doc! {"properties": {"a": {}}, "additionalProperties": true},
        &[doc! {"a": 1, "b": 2}],
        &[],
    );
    // `{additionalProperties: false}` alone is an empty object.
    check(doc! {"additionalProperties": false}, &[doc! {}], &[doc! {"a": 1}]);
}

#[test]
fn additional_properties_as_a_schema_tests_the_rest() {
    check(
        doc! {"properties": {"a": {"bsonType": "string"}}, "additionalProperties": {"bsonType": "int"}},
        &[doc! {"a": "x", "b": 1, "c": 2}, doc! {"a": "x"}],
        &[doc! {"a": "x", "b": "no"}, doc! {"a": 1}],
    );
}

#[test]
fn pattern_properties_test_the_keys_they_match() {
    check(
        doc! {"patternProperties": {"^n_": {"bsonType": "int"}, "_s$": {"bsonType": "string"}}},
        &[doc! {"n_a": 1, "x_s": "t"}, doc! {"other": [1]}],
        &[doc! {"n_a": "x"}, doc! {"x_s": 1}],
    );
    // A key matching both patterns answers to both.
    check(
        doc! {"patternProperties": {"^n_": {"bsonType": "int"}, "_s$": {"bsonType": "string"}}},
        &[],
        &[doc! {"n_s": 1}, doc! {"n_s": "x"}],
    );
    // Together with additionalProperties: it governs what neither names nor matches.
    check(
        doc! {
            "properties": {"id": {}},
            "patternProperties": {"^x": {"bsonType": "int"}},
            "additionalProperties": false
        },
        &[doc! {"id": 1, "x1": 2}],
        &[doc! {"id": 1, "y": 2}, doc! {"x1": "a"}],
    );
    // A property named in `properties` that a pattern matches answers to both.
    check(
        doc! {"properties": {"xa": {"bsonType": "int"}}, "patternProperties": {"^x": {"minimum": 5}}},
        &[doc! {"xa": 6}],
        &[doc! {"xa": 1}, doc! {"xa": "s"}],
    );
}

#[test]
fn property_counts() {
    check(
        doc! {"minProperties": 2},
        &[doc! {"a": 1, "b": 2}, doc! {"a": 1, "b": 2, "c": 3}],
        &[doc! {"a": 1}, doc! {}],
    );
    check(doc! {"maxProperties": 1}, &[doc! {"a": 1}, doc! {}], &[doc! {"a": 1, "b": 2}]);
    check(
        doc! {"properties": {"v": {"maxProperties": 1}}},
        &[doc! {"v": {"a": 1}}, doc! {"v": "string"}],
        &[doc! {"v": {"a": 1, "b": 2}}],
    );
}

#[test]
fn dependencies_in_both_forms() {
    check(
        doc! {"dependencies": {"card": ["billing", "zip"]}},
        &[doc! {}, doc! {"billing": 1}, doc! {"card": 1, "billing": 2, "zip": 3}],
        &[doc! {"card": 1}, doc! {"card": 1, "billing": 2}],
    );
    check(
        doc! {"dependencies": {"card": {"required": ["billing"], "properties": {"billing": {"bsonType": "string"}}}}},
        &[doc! {}, doc! {"other": 1}, doc! {"card": 1, "billing": "addr"}],
        &[doc! {"card": 1}, doc! {"card": 1, "billing": 5}],
    );
    // A dependency on a value that is not an object is ignored.
    check(
        doc! {"properties": {"v": {"dependencies": {"a": ["b"]}}}},
        &[doc! {"v": 5}, doc! {"v": {"a": 1, "b": 2}}],
        &[doc! {"v": {"a": 1}}],
    );
}

// -- combinators -------------------------------------------------------------

#[test]
fn all_of_any_of_one_of_and_not() {
    let a = || bson::bson!({"required": ["a"]});
    let b = || bson::bson!({"required": ["b"]});
    check(
        doc! {"allOf": [a(), b()]},
        &[doc! {"a": 1, "b": 1}],
        &[doc! {"a": 1}, doc! {"b": 1}, doc! {}],
    );
    check(
        doc! {"anyOf": [a(), b()]},
        &[doc! {"a": 1}, doc! {"b": 1}, doc! {"a": 1, "b": 1}],
        &[doc! {}, doc! {"c": 1}],
    );
    // Exactly one.
    check(
        doc! {"oneOf": [a(), b()]},
        &[doc! {"a": 1}, doc! {"b": 1}],
        &[doc! {"a": 1, "b": 1}, doc! {}],
    );
    check(doc! {"not": a()}, &[doc! {}, doc! {"b": 1}], &[doc! {"a": 1}]);
    // Nested: not inside oneOf inside allOf.
    check(
        doc! {"allOf": [{"oneOf": [{"not": a()}, b()]}]},
        &[doc! {"c": 1}, doc! {"a": 1, "b": 1}],
        &[],
    );
    check(
        doc! {"properties": {"v": {"oneOf": [{"bsonType": "int"}, {"minimum": 5}]}}},
        &[doc! {"v": 1}, doc! {"v": 7.5}],
        &[doc! {"v": 7}],
    );
}

// -- parse refusals ----------------------------------------------------------

#[test]
fn unsupported_keywords_are_refused_by_name() {
    for keyword in [
        "$ref",
        "$schema",
        "default",
        "definitions",
        "format",
        "id",
        "contentMediaType",
        "const",
        "contains",
        "if",
        "propertyNames",
        "examples",
        "somethingMadeUp",
    ] {
        let message = refusal(doc! {keyword: "x"});
        assert!(message.contains(&format!("{keyword:?}")), "{keyword}: {message}");
        assert!(message.contains("not supported"), "{keyword}: {message}");
    }
}

#[test]
fn an_unsupported_keyword_is_refused_wherever_it_is_nested() {
    let nested = [
        doc! {"properties": {"a": {"format": "email"}}},
        doc! {"patternProperties": {"^a": {"$ref": "#/x"}}},
        doc! {"additionalProperties": {"default": 1}},
        doc! {"items": {"id": "x"}},
        doc! {"items": [{"bsonType": "int"}, {"definitions": {}}]},
        doc! {"additionalItems": {"format": "x"}},
        doc! {"allOf": [{"bsonType": "object"}, {"$schema": "x"}]},
        doc! {"anyOf": [{"const": 1}]},
        doc! {"oneOf": [{"bsonType": "object"}, {"properties": {"a": {"nope": 1}}}]},
        doc! {"not": {"format": "x"}},
        doc! {"dependencies": {"a": {"format": "x"}}},
    ];
    for schema in nested {
        let message = refusal(schema.clone());
        assert!(message.contains("not supported"), "{schema}: {message}");
        assert!(message.contains(" at "), "the place is named: {message}");
    }
    // A property or a pattern named like a keyword is a name, not a keyword.
    schema(doc! {"properties": {"$ref": {"bsonType": "int"}, "format": {}, "default": {}}});
    schema(doc! {"dependencies": {"$schema": ["id"]}});
    schema(doc! {"enum": [{"$ref": 1}, {"format": "x"}]});
}

#[test]
fn the_place_of_a_refusal_is_named() {
    let m = refusal(doc! {"properties": {"a": {"items": {"minLength": -1}}}});
    assert!(m.contains("properties.a.items") && m.contains("minLength"), "{m}");
}

#[test]
fn a_schema_must_be_a_document() {
    assert!(Schema::parse(&Bson::Int32(1)).is_err());
    assert!(Schema::parse(&Bson::Array(vec![])).is_err());
    assert!(Schema::parse(&Bson::Null).is_err());
    assert!(refusal(doc! {"properties": {"a": 5}}).contains("a schema must be a document"));
    assert!(refusal(doc! {"not": [1]}).contains("a schema must be a document"));
    assert!(refusal(doc! {"items": 5}).contains("items"));
}

#[test]
fn every_keyword_argument_is_checked() {
    let bad: Vec<(&str, Bson)> = vec![
        ("bsonType", "madeup".into()),
        ("bsonType", 5.into()),
        ("bsonType", Bson::Array(vec![])),
        ("bsonType", bson::bson!(["int", 5])),
        ("bsonType", "integer".into()),
        ("type", "int".into()),
        ("type", "integer".into()),
        ("type", "madeup".into()),
        ("type", Bson::Array(vec![])),
        ("type", 5.into()),
        ("required", Bson::Array(vec![])),
        ("required", "a".into()),
        ("required", bson::bson!(["a", "a"])),
        ("required", bson::bson!(["a", 1])),
        ("properties", Bson::Array(vec![])),
        ("properties", "a".into()),
        ("additionalProperties", 5.into()),
        ("additionalProperties", "x".into()),
        ("patternProperties", true.into()),
        ("patternProperties", bson::bson!({"(": {}})),
        ("enum", Bson::Array(vec![])),
        ("enum", "a".into()),
        ("minimum", "a".into()),
        ("minimum", Bson::Null),
        ("maximum", true.into()),
        ("minimum", f64::NAN.into()),
        ("exclusiveMinimum", 1.into()),
        ("exclusiveMaximum", "true".into()),
        ("multipleOf", 0.into()),
        ("multipleOf", (-1).into()),
        ("multipleOf", 0.0.into()),
        ("multipleOf", "a".into()),
        ("multipleOf", f64::NAN.into()),
        ("multipleOf", f64::INFINITY.into()),
        ("multipleOf", dec("0")),
        ("multipleOf", dec("-1")),
        ("multipleOf", dec("NaN")),
        ("multipleOf", dec("Infinity")),
        ("minLength", (-1).into()),
        ("minLength", 1.5.into()),
        ("minLength", "1".into()),
        ("minLength", f64::NAN.into()),
        ("maxLength", (-1).into()),
        ("pattern", 5.into()),
        ("pattern", "(".into()),
        ("pattern", "(?=a)".into()),
        ("pattern", "(a)\\1".into()),
        ("items", "a".into()),
        ("items", bson::bson!([1])),
        ("additionalItems", 1.into()),
        ("minItems", (-1).into()),
        ("maxItems", 2.5.into()),
        ("uniqueItems", 1.into()),
        ("uniqueItems", "true".into()),
        ("minProperties", (-1).into()),
        ("maxProperties", "a".into()),
        ("dependencies", 5.into()),
        ("dependencies", bson::bson!({"a": 5})),
        ("dependencies", bson::bson!({"a": []})),
        ("dependencies", bson::bson!({"a": [1]})),
        ("dependencies", bson::bson!({"a": ["b", "b"]})),
        ("allOf", Bson::Array(vec![])),
        ("allOf", "a".into()),
        ("anyOf", Bson::Array(vec![])),
        ("anyOf", bson::bson!([1])),
        ("oneOf", Bson::Array(vec![])),
        ("oneOf", bson::bson!({})),
        ("not", "a".into()),
        ("not", Bson::Array(vec![])),
        ("title", 5.into()),
        ("description", true.into()),
    ];
    for (keyword, argument) in bad {
        let message = refusal(doc! {keyword: argument.clone()});
        assert!(message.contains(keyword), "{keyword}: {argument}: {message}");
    }
    // Their good forms parse.
    for (keyword, argument) in [
        ("bsonType", Bson::from("number")),
        ("type", "number".into()),
        ("required", bson::bson!(["a"])),
        ("minimum", dec("1.5")),
        ("multipleOf", dec("0.01")),
        ("multipleOf", 0.5.into()),
        ("minLength", 2.0.into()),
        ("minLength", dec("2")),
        ("minLength", 0_i64.into()),
        ("items", bson::bson!([])),
        ("title", "t".into()),
        ("description", "d".into()),
        ("dependencies", bson::bson!({"a": ["b"], "c": {}})),
    ] {
        schema(doc! {keyword: argument});
    }
}

#[test]
fn an_exclusive_flag_needs_its_bound() {
    assert!(refusal(doc! {"exclusiveMinimum": true}).contains("exclusiveMinimum"));
    assert!(refusal(doc! {"maximum": 5, "exclusiveMinimum": true}).contains("minimum"));
    assert!(refusal(doc! {"exclusiveMaximum": false}).contains("exclusiveMaximum"));
    schema(doc! {"exclusiveMinimum": true, "minimum": 1});
}

#[test]
fn the_bson_type_of_an_unknown_name_is_refused_with_the_names() {
    let m = refusal(doc! {"bsonType": "integer"});
    assert!(m.contains("integer") && m.contains("int") && m.contains("decimal"), "{m}");
}

#[test]
fn schemas_nest_at_most_the_depth_bound() {
    fn nested(levels: usize) -> Document {
        let mut s = doc! {"bsonType": "int"};
        for _ in 1..levels {
            s = doc! {"properties": {"a": s}};
        }
        s
    }
    schema(nested(MAX_DEPTH));
    let m = refusal(nested(MAX_DEPTH + 1));
    assert!(m.contains("32"), "{m}");
    // The other ways down count as well.
    let mut via_not = doc! {"bsonType": "int"};
    for _ in 1..MAX_DEPTH {
        via_not = doc! {"not": via_not};
    }
    schema(via_not.clone());
    refusal(doc! {"not": via_not});
    let mut via_items = doc! {};
    for _ in 1..=MAX_DEPTH {
        via_items = doc! {"items": [via_items]};
    }
    refusal(via_items);
    let mut via_all = doc! {};
    for _ in 1..=MAX_DEPTH {
        via_all = doc! {"allOf": [via_all]};
    }
    refusal(via_all);
    // A deep schema accepts documents without recursing into the document.
    let deep = schema(nested(MAX_DEPTH));
    let mut value = Bson::Int32(1);
    for _ in 0..(MAX_DEPTH - 1) {
        value = Bson::Document(doc! {"a": value});
    }
    assert!(deep.is_valid(&value.as_document().unwrap().clone()));
}

#[test]
fn a_document_nested_deeper_than_the_schema_is_not_walked() {
    let s = schema(doc! {"properties": {"a": {"bsonType": "object"}}});
    let mut value = Bson::Int32(1);
    for _ in 0..200 {
        value = Bson::Document(doc! {"a": value});
    }
    assert!(s.is_valid(value.as_document().unwrap()));
}

// -- regex limits ------------------------------------------------------------

#[test]
fn patterns_share_the_regex_limits() {
    // One pattern over the size limit.
    let big = "\\w{209}";
    let m = refusal(doc! {"pattern": big});
    assert!(m.contains("pattern") && m.contains("MiB"), "{m}");
    let m = refusal(doc! {"patternProperties": {big: {}}});
    assert!(m.contains("patternProperties"), "{m}");
    // More patterns than a filter may hold.
    let mut many = Document::new();
    for i in 0..33 {
        many.insert(format!("^p{i}$"), doc! {});
    }
    let m = refusal(doc! {"patternProperties": many.clone()});
    assert!(m.contains("32"), "{m}");
    many.remove("^p0$");
    schema(doc! {"patternProperties": many});
    // And a filter's budget is the one they are charged to.
    let mut filter = Vec::new();
    for _ in 0..31 {
        filter.push(Bson::Document(doc! {"s": {"$regex": "a"}}));
    }
    let too_many = doc! {"$and": filter, "$jsonSchema": {"patternProperties": {"a": {}, "b": {}}}};
    assert!(crate::filter::parse(&too_many).is_err());
}

// -- reasons -----------------------------------------------------------------

#[test]
fn explain_names_the_failing_keyword_and_where() {
    let cases: Vec<(Document, Document, &[&str])> = vec![
        (doc! {"bsonType": "string"}, doc! {}, &["(document)", "bsonType", "string", "object"]),
        (
            doc! {"properties": {"a": {"type": "string"}}},
            doc! {"a": 1},
            &["a:", "type", "string", "int"],
        ),
        (doc! {"properties": {"a": {"enum": [1, 2]}}}, doc! {"a": 3}, &["a:", "enum"]),
        (
            doc! {"allOf": [{"required": ["a"]}, {"required": ["b"]}]},
            doc! {"a": 1},
            &["required", "\"b\""],
        ),
        (doc! {"anyOf": [{"required": ["a"]}, {"required": ["b"]}]}, doc! {}, &["anyOf"]),
        (doc! {"oneOf": [{"required": ["a"]}, {"required": ["b"]}]}, doc! {}, &["oneOf", "none"]),
        (
            doc! {"oneOf": [{"required": ["a"]}, {"required": ["b"]}]},
            doc! {"a": 1, "b": 1},
            &["oneOf", "more than one"],
        ),
        (doc! {"not": {"required": ["a"]}}, doc! {"a": 1}, &["not"]),
        (doc! {"required": ["email", "age"]}, doc! {"email": "x"}, &["required", "\"age\""]),
        (
            doc! {"properties": {"age": {"minimum": 18}}},
            doc! {"age": 17},
            &["age:", "minimum", "17", "18"],
        ),
        (
            doc! {"properties": {"age": {"minimum": 18, "exclusiveMinimum": true}}},
            doc! {"age": 18},
            &["age:", "minimum", "exclusive"],
        ),
        (
            doc! {"properties": {"age": {"maximum": 5}}},
            doc! {"age": 6},
            &["age:", "maximum", "6", "5"],
        ),
        (
            doc! {"properties": {"age": {"maximum": 5, "exclusiveMaximum": true}}},
            doc! {"age": 5},
            &["maximum", "exclusive"],
        ),
        (
            doc! {"properties": {"n": {"multipleOf": 3}}},
            doc! {"n": 10},
            &["n:", "multipleOf", "10", "3"],
        ),
        (
            doc! {"properties": {"s": {"minLength": 3}}},
            doc! {"s": "ab"},
            &["s:", "minLength", "2", "3"],
        ),
        (doc! {"properties": {"s": {"maxLength": 1}}}, doc! {"s": "ab"}, &["s:", "maxLength"]),
        (doc! {"properties": {"s": {"pattern": "^a"}}}, doc! {"s": "ba"}, &["s:", "pattern", "^a"]),
        (doc! {"properties": {"t": {"minItems": 2}}}, doc! {"t": [1]}, &["t:", "minItems"]),
        (doc! {"properties": {"t": {"maxItems": 1}}}, doc! {"t": [1, 2]}, &["t:", "maxItems"]),
        (
            doc! {"properties": {"t": {"uniqueItems": true}}},
            doc! {"t": [1, 1.0]},
            &["t:", "uniqueItems"],
        ),
        (
            doc! {"properties": {"t": {"items": {"bsonType": "int"}}}},
            doc! {"t": [1, "a"]},
            &["t.1:", "bsonType"],
        ),
        (
            doc! {"properties": {"t": {"items": [{}], "additionalItems": false}}},
            doc! {"t": [1, 2]},
            &["t:", "additionalItems"],
        ),
        (
            doc! {"properties": {"t": {"items": [{}], "additionalItems": {"bsonType": "string"}}}},
            doc! {"t": [1, 2]},
            &["t.1:", "bsonType"],
        ),
        (doc! {"minProperties": 2}, doc! {"a": 1}, &["(document)", "minProperties"]),
        (doc! {"maxProperties": 1}, doc! {"a": 1, "b": 1}, &["maxProperties"]),
        (
            doc! {"additionalProperties": false, "properties": {"a": {}}},
            doc! {"a": 1, "zz": 1},
            &["additionalProperties", "\"zz\""],
        ),
        (
            doc! {"additionalProperties": {"bsonType": "int"}},
            doc! {"zz": "s"},
            &["zz:", "bsonType"],
        ),
        (
            doc! {"patternProperties": {"^x": {"bsonType": "int"}}},
            doc! {"xa": "s"},
            &["xa:", "bsonType"],
        ),
        (doc! {"dependencies": {"a": ["b"]}}, doc! {"a": 1}, &["dependencies", "\"a\"", "\"b\""]),
        (doc! {"dependencies": {"a": {"required": ["b"]}}}, doc! {"a": 1}, &["required", "\"b\""]),
        (
            doc! {"properties": {"o": {"properties": {"p": {"minimum": 1}}}}},
            doc! {"o": {"p": 0}},
            &["o.p:", "minimum"],
        ),
    ];
    for (s, d, expected) in cases {
        let r = reason(s.clone(), d.clone());
        for piece in expected {
            assert!(r.contains(piece), "{s} against {d}: {r:?} lacks {piece:?}");
        }
    }
}

#[test]
fn explain_reports_the_first_failure_in_a_fixed_order() {
    // required before properties, properties in the schema's order.
    let s = doc! {
        "required": ["a"],
        "properties": {"b": {"bsonType": "int"}, "c": {"bsonType": "int"}}
    };
    assert!(reason(s.clone(), doc! {"b": "x", "c": "y"}).contains("required"));
    assert!(reason(s.clone(), doc! {"a": 1, "c": "y", "b": "x"}).starts_with("b:"));
    // A failing alternative of anyOf does not become the reason.
    let any = doc! {"anyOf": [{"bsonType": "string"}, {"required": ["q"]}]};
    assert_eq!(reason(any, doc! {}), "(document): anyOf matches none of its schemas");
}

#[test]
fn a_valid_document_has_no_reason() {
    let s = schema(doc! {"required": ["a"]});
    assert_eq!(s.explain_failure(&doc! {"a": 1}), None);
}

// -- the owner's example -----------------------------------------------------

#[test]
fn the_documented_example() {
    let s = doc! {
        "required": ["email", "age"],
        "properties": {
            "email": {"bsonType": "string", "pattern": "@"},
            "age": {"bsonType": "int", "minimum": 0, "maximum": 150}
        }
    };
    check(
        s,
        &[doc! {"email": "a@b.c", "age": 30}],
        &[
            doc! {"email": "a@b.c"},
            doc! {"email": "nope", "age": 30},
            doc! {"email": "a@b.c", "age": -1},
            doc! {"email": "a@b.c", "age": 30.5},
        ],
    );
}

#[test]
fn schemas_compare_and_clone_cheaply() {
    let a = schema(doc! {"required": ["a"]});
    let b = schema(doc! {"required": ["a"]});
    assert_eq!(a, b);
    assert_ne!(a, schema(doc! {"required": ["b"]}));
    assert_eq!(a.clone(), a);
    assert_eq!(a.document(), &doc! {"required": ["a"]});
}

// -- no panics ---------------------------------------------------------------

fn any_bson() -> impl Strategy<Value = Bson> {
    let leaf = prop_oneof![
        Just(Bson::Null),
        any::<bool>().prop_map(Bson::Boolean),
        any::<i32>().prop_map(Bson::Int32),
        any::<i64>().prop_map(Bson::Int64),
        any::<f64>().prop_map(Bson::Double),
        any::<u128>().prop_map(|bits| Bson::Decimal128(Decimal128::from_bytes(bits.to_le_bytes()))),
        "[a-z@]{0,6}".prop_map(Bson::String),
        any::<i64>().prop_map(|ms| Bson::DateTime(bson::DateTime::from_millis(ms))),
    ];
    leaf.prop_recursive(4, 48, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..6).prop_map(Bson::Array),
            prop::collection::vec(("[a-d]{1,2}", inner), 0..5).prop_map(|entries| {
                let mut d = Document::new();
                for (k, v) in entries {
                    d.insert(k, v);
                }
                Bson::Document(d)
            }),
        ]
    })
}

fn rich_schema() -> Schema {
    schema(doc! {
        "bsonType": "object",
        "required": ["a"],
        "minProperties": 1,
        "maxProperties": 6,
        "properties": {
            "a": {"bsonType": ["int", "double", "decimal", "long"], "minimum": -5, "maximum": 100,
                  "multipleOf": 0.5, "enum": [1, 2.5, {"x": 1}]},
            "b": {"type": "array", "uniqueItems": true, "minItems": 1, "maxItems": 4,
                  "items": [{"bsonType": "string", "pattern": "^a"}, {"multipleOf": dec("0.1")}],
                  "additionalItems": {"not": {"bsonType": "null"}}},
            "c": {"type": "string", "minLength": 1, "maxLength": 3},
            "d": {"oneOf": [{"bsonType": "object"}, {"bsonType": "array"}],
                  "properties": {"a": {"anyOf": [{"type": "number"}, {"type": "string"}]}},
                  "additionalProperties": {"allOf": [{}]}},
        },
        "patternProperties": {"^a.": {"not": {"bsonType": "string"}}},
        "dependencies": {"b": ["c"], "c": {"required": ["a"]}},
        "additionalProperties": true
    })
}

proptest! {
    #[test]
    fn random_documents_never_panic(entries in prop::collection::vec(("[a-d]{1,2}", any_bson()), 0..6)) {
        let mut d = Document::new();
        for (k, v) in entries {
            d.insert(k, v);
        }
        let s = rich_schema();
        let valid = s.is_valid(&d);
        prop_assert_eq!(s.explain_failure(&d).is_none(), valid);
    }

    #[test]
    fn a_number_is_a_multiple_exactly_when_the_integers_say_so(v in -1000i64..1000, d in 1i64..40) {
        let s = schema(doc! {"properties": {"n": {"multipleOf": d}}});
        let decimal = schema(doc! {"properties": {"n": {"multipleOf": dec(&d.to_string())}}});
        let expected = v % d == 0;
        prop_assert_eq!(s.is_valid(&doc! {"n": v}), expected);
        prop_assert_eq!(s.is_valid(&doc! {"n": dec(&v.to_string())}), expected);
        prop_assert_eq!(decimal.is_valid(&doc! {"n": v}), expected);
        prop_assert_eq!(decimal.is_valid(&doc! {"n": dec(&format!("{v}.000"))}), expected);
    }

    #[test]
    fn decimal_multiples_agree_with_scaled_integers(v in -100_000i64..100_000, d in 1i64..500) {
        // v hundredths against d hundredths is v against d.
        let s = schema(doc! {"properties": {"n": {"multipleOf": dec(&format!("{d}E-2"))}}});
        prop_assert_eq!(s.is_valid(&doc! {"n": dec(&format!("{v}E-2"))}), v % d == 0);
    }
}
