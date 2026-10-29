//! `$merge` and `$out` (ADR-255): what parses, and what is refused.

use bson::{Document, doc};

use crate::aggregate::{self, Limits, MergeSpec, Stage, Target, WhenMatched};

fn refused(pipeline: &[Document]) -> String {
    aggregate::parse(pipeline).expect_err("must be refused").to_string()
}

fn merge(spec: Document) -> MergeSpec {
    match aggregate::parse(&[doc! {"$merge": spec}]).unwrap().pop() {
        Some(Stage::Merge(spec)) => *spec,
        other => panic!("not a $merge: {other:?}"),
    }
}

fn into(coll: &str) -> Target {
    Target { db: None, coll: coll.into() }
}

#[test]
fn a_merge_with_only_a_target_takes_the_defaults() {
    let spec = merge(doc! {"into": "daily"});
    assert_eq!(
        spec,
        MergeSpec { into: into("daily"), when_matched: WhenMatched::Merge, insert_unmatched: true }
    );
}

#[test]
fn every_when_matched_and_when_not_matched_form_parses() {
    for (name, mode) in [
        ("replace", WhenMatched::Replace),
        ("merge", WhenMatched::Merge),
        ("keepExisting", WhenMatched::KeepExisting),
    ] {
        for (not, insert) in [("insert", true), ("discard", false)] {
            let spec = merge(doc! {
                "into": "t", "on": "_id", "whenMatched": name, "whenNotMatched": not
            });
            assert_eq!(spec.when_matched, mode);
            assert_eq!(spec.insert_unmatched, insert);
        }
    }
}

#[test]
fn a_target_in_another_database_parses_with_its_name() {
    let spec = merge(doc! {"into": {"db": "reports", "coll": "daily"}});
    assert_eq!(spec.into, Target { db: Some("reports".into()), coll: "daily".into() });
    let out = aggregate::parse(&[doc! {"$out": {"db": "reports", "coll": "snap"}}]).unwrap();
    assert!(matches!(&out[0], Stage::Out(t) if t.db.as_deref() == Some("reports")));
}

#[test]
fn a_merge_on_any_key_but_id_is_refused_with_the_reason() {
    for on in
        [bson::Bson::from("sku"), bson::Bson::from(vec!["_id"]), bson::Bson::from(vec!["a", "b"])]
    {
        let err = refused(&[doc! {"$merge": {"into": "t", "on": on}}]);
        assert!(err.contains("`on` can only be \"_id\""), "{err}");
        assert!(err.contains("members"), "the reason is the concurrent members: {err}");
    }
}

#[test]
fn the_refused_modes_each_name_what_is_refused() {
    let cases = [
        (doc! {"into": "t", "whenMatched": "fail"}, "whenMatched \"fail\""),
        (doc! {"into": "t", "whenNotMatched": "fail"}, "whenNotMatched \"fail\""),
        (
            doc! {"into": "t", "whenMatched": [{"$set": {"a": 1}}]},
            "pipeline-form $merge `whenMatched`",
        ),
        (doc! {"into": "t", "let": {"a": 1}}, "`let` is not supported"),
    ];
    for (spec, needle) in cases {
        let err = refused(&[doc! {"$merge": spec.clone()}]);
        assert!(err.contains(needle), "{spec}: {err}");
        assert!(err.contains("not supported"), "{spec}: {err}");
    }
}

#[test]
fn malformed_merge_specs_are_refused() {
    for spec in [
        doc! {},
        doc! {"into": 5},
        doc! {"into": ""},
        doc! {"into": {"coll": "t"}},
        doc! {"into": {"db": "d", "coll": "t", "extra": 1}},
        doc! {"into": "t", "whenMatched": "sometimes"},
        doc! {"into": "t", "whenMatched": 1},
        doc! {"into": "t", "whenNotMatched": "sometimes"},
        doc! {"into": "t", "whenNotMatched": true},
        doc! {"into": "t", "typo": 1},
    ] {
        aggregate::parse(&[doc! {"$merge": spec.clone()}]).expect_err(&spec.to_string());
    }
    assert!(aggregate::parse(&[doc! {"$merge": "t"}]).is_err(), "the string form is a $merge");
}

#[test]
fn out_takes_a_name_or_a_target_and_refuses_the_timeseries_forms_by_name() {
    let stages = aggregate::parse(&[doc! {"$out": "snapshot"}]).unwrap();
    assert!(matches!(&stages[0], Stage::Out(t) if *t == into("snapshot")));
    let err = refused(&[doc! {"$out": {"db": "d", "coll": "c", "timeseries": {"timeField": "t"}}}]);
    assert!(err.contains("`timeseries`") && err.contains("not supported"), "{err}");
    for bad in [doc! {"$out": 5}, doc! {"$out": ""}, doc! {"$out": {"db": "d"}}] {
        aggregate::parse(std::slice::from_ref(&bad)).expect_err(&bad.to_string());
    }
}

#[test]
fn a_write_stage_is_only_ever_last() {
    for name in ["$merge", "$out"] {
        let stage = if name == "$merge" {
            doc! {name: {"into": "t"}}
        } else {
            doc! {name: "t"}
        };
        let err = refused(&[stage.clone(), doc! {"$limit": 1}]);
        assert!(err.contains(name) && err.contains("last stage"), "{err}");
        aggregate::parse(&[doc! {"$limit": 1}, stage.clone()]).unwrap();
        aggregate::parse(&[stage]).unwrap();
    }
    // Two writes: the first is not last.
    let err = refused(&[doc! {"$out": "a"}, doc! {"$out": "b"}]);
    assert!(err.contains("$out must be the last stage"), "{err}");
}

#[test]
fn a_write_stage_is_refused_inside_a_facet_and_a_lookup_sub_pipeline() {
    for stage in [doc! {"$merge": {"into": "t"}}, doc! {"$out": "t"}] {
        let facet = refused(&[doc! {"$facet": {"a": [stage.clone()]}}]);
        assert!(facet.contains("cannot run inside a $facet"), "{facet}");
        let lookup = refused(&[doc! {"$lookup": {
            "from": "x", "as": "j", "pipeline": [stage.clone()]
        }}]);
        assert!(lookup.contains("cannot run inside a $lookup sub-pipeline"), "{lookup}");
        let nested = refused(&[doc! {"$facet": {"a": [doc! {"$lookup": {
            "from": "x", "as": "j", "pipeline": [stage.clone()]
        }}]}}]);
        assert!(nested.contains("$facet"), "{nested}");
    }
}

#[test]
fn the_pure_pipeline_will_not_run_a_write() {
    let stages = aggregate::parse(&[doc! {"$out": "t"}]).unwrap();
    assert!(stages[0].needs_storage() && stages[0].writes());
    let err = aggregate::apply(&stages[0], vec![doc! {"_id": 1}], &Limits::default()).unwrap_err();
    assert!(err.to_string().contains("$out writes a collection"), "{err}");
}

#[test]
fn foreign_collections_are_found_through_every_nesting() {
    let stages = aggregate::parse(&[
        doc! {"$lookup": {"from": "a", "localField": "x", "foreignField": "y", "as": "j"}},
        doc! {"$lookup": {"from": "b", "as": "k", "pipeline": [
            doc! {"$lookup": {"from": "c", "localField": "x", "foreignField": "y", "as": "n"}},
        ]}},
        doc! {"$graphLookup": {
            "from": "d", "startWith": "$x", "connectFromField": "x", "connectToField": "y",
            "as": "g", "maxDepth": 1
        }},
        doc! {"$facet": {"f": [
            doc! {"$lookup": {"from": "e", "localField": "x", "foreignField": "y", "as": "m"}},
            doc! {"$lookup": {"from": "a", "localField": "x", "foreignField": "y", "as": "m2"}},
        ]}},
    ])
    .unwrap();
    assert_eq!(aggregate::foreign_collections(&stages), ["a", "b", "c", "d", "e"]);
}
