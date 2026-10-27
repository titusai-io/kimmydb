//! `$graphLookup` (ADR-254): what it parses and what it refuses before it runs.

use bson::{Bson, Document, doc};

use crate::aggregate::{self, Limits, MAX_GRAPH_DEPTH, Stage};

fn spec() -> Document {
    doc! {"from": "emp", "startWith": "$boss", "connectFromField": "boss",
    "connectToField": "_id", "as": "up", "maxDepth": 5}
}

fn with(key: &str, value: impl Into<Bson>) -> Document {
    let mut s = spec();
    s.insert(key, value);
    s
}

fn without(key: &str) -> Document {
    let mut s = spec();
    s.remove(key);
    s
}

fn refused(spec: Document) -> String {
    aggregate::parse(&[doc! {"$graphLookup": spec}]).expect_err("must be refused").to_string()
}

#[test]
fn a_complete_spec_parses_and_needs_storage() {
    let mut full = spec();
    full.insert("depthField", "step");
    full.insert("restrictSearchWithMatch", doc! {"active": true});
    let stages = aggregate::parse(&[doc! {"$graphLookup": full}]).unwrap();
    assert!(stages[0].needs_storage());
    let Stage::GraphLookup(g) = &stages[0] else { panic!("not a $graphLookup") };
    assert_eq!((g.from.as_str(), g.max_depth), ("emp", 5));
    assert_eq!(g.depth_field.as_deref(), Some("step"));
    assert!(g.restrict.is_some());
    // The pure pipeline cannot run it.
    let err = aggregate::apply(&stages[0], vec![doc! {}], &Limits::default()).unwrap_err();
    assert!(err.to_string().contains("must be run by the executor"), "{err}");
}

#[test]
fn max_depth_is_required_and_bounded() {
    let message = refused(without("maxDepth"));
    assert!(message.contains("needs `maxDepth`") && message.contains("not run"), "{message}");
    for (value, wants) in [
        (Bson::Int32(-1), "is from 0 to 1000, found -1"),
        (Bson::Int64(MAX_GRAPH_DEPTH as i64 + 1), "is from 0 to 1000, found 1001"),
        (Bson::Double(1.5), "must be a whole number"),
        (Bson::String("5".into()), "must be a whole number"),
        (Bson::Null, "must be a whole number"),
    ] {
        let message = refused(with("maxDepth", value.clone()));
        assert!(message.contains("maxDepth") && message.contains(wants), "{value:?}: {message}");
    }
    for ok in [Bson::Int32(0), Bson::Int64(MAX_GRAPH_DEPTH as i64), Bson::Double(3.0)] {
        assert!(aggregate::parse(&[doc! {"$graphLookup": with("maxDepth", ok)}]).is_ok());
    }
}

#[test]
fn every_other_required_key_and_every_unknown_key_is_refused() {
    for key in ["from", "connectFromField", "connectToField", "as"] {
        let message = refused(without(key));
        assert!(message.contains(&format!("needs a string `{key}`")), "{key}: {message}");
        let message = refused(with(key, 5));
        assert!(message.contains(&format!("needs a string `{key}`")), "{key}: {message}");
    }
    assert!(refused(without("startWith")).contains("needs `startWith`"));
    for bad in ["", "$id", "a.$b"] {
        for key in ["connectFromField", "connectToField", "as"] {
            let message = refused(with(key, bad));
            assert!(message.contains("is a field name"), "{key} {bad:?}: {message}");
        }
    }
    assert!(refused(with("depthField", "")).contains("is a field name"));
    assert!(refused(with("depthField", 3)).contains("needs a string `depthField`"));
    assert!(refused(with("restrictSearchWithMatch", 1)).contains("is a filter document"));
    assert!(refused(with("restrictSearchWithMatch", doc! {"$nope": 1})).contains("$nope"));
    let message = refused(with("graph", 1));
    assert!(message.contains("graph"), "{message}");
}

#[test]
fn as_and_depth_field_are_one_field_not_a_path() {
    for key in ["as", "depthField"] {
        let message = refused(with(key, "a.b"));
        assert!(
            message.contains(&format!(
                "`{}` is the name of one field, not a path",
                if key == "as" { "as" } else { "depthField" }
            )) && message.contains("a.b"),
            "{key}: {message}"
        );
    }
    assert!(aggregate::parse(&[doc! {"$graphLookup": with("depthField", "step")}]).is_ok());
}

#[test]
fn a_variable_string_in_restrict_search_with_match_in_a_lookup_sub_pipeline_is_refused() {
    let restrict = doc! {"restrictSearchWithMatch": {"owner": "$$who"}};
    let mut inner = spec();
    inner.extend(restrict);
    let in_lookup = aggregate::parse(&[doc! {"$lookup": {
    "from": "x", "let": {"who": "$w"},
    "pipeline": [{"$graphLookup": inner.clone()}], "as": "j"}}]);
    let message = in_lookup.expect_err("a variable is not read by the filter language").to_string();
    assert!(
        message.contains("$$who")
            && message
                .contains("`restrictSearchWithMatch`: `let` variables are not available there")
            && !message.contains("write the comparison in $expr"),
        "{message}"
    );
    // At the top level there is no `let`, and the string is the literal it is in a `$match`.
    assert!(aggregate::parse(&[doc! {"$graphLookup": inner}]).is_ok());
}
