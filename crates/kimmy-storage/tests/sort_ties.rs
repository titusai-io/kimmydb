//! The tie order of a sort with no `_id` key (q7s3n, `data/sort_ties.tsv`, built by
//! `data/sort_ties_gen.py`): 14 documents of tied keys, sorted by `find` and by a `$sort` stage.
//!
//! * No index (the first record of each container), and every sort that ends in `_id`: EXACT. The
//!   tied documents stay in stored order in both directions, as the reference keeps them when it
//!   sorts in memory.
//! * An index on `v` and `{v: 1}`: the reference's index order is the ascending in-memory list, so
//!   it is exact as well.
//! * An index on `v` and `{v: -1}` with no `_id` key (the rows the generator marks `grouped`): the
//!   reference reads the index backwards and answers the exact reverse, ties included. Ours keeps
//!   stored order, so these rows are compared group by group: the sequence of distinct key values
//!   must equal the recorded one, and each group of equal keys must hold the same ids as a set
//!   (docs/deviations.md, ties in a sort with no unique key).
//!
//! The unhinted and the hinted answers are each compared. Every failing row is reported before
//! the test asserts, with both orders.

use bson::{Bson, Document};
use kimmy_query::Limits;
use serde_json::Value;

fn bson_of(value: &Value) -> Bson {
    Bson::try_from(value.clone()).expect("extended JSON")
}

fn documents(text: &str) -> Vec<Document> {
    let value: Value = serde_json::from_str(text).unwrap();
    value.as_array().unwrap().iter().map(|d| bson_of(d).as_document().unwrap().clone()).collect()
}

fn id_of(doc: &Document) -> i64 {
    i64::from(doc.get_i32("_id").expect("an Int32 _id"))
}

fn ids_of(value: &Value) -> Option<Vec<i64>> {
    value.as_array().map(|a| a.iter().map(|x| x.as_i64().unwrap()).collect())
}

/// A key as the sort compares it: equal numbers of every type are one key, and a null and a
/// missing field are one key.
fn key_of(doc: &Document, field: &str) -> String {
    match doc.get(field) {
        Some(Bson::Int32(n)) => format!("n:{}", f64::from(*n)),
        Some(Bson::Int64(n)) => format!("n:{}", *n as f64),
        Some(Bson::Double(n)) => format!("n:{}", n + 0.0),
        Some(Bson::Decimal128(n)) => match n.to_string().parse::<f64>() {
            Ok(x) => format!("n:{}", x + 0.0),
            Err(_) => format!("{n:?}"),
        },
        None | Some(Bson::Null) => "null".to_string(),
        other => format!("{other:?}"),
    }
}

/// An answer cut into runs of equal sort keys, each run's ids as a set.
fn tie_groups(fields: &[String], stored: &[Document], ids: &[i64]) -> Vec<(String, Vec<i64>)> {
    let mut groups: Vec<(String, Vec<i64>)> = Vec::new();
    for &id in ids {
        let doc = stored.iter().find(|d| id_of(d) == id).expect("a stored id");
        let key = fields.iter().map(|f| key_of(doc, f)).collect::<Vec<_>>().join("|");
        match groups.last_mut() {
            Some((last, members)) if *last == key => members.push(id),
            _ => groups.push((key, vec![id])),
        }
    }
    for (_, members) in &mut groups {
        members.sort_unstable();
    }
    groups
}

fn by_find(stored: &[Document], spec: &Document) -> Vec<i64> {
    let keys = kimmy_query::shape::parse_sort(spec).unwrap();
    let mut docs = stored.to_vec();
    kimmy_query::shape::sort(&keys, &mut docs);
    docs.iter().map(id_of).collect()
}

fn by_stage(stored: &[Document], pipeline: &[Document]) -> Vec<i64> {
    let stages = kimmy_query::aggregate::parse(pipeline).unwrap();
    let mut docs = stored.to_vec();
    for stage in &stages {
        docs = kimmy_query::aggregate::apply(stage, docs, &Limits::default()).unwrap();
    }
    docs.iter().map(id_of).collect()
}

fn check(row: &str) -> Vec<String> {
    let [label, indexed, docs, expected] = row.split('\t').collect::<Vec<_>>()[..] else {
        panic!("a row of four columns: {row}")
    };
    let stored = documents(docs);
    let expected: Value = serde_json::from_str(expected).unwrap();
    let mut failures = Vec::new();
    let mut compare = |what: &str, entry: &Value, got: Vec<i64>| {
        let spec = bson_of(&entry["spec"]).as_document().unwrap().clone();
        let fields: Vec<String> = spec.keys().cloned().collect();
        let grouped = entry["grouped"].as_bool().unwrap();
        for (kind, want) in [("unhinted", &entry["unhinted"]), ("hinted", &entry["hinted"])] {
            let Some(want) = ids_of(want) else { continue };
            let same = if grouped {
                tie_groups(&fields, &stored, &got) == tie_groups(&fields, &stored, &want)
            } else {
                got == want
            };
            if !same {
                let how = if grouped { "group by group" } else { "exactly" };
                failures.push(format!(
                    "{label}: {what} {spec} ({how}, {indexed} = indexed): ours {got:?}, recorded {kind} {want:?}"
                ));
            }
        }
    };
    for entry in expected["sorted"].as_array().unwrap() {
        let spec = bson_of(&entry["spec"]).as_document().unwrap().clone();
        compare("find", entry, by_find(&stored, &spec));
    }
    for entry in expected["aggs"].as_array().unwrap() {
        let pipeline = documents(&entry["pipeline"].to_string());
        compare("$sort stage", entry, by_stage(&stored, &pipeline));
    }
    failures
}

#[test]
fn every_recorded_tie_order_is_kept_as_recorded_or_by_group_where_the_reference_reverses_it() {
    let mut failures = Vec::new();
    let mut rows = 0;
    for row in include_str!("data/sort_ties.tsv").lines() {
        rows += 1;
        failures.extend(check(row));
    }
    assert_eq!(rows, 9, "the generator wrote 9 cases");
    println!("sort_ties: {rows} cases");
    assert!(failures.is_empty(), "{} differ:\n{}", failures.len(), failures.join("\n"));
}
