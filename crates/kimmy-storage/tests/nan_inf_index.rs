//! NaN, the infinities, null and a missing field in indexes (q7s3k, the 35 `index` records: lines
//! 1 to 8 and 67 to 93), over a real engine, one row of `data/nan_inf_index.tsv` at a time.
//!
//! * **Answers.** For `$eq`, `$ne`, `$gt`, `$gte`, `$lt` and `$lte` of a Double NaN, a Decimal128
//!   NaN, 5, both infinities and null, and the other queries of each row, the documents the
//!   filter selects (the scan) are the recorded `idsUnhinted`, and where the planner answers a
//!   query from the index, that answer is the recorded `ids`: a plain or unique index answers as a
//!   scan does. "Hinted" is an `IndexScan` on that index, as the engine has no `hint`.
//! * **Sorts.** The sort of the documents by `{v: 1}` and `{v: -1}` is the recorded
//!   `idsUnhinted`: null and missing, then NaN (a Double, then a Decimal128, in `_id` order),
//!   `-Infinity`, the numbers, `+Infinity`; descending the exact reverse. The order the index
//!   delivers (`idsHinted`) is not reproduced: the engine's scan has no sort interface.
//! * **Partial indexes** (q7s3k0067 to q7s3k0074): only the MEMBERSHIP is asserted, the documents
//!   `PartialFilter::selects` holds against the recorded hinted answer of `{v: {$exists: true}}`.
//!   The reference answers a query hinted to a partial index that cannot answer it with only what
//!   that index holds; this engine has no `hint`, so those answers are not compared (see
//!   `docs/deviations.md`).
//! * **Unique indexes** (lines 75 to 93): the inserts that collide and the builds that fail, as
//!   recorded: a Double NaN with a Decimal128 NaN, `-0.0` with `0`, a Double `Infinity` with a
//!   Decimal128 `Infinity`, the four spellings of 1, null with null, a missing field with a missing
//!   field, null with a missing field.
//!
//! Every failing row is reported, all of them, before the test asserts.

use bson::{Bson, Document, doc};
use kimmy_storage::{CandidateOrder, CollectionMeta, Engine, IndexField, IndexScan};
use serde_json::Value;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    e.create_collection("app", "docs").unwrap();
    (e, dir)
}

fn bson_of(text: &str) -> Bson {
    Bson::try_from(serde_json::from_str::<Value>(text).expect("JSON")).expect("extended JSON")
}

fn documents(text: &str) -> Vec<Document> {
    let Bson::Array(items) = bson_of(text) else { panic!("an array: {text}") };
    items.into_iter().map(|d| d.as_document().expect("a document").clone()).collect()
}

fn id_of(doc: &Document) -> i64 {
    i64::from(doc.get_i32("_id").expect("an Int32 _id"))
}

fn ids_of(value: &Value) -> Option<Vec<i64>> {
    value.as_array().map(|a| a.iter().map(|x| x.as_i64().unwrap()).collect())
}

fn by_scan(docs: &[Document], query: &Document) -> Vec<i64> {
    let filter = kimmy_query::filter::parse(query).unwrap();
    docs.iter().filter(|d| kimmy_query::filter::matches(&filter, d).unwrap()).map(id_of).collect()
}

/// The ids the planner answers from the index, `None` when it chose a scan.
fn indexed(engine: &Engine, coll: &CollectionMeta, query: &Document) -> Option<Vec<i64>> {
    let filter = kimmy_query::filter::parse(query).unwrap();
    let plan = kimmy_query::plan::choose(&filter, &coll.indexes)?;
    let scan = IndexScan {
        index_id: plan.index_id,
        ranges: &plan.ranges,
        both_bounds: plan.both_bounds,
        exact: plan.exact,
    };
    let mut ids = Vec::new();
    engine
        .visit_index_candidates(
            coll,
            &scan,
            CandidateOrder::Any,
            kimmy_storage::WalkScope::Request,
            |_, _, doc| {
                if kimmy_query::filter::matches(&filter, &doc).unwrap() {
                    ids.push(id_of(&doc));
                }
                Ok(true)
            },
        )
        .unwrap()
        .expect("the scan was not refused");
    ids.sort_unstable();
    ids.dedup();
    Some(ids)
}

fn sorted(mut v: Vec<i64>) -> Vec<i64> {
    v.sort_unstable();
    v
}

/// The field names of a sort spec.
fn keys_of(sort: &Document) -> Vec<String> {
    sort.keys().cloned().collect()
}

/// An answer cut into runs of equal sort keys, each run's ids as a set. A sort with no unique last
/// key leaves the order inside a run to the reference's reading direction (docs/deviations.md, ties
/// in a sort with no unique key), so the runs and their members are compared, not the order inside.
fn tie_groups(fields: &[String], stored: &[Document], ids: &[i64]) -> Vec<(String, Vec<i64>)> {
    let key = |id: i64| -> String {
        let doc = stored.iter().find(|d| id_of(d) == id).expect("a stored id");
        fields
            .iter()
            .map(|f| {
                let value =
                    f.split('.').try_fold(Bson::Document(doc.clone()), |at, part| match at {
                        Bson::Document(d) => d.get(part).cloned(),
                        _ => None,
                    });
                // numbers of every type that are equal are one key
                match value {
                    Some(Bson::Int32(n)) => format!("n:{}", f64::from(n)),
                    Some(Bson::Int64(n)) => format!("n:{}", n as f64),
                    Some(Bson::Double(n)) => format!("n:{}", n + 0.0),
                    Some(Bson::Decimal128(n)) => match n.to_string().parse::<f64>() {
                        Ok(x) => format!("n:{}", x + 0.0),
                        Err(_) => format!("{n:?}"),
                    },
                    // a null and a missing field sort as one key
                    None | Some(Bson::Null) => "null".to_string(),
                    other => format!("{other:?}"),
                }
            })
            .collect::<Vec<_>>()
            .join("|")
    };
    let mut groups: Vec<(String, Vec<i64>)> = Vec::new();
    for &id in ids {
        let k = key(id);
        match groups.last_mut() {
            Some((last, members)) if *last == k => members.push(id),
            _ => groups.push((k, vec![id])),
        }
    }
    for (_, members) in &mut groups {
        members.sort_unstable();
    }
    groups
}

/// One row; the failures it finds, each with the recorded and the ours.
fn check(row: &str) -> Vec<String> {
    let [label, mode, unique, partial, docs, queries, sorts, expected] =
        row.split('\t').collect::<Vec<_>>()[..]
    else {
        panic!("a row of eight columns: {row}")
    };
    let docs = documents(docs);
    let queries = documents(queries);
    let sorts = documents(sorts);
    let expected: Value = serde_json::from_str(expected).unwrap();
    let partial_filter: Option<Document> =
        (partial != "-").then(|| bson_of(partial).as_document().unwrap().clone());
    let mut failures = Vec::new();
    let mut fail = |what: String| failures.push(format!("{label}: {what}"));

    let (engine, _dir) = engine();
    let build = |engine: &Engine| {
        engine.create_index_with(
            "app",
            "docs",
            vec![IndexField::ascending("v")],
            unique == "1",
            Default::default(),
            Some("ix".into()),
            None,
            partial_filter.clone(),
        )
    };
    let mut stored: Vec<Document> = Vec::new();
    let mut index_exists = true;
    if mode == "pre" {
        let coll = engine.get_collection("app", "docs").unwrap();
        for d in &docs {
            engine.insert(&coll, d.clone()).unwrap();
        }
        stored = docs.clone();
        let built = build(&engine);
        let code = expected["create"].as_i64().unwrap();
        if (code != 0) != built.is_err() {
            fail(format!("the build: recorded code {code}, ours {:?}", built.as_ref().map(|_| ())));
        }
        index_exists = built.is_ok();
    } else {
        build(&engine).expect("an index built over nothing");
        let coll = engine.get_collection("app", "docs").unwrap();
        let codes = expected["inserts"].as_array().unwrap();
        for (d, code) in docs.iter().zip(codes) {
            let refused = engine.insert(&coll, d.clone()).is_err();
            if refused != (code.as_i64().unwrap() != 0) {
                fail(format!(
                    "insert of _id {}: recorded code {code}, ours refused {refused}",
                    id_of(d)
                ));
            }
            if !refused {
                stored.push(d.clone());
            }
        }
    }
    let coll = engine.get_collection("app", "docs").unwrap();

    // The documents a partial filter selects, against the recorded hinted `$exists: true`.
    let membership: Option<Vec<i64>> = partial_filter.as_ref().map(|filter| {
        let parsed = kimmy_core::PartialFilter::parse(filter).unwrap();
        stored.iter().filter(|d| parsed.selects(d)).map(id_of).collect()
    });

    for (query, answer) in queries.iter().zip(expected["queries"].as_array().unwrap()) {
        let scan = sorted(by_scan(&stored, query));
        if let Some(want) = ids_of(&answer["idsUnhinted"])
            && scan != sorted(want.clone())
        {
            fail(format!("{query}: scan {scan:?}, recorded unhinted {want:?}"));
        }
        if let Some(count) = answer["countUnhinted"].as_u64()
            && scan.len() as u64 != count
        {
            fail(format!("{query}: scan counts {}, recorded {count}", scan.len()));
        }
        match (&membership, ids_of(&answer["ids"])) {
            (None, Some(want)) if index_exists => {
                if let Some(got) = indexed(&engine, &coll, query)
                    && got != sorted(want.clone())
                {
                    fail(format!("{query}: the index answers {got:?}, recorded {want:?}"));
                }
            }
            (Some(held), Some(want))
                if query == &doc! { "v": { "$exists": true } }
                    && sorted(held.clone()) != sorted(want.clone()) =>
            {
                fail(format!("membership {held:?}, recorded hinted `$exists` {want:?}"));
            }
            _ => {}
        }
    }
    for (sort, answer) in sorts.iter().zip(expected["sorted"].as_array().unwrap()) {
        let keys = kimmy_query::shape::parse_sort(sort).unwrap();
        let mut docs = stored.clone();
        kimmy_query::shape::sort(&keys, &mut docs);
        let got: Vec<i64> = docs.iter().map(id_of).collect();
        if let Some(want) = ids_of(&answer["idsUnhinted"]) {
            let same = if answer["tied"].as_bool().unwrap_or(false) {
                tie_groups(&keys_of(sort), &stored, &got)
                    == tie_groups(&keys_of(sort), &stored, &want)
            } else {
                got == want
            };
            if !same {
                fail(format!("sort {sort}: ours {got:?}, recorded unhinted {want:?}"));
            }
        }
    }
    failures
}

#[test]
fn every_recorded_index_case_answers_as_the_reference_does() {
    let mut failures = Vec::new();
    let mut rows = 0;
    for row in include_str!("data/nan_inf_index.tsv").lines() {
        rows += 1;
        failures.extend(check(row));
    }
    assert_eq!(rows, 35, "the generator wrote 35 cases");
    println!("nan_inf_index: {rows} cases");
    assert!(failures.is_empty(), "{} differ:\n{}", failures.len(), failures.join("\n"));
}
