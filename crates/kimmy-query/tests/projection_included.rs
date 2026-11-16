//! An INCLUDED dotted path keeps its parent whatever it finds inside, in a `find` projection and in
//! a `$project` stage alike (`data/projection_included.tsv`, built by `data/projection_included_gen.py`
//! from q7s4q 1 to 12, q7s3d 23, 25, 32, 35, 41 and q7s4g 35, 97, 98, 99):
//!
//! * a sub-document stays a document holding only what was selected, EMPTY if nothing was
//!   (`{"o.zz": 1}` over `{o: {k: 1}}` answers `{o: {}}`);
//! * an array stays an array with one projected document per document element, empty documents
//!   included, scalar elements dropped, nested arrays walked the same way;
//! * only a parent that is a scalar, null or missing is left out; an empty document parent stays
//!   `{}` and an empty array `[]`;
//! * a digit segment is a field name like any other.
//!
//! The documents and the answers are compared whole, typed and in order. A `find` is
//! `shape::parse_projection` and `shape::project`; a `$project` is the stage of an aggregation
//! pipeline. Every failing row is reported before the test asserts.

use bson::{Bson, Document};
use kimmy_query::{Limits, aggregate, shape};
use serde_json::Value;

fn documents(text: &str) -> Vec<Document> {
    let value: Value = serde_json::from_str(text).unwrap();
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|d| Bson::try_from(d.clone()).unwrap().as_document().unwrap().clone())
        .collect()
}

fn find(projection: &Document, docs: &[Document]) -> Vec<Document> {
    let projection = shape::parse_projection(projection).unwrap();
    docs.iter().map(|d| shape::project(projection.as_ref(), d)).collect()
}

fn stages(pipeline: &[Document], docs: Vec<Document>) -> Vec<Document> {
    let stages = aggregate::parse(pipeline).unwrap();
    let mut out = docs;
    for stage in &stages {
        out = aggregate::apply(stage, out, &Limits::default()).unwrap();
    }
    out
}

#[test]
fn an_included_path_keeps_its_parent_in_a_find_and_in_a_project_stage() {
    let mut failures = Vec::new();
    let mut rows = 0;
    for row in include_str!("data/projection_included.tsv").lines() {
        let [label, kind, docs, spec, answer] = row.split('\t').collect::<Vec<_>>()[..] else {
            panic!("a row of five columns: {row}")
        };
        rows += 1;
        let docs = documents(docs);
        let want = documents(answer);
        let got = if kind == "find" {
            let spec = documents(&format!("[{spec}]")).remove(0);
            find(&spec, &docs)
        } else {
            stages(&documents(spec), docs)
        };
        if got != want {
            failures.push(format!(
                "{label} ({kind} {spec}):\n   ours     {got:?}\n   recorded {want:?}"
            ));
        }
    }
    assert_eq!(rows, 21, "the generator wrote 21 cases");
    println!("projection_included: {rows} cases");
    assert!(failures.is_empty(), "{} of {rows} differ:\n{}", failures.len(), failures.join("\n"));
}
